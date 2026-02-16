import lightkurve as lk
import pandas as pd
import argparse
import asyncio
import aiofiles
import os
from astropy.utils.data import conf
import warnings
from astropy.utils.exceptions import AstropyWarning

warnings.simplefilter('ignore', category=AstropyWarning)

conf.remote_timeout = 60
lc_folder = "kepler-data/light-curves"
max_plnt_num = 7

async def search_lightcurve_async(kic_id):
    print(f"searching lightcurves for {kic_id}...")
    try:
        search_res = await asyncio.to_thread(
            lk.search_lightcurve, kic_id, exptime=60
        )
    except TimeoutError as err:
        print(f"TimeoutError for {kic_id}")
        return

    if len(search_res) == 0:
        print(f"No lightcurves found for {kic_id}")
        return
    return search_res


async def download_lightcurve_async(kic_id, search_res):
    print(f"downloading lightcurve for {kic_id}...")
    try:
        lc = await asyncio.to_thread(
            search_res.download_all
        )
    except TimeoutError as err:
        print(f"TimeoutError for {kic_id}")
        return

    if not lc:
        print(f"Downloaded empty lightcurve for {kic_id}")
        return

    return lc.stitch()


def save_lightcurve_fits(folder, kic_id, lc):
    lc_path = os.path.join(folder, f"{kic_id}.fits")
    print(f"Saving lightcurve to {lc_path}")
    with open(lc_path, "wb") as f:
        lc.to_fits(path=f)
        # await asyncio.to_thread(
        #     lc.to_fits, f
        # )


def get_transit_meta(lc, meta):
    pass


async def lightcurve_pipeline_task_async(kic_id, meta_row):
    search_res = await search_lightcurve_async(kic_id)
    if not search_res:
        return

    lc = await download_lightcurve_async(kic_id, search_res)
    if not lc:
        return
    
    # synchronously do some bare minimum pre-processing
    lc = lc.remove_nans().remove_outliers()
    
    await asyncio.to_thread(
        save_lightcurve_fits, lc_folder, kic_id, lc
    )
    # save_lightcurve_fits(kepid_folder, kic_id, lc)
    




async def main(args):
    kep_meta = pd.read_csv(args.metadata, index_col=0)

    # switch from indexing on planets (kepid + tce_plnt_num) to indexing on stars (kepid)
    kep_stars = kep_meta.pivot(
        index="kepid", 
        columns="tce_plnt_num", 
        # only want values that are per-planet and not per-star
        values=[
            "tce_time0bk",
            "tce_duration",
            "tce_period",
            "tce_num_transits",
            "tce_model_snr"
        ]
    )
    # get max tce_plnt_num so we know how far to iterate through the pivot columns
    global max_plnt_num
    max_plnt_num = max(kep_meta["tce_plnt_num"].to_list())

    kep_stars = kep_stars[:10]
    # print(kep_stars)
    # return

    print(f"Downloading lightcurves and creating metadata for {len(kep_stars.index)} Kepler stars...")

    coroutines = [
        lightcurve_pipeline_task_async(
            kic_id=f"KIC{kepid}",
            meta_row=row
        ) for kepid, row in kep_stars.iterrows()
    ]

    await asyncio.gather(*coroutines)

    
        


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Script to download light curve data for Kepler exoplanets.")
    parser.add_argument(
        "-o", "--output_dir", 
        type=str, help="output directory", 
        default="kepler-data/light-curves"
    )
    parser.add_argument(
        "-m", "--metadata", 
        type=str, help="relative path to metadata file", 
        default="kepler-data/kepler_metadata.csv"
    )
    args = parser.parse_args()

    main_coroutine = main(args)
    asyncio.run(main_coroutine)
