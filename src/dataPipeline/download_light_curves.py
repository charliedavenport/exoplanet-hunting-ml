import lightkurve as lk
import pandas as pd
import argparse
import asyncio
import os
from astropy.utils.data import conf

conf.remote_timeout = 60
lc_folder = "../../kepler-data/light-curves"

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
    print(f"saving lightcurve to {folder}")
    if not os.path.exists(folder):
        os.mkdir(folder)
    lc_path = os.path.join(folder, "lightcurve.fits")
    if not os.path.exists(lc_path):
        lc.to_fits(lc_path)


def get_transit_meta(lc, meta):
    pass


async def lightcurve_pipeline_task_async(kic_id):
    search_res = await search_lightcurve_async(kic_id)
    if not search_res:
        return

    lc = await download_lightcurve_async(kic_id, search_res)
    if not lc:
        return
    
    # synchronously do some bare minimum pre-processing
    lc = lc.remove_nans().remove_outliers()
    
    kepid_folder = os.path.join(lc_folder, str(kic_id))
    await asyncio.to_thread(
        save_lightcurve_fits, kepid_folder, kic_id, lc
    )
    




async def main(args):
    kep_meta = pd.read_csv(args.metadata)

    kep_meta_batch = kep_meta[:10]




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
