import lightkurve as lk
import pandas as pd
import numpy as np
import argparse
import asyncio
import os
import json
# from astropy.utils.data import conf
import warnings
from astropy.utils.exceptions import AstropyWarning
from tqdm import tqdm
from dataclasses import dataclass
import logging

# conf.remote_timeout = 60
max_plnt_num = 7 # not all-caps because this is global but mutable
# Consts
LC_FOLDER = "kepler-data/light-curves"
BATCH_SIZE = 100
TRANSIT_WINDOW_SIZE = 15000 # 25 hours * 60 exposures per hour
TRANSIT_WINDOW_SIZE_BJD = 25.0 / 24.0 # measured in days instead of number of expected datapoints

logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s %(message)s'
)
logging.getLogger("lightkurve.utils").setLevel(logging.CRITICAL)
logger = logging.getLogger(__name__)


@dataclass
class TransitWindow:
    window_min: float
    window_max: float
    midpoint: float
    transit_min: float
    transit_max: float


async def search_lightcurve_async(kic_id):
    logger.debug(f"searching lightcurves for {kic_id}...")
    try:
        search_res = await asyncio.to_thread(
            lk.search_lightcurve, kic_id, exptime=60
        )
    except TimeoutError as err:
        logger.error(f"TimeoutError for {kic_id}")
        return

    if len(search_res) == 0:
        logger.info(f"No lightcurves found for {kic_id}")
        return
    return search_res


async def download_lightcurve_async(kic_id, search_res):
    logger.debug(f"downloading lightcurve for {kic_id}...")
    try:
        lc = await asyncio.to_thread(
            search_res.download_all
        )
    except TimeoutError as err:
        logger.error(f"TimeoutError for {kic_id}")
        return

    if not lc:
        logger.warning(f"Downloaded empty lightcurve for {kic_id}")
        return

    return lc.stitch()


def save_lightcurve_fits(folder, kepid, lc):
    lc_path = os.path.join(folder, f"{kepid}.fits")
    logger.debug(f"Saving lightcurve to {lc_path}")
    with open(lc_path, "wb") as f:
        lc.to_fits(path=f)


def get_transit_meta(lc, meta_row):
    transit_meta = {}
    transits = []
    non_transit_windows = []

    for i in range(1, max_plnt_num):
        # column names are tuples due to the pivot on tce_plnt_num
        transit_t0 = meta_row[("tce_time0bk", i)]
        transit_period = meta_row[("tce_period", i)]
        transit_duration = meta_row[("tce_duration", i)]
        num_transits = (meta_row[("tce_num_transits", i)])

        # skip if null for this planet number
        if np.isnan(transit_t0):
            continue

        for j in range(int(num_transits)):
            transit_midpoint = transit_t0 + (j * transit_period)
            trans_window_min = transit_midpoint - (0.5 * TRANSIT_WINDOW_SIZE_BJD)
            trans_window_max = transit_midpoint + (0.5 * TRANSIT_WINDOW_SIZE_BJD)
            trans_duration_min = transit_midpoint - (0.5 * transit_duration)
            trans_duration_max = transit_midpoint + (0.5 * transit_duration)

            lc_slice = lc.truncate(trans_window_min, trans_window_max, "time")

            num_datapoints = len(lc_slice)
            if num_datapoints == 0: # TODO: find a good min_datapoints threshold instead of using 0
                continue

            transits.append(
                TransitWindow(trans_window_min, trans_window_max, transit_midpoint, trans_duration_min, trans_duration_max)
            )

    # convert to minimal dict representation for json serialization
    transit_meta["transits"] = [{
        "min": trans.window_min, 
        "max": trans.window_max} for trans in transits]

    ### Get windows of no transits

    transits.sort(lambda x: x.trans_window_min)

    for i in range(len(transits) - 2):
        curr_transit : TransitWindow = transits[i]
        next_transit : TransitWindow = transits[i+1]

        non_trans_interval = next_transit.transit_min - curr_transit.transit_max
        if non_trans_interval >= TRANSIT_WINDOW_SIZE_BJD:
            non_transit_windows.append({
                "min": curr_transit.transit_max,
                "max": next_transit.transit_min
            })

    transit_meta["non_transit_windows"] = non_transit_windows

    return transit_meta


def save_transit_meta(folder, kepid, meta):
    filepath = os.path.join(folder, f"{kepid}.json")
    with open(filepath, "wb") as f:
        f.write(json.dumps(meta))


async def lightcurve_pipeline_task_async(kepid, row):
    kic_id = f"KIC{kepid}"
    search_res = await search_lightcurve_async(kic_id)
    task_meta = {"kepid": kepid}
    if not search_res:
        task_meta["result"] = "no search result found"
        return task_meta

    lc = await download_lightcurve_async(kic_id, search_res)
    if not lc:
        task_meta["result"] = "failed to download lightcurve"
    
    lc = lc.remove_nans().remove_outliers()

    await asyncio.to_thread(save_lightcurve_fits, LC_FOLDER, kepid, lc)

    transit_meta = get_transit_meta(lc, row)

    await asyncio.to_thread(save_transit_meta, LC_FOLDER, kepid, transit_meta)

    task_meta["result"] = "succeeded"
    return task_meta
    

async def process_all_lightcurves_async(kep_stars):
    coroutines = [
        lightcurve_pipeline_task_async(kepid, row) \
            for kepid, row in kep_stars.iterrows()
    ]

    return await asyncio.gather(*coroutines)


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

    # kep_stars = kep_stars[:10]

    kep_stars_batched = [
        kep_stars[i : i + BATCH_SIZE] \
            for i in range(0, len(kep_stars.index), BATCH_SIZE)
    ]

    for i, batch in enumerate(kep_stars_batched):
        logger.info(f"Processing batch {i} of Kepler Stars...")
        task_results = await process_all_lightcurves_async(batch)
        logger.info(task_results)
        break
    
    # for kepid, lc in lightcurves:
    #     logger.info(f"{kepid}: {(lc is not None)}")

    
        


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

    warnings.simplefilter('ignore', category=AstropyWarning)

    main_coroutine = main(args)
    asyncio.run(main_coroutine, debug=False)

