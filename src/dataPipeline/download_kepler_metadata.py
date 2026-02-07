"""
TODO: 
    - Download light curve data and save to disk
    - Check if metadata file already exists
"""

import pyvo
import argparse
import os

base_service_url = "https://exoplanetarchive.ipac.caltech.edu/TAP"

def get_exoplanet_tces(service : pyvo.dal.TAPService):
    # https://exoplanetarchive.ipac.caltech.edu/cgi-bin/TblView/nph-tblView?app=ExoTbls&config=tce
    adql_query = """
        SELECT kepid, tce_plnt_num, tce_time0, tce_period, tce_time0bk, tce_duration, 
            tce_incl,  tce_num_transits, tce_model_snr, tce_depth
        FROM q1_q17_dr25_tce
        WHERE tce_rogue_flag = 0
    """
    return service.search(adql_query).to_table()


def get_confirmed_koi(service : pyvo.dal.TAPService):
    # https://exoplanetarchive.ipac.caltech.edu/cgi-bin/TblView/nph-tblView?app=ExoTbls&config=cumulative
    adql_query = """
        SELECT kepid, kepoi_name, kepler_name, koi_tce_plnt_num, koi_count, koi_comment,
            koi_period, koi_time0bk, koi_duration, koi_model_snr, koi_quarters, koi_num_transits
        FROM cumulative
        WHERE koi_disposition = 'CONFIRMED'
    """
    return service.search(adql_query).to_table()


def get_non_ttv_planets(service: pyvo.dal.TAPService):
    # https://exoplanetarchive.ipac.caltech.edu/cgi-bin/TblView/nph-tblView?app=ExoTbls&config=TD
    adql_query = """
        SELECT pl_name, ttv_flag
        FROM TD
        WHERE ttv_flag = 0
        AND default_flag = 1
    """
    return service.search(adql_query).to_table()


def get_pl_names(service: pyvo.dal.TAPService):
    # https://exoplanetarchive.ipac.caltech.edu/cgi-bin/TblView/nph-tblView?app=ExoTbls&config=kep_conf_names 
    adql_query = """
        SELECT kepid, koi_name, kepler_name, pl_name
        from keplernames
    """
    return service.search(adql_query).to_table()


def main(args):
    service = pyvo.dal.TAPService(base_service_url)
    
    # get "base" tables from exoplanet archive
    koi_df = get_confirmed_koi(service)
    tce_df = get_exoplanet_tces(service)
    non_ttv_df = get_non_ttv_planets(service)
    pl_name_df = get_pl_names(service)

    koi_pd = koi_df.to_pandas()
    tce_pd = tce_df.to_pandas()
    ttv_pd = non_ttv_df.to_pandas()
    pl_name_pd = pl_name_df.to_pandas()

    # filter out koi's without a planet number
    koi_filtered = (
        koi_pd[~koi_pd["koi_tce_plnt_num"].isna()]
        .rename(columns={"koi_tce_plnt_num": "tce_plnt_num"})
    )

    koi_tce = koi_filtered.merge(tce_pd, how="inner", on=["kepid", "tce_plnt_num"])

    # merge pl_names into ttv to get kep id's
    non_ttv = pl_name_pd.merge(ttv_pd, how="inner", on="pl_name").drop("kepid", axis=1)

    # filter out koi's that show TTV
    koi_tce_filtered = koi_tce.merge(non_ttv, how="inner", on="kepler_name")

    output_filepath = os.path.join(args.output_dir, "kepler_metadata.csv")
    koi_tce_filtered.to_csv(output_filepath, mode="w")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Script to download metadata related to Kepler exoplanets.")
    parser.add_argument("-o", "--output_dir", type=str, help="output directory where metadata file will be written.", default="kepler-data")
    args = parser.parse_args()
    main(args)
