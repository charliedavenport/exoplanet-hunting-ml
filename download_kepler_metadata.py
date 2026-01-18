"""
TODO: 
    - Download light curve data and save to disk
    - Check if metadata file already exists
"""

import pyvo
import argparse
from astropy.table import Table
import os

tce_table_name = "q1_q17_dr25_tce"
planets_table_name = "ps"
base_service_url = "https://exoplanetarchive.ipac.caltech.edu/TAP"

def get_exoplanet_tces(service : pyvo.dal.TAPService):
    adql_query = f"""
        SELECT kepid, tce_plnt_num, tce_time0, tce_period, tce_time0bk, tce_duration, 
            tce_incl,  tce_num_transits, tce_model_snr, tce_depth
        FROM {tce_table_name}
        WHERE tce_rogue_flag = 0
    """
    return service.search(adql_query).to_table()


def get_confirmed_koi(service : pyvo.dal.TAPService):
    adql_query = f"""
        SELECT kepid, kepoi_name, kepler_name, koi_tce_plnt_num, koi_count, koi_comment,
            koi_period, koi_time0bk, koi_duration, koi_model_snr, koi_quarters, koi_num_transits
        FROM cumulative
        WHERE koi_disposition = 'CONFIRMED'
    """
    return service.search(adql_query).to_table()

def get_non_ttv_planets(service: pyvo.dal.TAPService):
    # https://exoplanetarchive.ipac.caltech.edu/cgi-bin/TblView/nph-tblView?app=ExoTbls&config=TD
    adql_query = f"""
        SELECT pl_name, ttv_flag
        FROM TD
        WHERE ttv_flag = 0
        AND default_flag = 1
    """
    return service.search(adql_query).to_table()

def get_pl_names(service: pyvo.dal.TAPService):
    # https://exoplanetarchive.ipac.caltech.edu/cgi-bin/TblView/nph-tblView?app=ExoTbls&config=kep_conf_names 
    adql_query = f"""
        SELECT kepid, koi_name, kepler_name, pl_name
        from keplernames
    """
    return service.search(adql_query).to_table()

def main(args):
    service = pyvo.dal.TAPService(base_service_url)
    confirmed_kois : Table = get_confirmed_koi(service)
    output_filepath = os.path.join(args.output_dir, "confirmed_kois.csv")
    confirmed_kois.write(output_filepath, format="ascii.csv", overwrite=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simple script to download metadata related to Kepler exoplanets.")
    parser.add_argument("output_dir", type=str, help="output directory")
    args = parser.parse_args()
    main(args)
