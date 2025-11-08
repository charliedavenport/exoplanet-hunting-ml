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
            tce_incl,  tce_num_transits, tce_model_snr
        FROM {tce_table_name}
        WHERE tce_rogue_flag = 0
    """

    result = service.search(adql_query)
    return result.to_table()


def get_confirmed_koi(service : pyvo.dal.TAPService):
    adql_query = f"""
        SELECT kepid, kepoi_name, kepler_name, koi_tce_plnt_num, koi_count, koi_comment,
            koi_period, koi_time0bk, koi_duration, koi_model_snr, koi_quarters, koi_num_transits
        FROM cumulative
        WHERE koi_disposition = 'CONFIRMED'
    """

    result = service.search(adql_query)
    return result.to_table()

def main(args):
    service = pyvo.dal.TAPService(base_service_url)
    confirmed_kois : Table = get_confirmed_koi(service)
    output_filepath = os.path.join(args.output_dir, "confirmed_kois.csv")
    confirmed_kois.write(output_filepath, format="ascii.csv")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simple script to download metadata related to Kepler exoplanets.")
    parser.add_argument("output_dir", type=str, help="output directory")
    args = parser.parse_args()
    main(args)
