import lightkurve as lk
import polars as pl
import pandas as pd
import argparse


def main(args):
    kep_meta = pd.read_csv(args.metadata)
    
    for ind, row in kep_meta.iterrows():
        kep_id = row["kepid"]

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
    main(args)
