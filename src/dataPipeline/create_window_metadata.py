import lightkurve as lk
import asyncio
import argparse
import os
import pandas as pd
from tqdm import tqdm

window_size_bjd = 25.0 / 24.0 # 25 hours expressed in days


def get_kic_meta(kep_meta: pd.DataFrame):
	return kep_meta.pivot(
		index="kepid", 
		columns="tce_plnt_num", 
		# values that are per-planet and not per-star
		values=[
			"tce_time0bk",
			"tce_duration",
			"tce_period",
			"tce_num_transits",
			"tce_model_snr"
		]
	)

def get_transit_windows(meta_row, lc)

def main(kepler_dir):
	kep_meta = pd.read_csv(os.path.join(kepler_dir, "kepler_metadata.csv"))
	kic_meta = get_kic_meta(kep_meta)

	lightcurve_dir = os.path.join(kepler_dir, "light-curves")
	lightcurve_files = [
		f for f in os.listdir(lightcurve_dir)
		if os.path.isfile() and f.lower().endswith(".fits")
	]

	for lc_file in tqdm(lightcurve_files):
		lc = lk.read(lc_file)
		meta_row = kic_meta[kic_meta["kepid"]]
	

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument(
		"-k", "--kepler_dir",
		type=str, help="kepler-data directory",	
	)
	args = parser.parse_args()
	main(args[0])