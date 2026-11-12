"""
This file slices the light curve data into 25 hour segments. Positive training samples
are segments that are centered around a known planet transit. Negative training
samples are segments that are outside the bounds of any known transits.
"""

import lightkurve as lk
import asyncio
import argparse
import os
import pandas as pd
from tqdm import tqdm
import numpy as np


window_size_bjd = 25.0 / 24.0 # 25 hours expressed in days
min_datapoints_slice = 5000 # 25 hours * 60 exposures per hour = 15000 max datapoints. This is 1/3 of that


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

def resolve_kep_id(fits_filename : str):
	return int(fits_filename.replace("KIC", "").replace(".fits", ""))


def get_non_transit_windows():
	nontrans_windows = []

	return nontrans_windows

def get_transit_windows(meta_row, lc, max_plnt_num):
	transits = []
	# - iterate over range(max_plnt_num) to get tce columns per planet
	for i in range(1, max_plnt_num+1):
			# column names are tuples after the pivot
			transit_t0 = meta_row[("tce_time0bk", i)]
			transit_period = meta_row[("tce_period", i)]
			num_transits = (meta_row[("tce_num_transits", i)])
	
			if np.isnan(transit_t0):
				continue
	
			# - collect metadata for transit windows that have enough datapoints
			for j in range(int(num_transits)):
				transit_j_t0 = transit_t0 + (j * transit_period)
				trans_window_min = transit_j_t0 + (0.5 * window_size_bjd)
				trans_window_max = transit_j_t0 - (0.5 * window_size_bjd)
	
				lc_slice = lc.truncate(trans_window_min, trans_window_max, "time")
	
				num_datapoints = len(lc_slice)
				if num_datapoints < min_datapoints_slice:
					continue

				transits.append((trans_window_min, trans_window_max))
	return transits

def main(kepler_dir):
	kep_meta = pd.read_csv(os.path.join(kepler_dir, "kepler_metadata.csv"))
	kic_meta = get_kic_meta(kep_meta)
	max_plnt_num = max(kic_meta["tce_plnt_num"].to_list())

	lightcurve_dir = os.path.join(kepler_dir, "light-curves")
	lightcurve_files = [
		f for f in os.listdir(lightcurve_dir)
		if os.path.isfile() and f.lower().endswith(".fits")
	]
	lightcurve_dict = {
		resolve_kep_id(f) : lk.read(f) for f in lightcurve_files
	}

	training_windows = {}

	for kep_id, lc in tqdm(lightcurve_dict.items()):
		meta_row = kic_meta[kic_meta["kepid"] == kep_id]
		transit_windows = get_transit_windows(meta_row, lc, max_plnt_num)
		non_transit_windows = get_non_transit_windows()
		# TODO: add checks for empty lists etc
		training_windows[kep_id] = {
			"positive_samples" : transit_windows,
			"negative_samples" : non_transit_windows
		}


if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument(
		"-k", "--kepler_dir",
		type=str, help="kepler-data directory",	
	)
	args = parser.parse_args()
	main(args[0])