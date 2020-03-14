#!/usr/bin/env python
# coding: utf-8

import argparse
import time
import os
import re
import requests
import pandas as pd
from bs4 import BeautifulSoup
from tqdm import tqdm
import scraper

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start_year", type=int, default=1986)
    parser.add_argument("--end_year", type=int, default=2020)
    parser.add_argument("--csv_info_path", type=str, default="netkeiba_info.csv")
    parser.add_argument("--csv_data_path", type=str, default="netkeiba_data.csv")
    return parser.parse_args()

def get_exist_race_ids(start_year, end_year, csvpath):
    if os.path.isfile(csvpath["info"]):
        df = pd.read_csv(csvpath["info"], header=None, usecols=[0, 1])
        df = df[df[1] >= start_year]
        df = df[df[1] <= end_year]
        exist_race_ids = [str(id) for id in df[0].tolist()]
        return exist_race_ids
    return []

def insert_into_csv(race_info, race_data, csvpath):
    race_info.to_csv(csvpath["info"], mode="a", header=False, index=False)
    race_data.to_csv(csvpath["data"], mode="a", header=False, index=False)
    print("Inserted race_id %s" % race_info["race_id"].values[0])

def scraping(start_year, end_year, csvpath):
    print("Start scraping data from %d to %d" % (start_year, end_year))

    exist_race_ids = get_exist_race_ids(start_year, end_year, csvpath)
    race_ids = sorted(list(set(scraper.get_race_ids(start_year, end_year)) - set(exist_race_ids)))

    for race_id in tqdm(race_ids):
        time_start = time.time()
        soup = scraper.get_html(race_id)
        if soup is not None:
            df_race_info, df_race_records = scraper.collect_data(soup, race_id)
            insert_into_csv(df_race_info, df_race_records, csvpath)
        elapsed_time = time.time() - time_start
        if elapsed_time < 1:
            time.sleep(1 - elapsed_time)

if __name__ == "__main__":
    ARGS = get_args()
    scraping(ARGS.start_year, ARGS.end_year,
             {"info": ARGS.csv_info_path, "data": ARGS.csv_data_path})
