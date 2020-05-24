#!/usr/bin/env python
# coding: utf-8

import argparse
import sqlite3
import time
from tqdm import tqdm
import scraper

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start_year", type=int, default=1986)
    parser.add_argument("--end_year", type=int, default=2020)
    parser.add_argument("--dbpath", type=str, default="netkeiba.db")
    return parser.parse_args()

def init_database(dbpath):
    print("Initializing database")
    connection = sqlite3.connect(dbpath)
    cursor = connection.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS race_info (
            race_id          INTEGER PRIMARY KEY,
            year             INTEGER,
            month            INTEGER,
            day              INTEGER,
            venue            TEXT,
            race_number      INTEGER,
            race_name        TEXT,
            course_type      TEXT,
            course_direction TEXT,
            course_distance  INTEGER,
            weather          TEXT,
            course_state     TEXT,
            win_number       TEXT,
            win_refund       TEXT,
            win_population   TEXT,
            place_number                TEXT,
            place_refund                TEXT,
            place_population            TEXT,
            bracket_quinella_number     TEXT,
            bracket_quinella_refund     TEXT,
            bracket_quinella_population TEXT,
            quinella_number             TEXT,
            quinella_refund             TEXT,
            quinella_population         TEXT,
            quinella_place_number       TEXT,
            quinella_place_refund       TEXT,
            quinella_place_population   TEXT,
            exacta_number               TEXT,
            exacta_refund               TEXT,
            exacta_population           TEXT,
            trio_number                 TEXT,
            trio_refund                 TEXT,
            trio_population             TEXT,
            tierce_number               TEXT,
            tierce_refund               TEXT,
            tierce_population           TEXT
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS race_data (
            race_id           INTEGER,
            horse_id          INTEGER,
            rank              INTEGER,
            slot              INTEGER,
            horse_num         INTEGER,
            horse_name        TEXT,
            horse_gender      TEXT,
            horse_age         INTEGER,
            jockey_weight     REAL,
            jockey_name       TEXT,
            goal_time         REAL,
            last_time         REAL,
            odds              REAL,
            popularity        INTEGER,
            horse_weight      REAL,
            horse_weight_diff REAL,
            trainer           TEXT,
            prize             REAL,
            odds_place        REAL
        )
    """)
    connection.commit()
    connection.close()

def get_exist_race_ids(start_year, end_year, dbpath):
    connection = sqlite3.connect(dbpath)
    cursor = connection.cursor()
    cursor.execute("SELECT race_id FROM race_info WHERE year BETWEEN ? AND ?",
                   [start_year, end_year])
    exist_race_ids = [str(id[0]) for id in cursor.fetchall()]
    return exist_race_ids

def insert_into_database(race_info, race_data, dbpath):
    connection = sqlite3.connect(dbpath, isolation_level="EXCLUSIVE")
    try:
        race_info.to_sql(u"race_info", connection, if_exists="append", index=False)
        race_data.to_sql(u"race_data", connection, if_exists="append", index=False)
        connection.commit()
        print("Inserted race_id %d" % race_info["race_id"])
    except sqlite3.OperationalError as exception:
        print("\033[31m")
        print("Could not insert race_id %d" % race_info["race_id"])
        print(exception)
        print("\033[0m")
        connection.rollback()
    finally:
        connection.close()

def scraping(start_year, end_year, dbpath):
    print("Start scraping data from %d to %d" % (start_year, end_year))

    exist_race_ids = get_exist_race_ids(start_year, end_year, dbpath)
    race_ids = sorted(list(set(scraper.get_race_ids(start_year, end_year)) - set(exist_race_ids)))

    for race_id in tqdm(race_ids):
        time_start = time.time()
        soup = scraper.get_html(race_id)

        if soup is not None:
            df_race_info, df_race_records = scraper.collect_data(soup, race_id)
            insert_into_database(df_race_info, df_race_records, dbpath)

        elapsed_time = time.time() - time_start
        time.sleep(max(1 - elapsed_time, 0))

if __name__ == "__main__":
    ARGS = get_args()
    init_database(ARGS.dbpath)
    scraping(ARGS.start_year, ARGS.end_year, ARGS.dbpath)
