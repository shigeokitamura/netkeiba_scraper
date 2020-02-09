#!/usr/bin/env python
# coding: utf-8

import argparse
import sqlite3
import time
import re
import requests
import pandas as pd
from bs4 import BeautifulSoup
from tqdm import tqdm

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
            course_state     TEXT
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS race_data (
            race_id           INTEGER,
            horse_id          INTEGER,
            rank              INTEGER,
            slot              INTEGER,
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
            trainer           TEXT
        )
    """)
    connection.commit()
    connection.close()

def get_html(race_id):
    url_base = "https://db.netkeiba.com/race/"
    url = url_base + str(race_id)
    html = requests.get(url)
    html.encoding = "EUC-JP"
    soup = BeautifulSoup(html.text, "html.parser")
    if soup.find_all("table", "race_table_01 nk_tb_common") == []:
        return None
    return soup

def get_race_info(soup, race_id):
    date_text = soup.find("div", "data_intro").find("p", "smalltxt").get_text(strip=True)
    date = re.match(r"(\d+)年(\d+)月(\d+)日.+", date_text)
    year = date.group(1)
    month = date.group(2)
    day = date.group(3)

    venue_names = [None, "札幌", "函館", "福島", "新潟", "東京", "中山", "中京", "京都", "阪神", "小倉"]
    venue = venue_names[int(race_id[4:6])]

    race_number = int(race_id[10:])
    race_name = soup.find("dl", "racedata fc").find("h1").get_text(strip=True)

    conditions = soup.find("dl", "racedata fc").find("span")\
                 .get_text(strip=True).replace("\xa0", "").split("/")
    course_type = ""
    if "芝" in conditions[0]:
        course_type += "芝"
    if "ダ" in conditions[0]:
        course_type += "ダ"

    if "左" in conditions[0]:
        course_direction = "左"
    elif "右" in conditions[0]:
        course_direction = "右"
    elif "障" in conditions[0]:
        course_direction = "障"
    elif "直線" in conditions[0]:
        course_direction = "直"

    course_distance = re.match(r".+([0-9]{4})m", conditions[0]).group(1)
    weather = conditions[1].split(" : ")[1]
    if course_type == "芝ダ":
        states = re.match(r"芝 : (.+)ダート : (.+)", conditions[2])
        course_state = states.group(1) + "/" + states.group(2)
    else:
        course_state = conditions[2].split(" : ")[1]

    return [race_id, year, month, day, venue, race_number, race_name,
            course_type, course_direction, course_distance, weather, course_state]

def to_sec(str_time):
    t = str_time.split(":")
    return float(t[0]) * 60 + float(t[1])

def get_race_records(table, race_id):
    records = []
    for i in range(1, len(table)):
        row = table[i].find_all("td")
        rank = row[0].get_text(strip=True)
        if not rank.isdecimal():
            continue
        slot = row[1].get_text(strip=True)
        horse_name = row[3].get_text(strip=True)
        horse_id = row[3].find("a").get("href").split("/")[2]
        horse_gender = row[4].get_text(strip=True)[0]
        horse_age = row[4].get_text(strip=True)[1:]
        jockey_weight = row[5].get_text(strip=True)
        jockey_name = row[6].get_text(strip=True)
        goal_time = to_sec(row[7].get_text(strip=True))
        last_time = row[11].get_text(strip=True)
        odds = row[12].get_text(strip=True)
        popularity = row[13].get_text(strip=True)
        weight = re.match(r"(\d+)\((\D*\d+)\)", row[14].get_text(strip=True))
        if weight is not None:
            horse_weight = weight.group(1)
            horse_weight_diff = weight.group(2)
        else:
            horse_weight = ""
            horse_weight_diff = ""
        trainer = row[18].get_text(strip=True)
        record = [race_id, horse_id, rank, slot, horse_name, horse_gender, horse_age,
                  jockey_weight, jockey_name, goal_time, last_time, odds, popularity,
                  horse_weight, horse_weight_diff, trainer]
        records.append(record)
    return records

def get_race_ids(start_year, end_year, dbpath):
    years = list(range(start_year, end_year + 1))
    venues = list(range(1, 11))
    numbers = list(range(1, 11))
    days = list(range(1, 11))
    races = list(range(1, 13))

    race_ids = [f"{y}{v:02}{n:02}{d:02}{r:02}" \
                for y in years \
                for v in venues \
                for n in numbers \
                for d in days \
                for r in races]

    exist_race_ids = get_exist_race_ids(start_year, end_year, dbpath)
    race_ids = list(set(race_ids) - set(exist_race_ids))
    return sorted(race_ids)

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
    except Exception as e:
        print("\033[31m")
        print("Could not insert race_id %d" % race_info["race_id"])
        print(e)
        print("\033[0m")
        connection.rollback()
    finally:
        connection.close()

def scraping(start_year, end_year, dbpath):
    print("Start scraping data from %d to %d" % (start_year, end_year))
    race_info_columns = [
        "race_id",          # レースID
        "year",             # 年
        "month",            # 月
        "day",              # 日
        "venue",            # 開催場所
        "race_number",      # 何レース目
        "race_name",        # レース名
        "course_type",      # コース
        "course_direction", # 左右
        "course_distance",  # 距離
        "weather",          # 天候
        "course_state"      # 馬場状態
    ]
    race_data_columns = [
        "race_id",           # レースID
        "horse_id",          # 馬ID
        "rank",              # 着順
        "slot",              # 枠番
        "horse_name",        # 馬名
        "horse_gender",      # 性別
        "horse_age",         # 年齢
        "jockey_weight",     # 斤量
        "jockey_name",       # 騎手名
        "goal_time",         # タイム
        "last_time",         # 上り
        "odds",              # 単勝のオッズ
        "popularity",        # 人気
        "horse_weight",      # 馬体重
        "horse_weight_diff", # 馬体重の増減
        "trainer"            # 調教師
    ]

    race_ids = get_race_ids(start_year, end_year, dbpath)

    for race_id in tqdm(race_ids):
        time_start = time.time()
        soup = get_html(race_id)
        if soup is not None:
            race_info = get_race_info(soup, race_id)
            race_table = soup.find("table", "race_table_01 nk_tb_common").find_all("tr")
            race_records = get_race_records(race_table, race_id)

            df_race_info = pd.DataFrame([race_info], index=None, columns=race_info_columns)
            df_race_data = pd.DataFrame(race_records, index=None, columns=race_data_columns)

            insert_into_database(df_race_info, df_race_data, dbpath)
        elapsed_time = time.time() - time_start
        if elapsed_time < 1:
            time.sleep(1 - elapsed_time)


if __name__ == "__main__":
    ARGS = get_args()
    init_database(ARGS.dbpath)
    scraping(ARGS.start_year, ARGS.end_year, ARGS.dbpath)
