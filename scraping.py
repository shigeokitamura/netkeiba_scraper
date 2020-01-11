#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import re
import requests
import time
from bs4 import BeautifulSoup

def get_html(race_id):
    url_base = "https://db.netkeiba.com/race/"
    url = url_base + race_id
    html = requests.get(url)
    html.encoding = "EUC-JP"
    soup = BeautifulSoup(html.text, "html.parser")
    if soup.find_all("table", "race_table_01 nk_tb_common") == []:
        return None
    else:
        return soup

def get_race_info(soup, venue_number, race_number):
    date_text = soup.find("div", "data_intro").find("p", "smalltxt").get_text(strip=True)
    date = re.match(r"(\d+)年(\d+)月(\d+)日.+", date_text)
    year = date.group(1)
    month = date.group(2)
    day = date.group(3)

    venue_names = [None, "札幌", "函館", "福島", "新潟", "東京", "中山", "中京", "京都", "阪神", "小倉"]
    venue = venue_names[venue_number]

    race_name = soup.find("dl", "racedata fc").find("h1").get_text(strip=True)

    conditions = soup.find("dl", "racedata fc").find("span").get_text(strip=True).replace("\xa0", "").split("/")
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

    return year, month, day, venue, race_number, race_name, course_type, course_direction, course_distance, weather, course_state

def to_sec(time):
    t = time.split(":")
    return float(t[0]) * 60 + float(t[1])

def get_race_records(table):
    records = []
    for i in range(1, len(table)):
        row = table[i].find_all("td")
        rank = row[0].get_text(strip=True)
        if not rank.isdecimal():
            continue
        slot = row[1].get_text(strip=True)
        horse_name = row[3].get_text(strip=True)
        horse_gender = row[4].get_text(strip=True)[0]
        horse_age = row[4].get_text(strip=True)[1:]
        jockey_weight = row[5].get_text(strip=True)
        jockey_name = row[6].get_text(strip=True)
        time = to_sec(row[7].get_text(strip=True))
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
        record = (rank, slot, horse_name, horse_gender, horse_age, jockey_weight, jockey_name, time, last_time, odds, popularity, horse_weight, horse_weight_diff, trainer)
        records.append(record)
    return records

def scraping():
    df_columns = [
        "year",              # 年
        "month",             # 月
        "day",               # 日
        "venue",             # 開催場所
        "race_number",       # 何レース目
        "race_name",         # レース名
        "course_type",       # コース
        "course_direction",  # 左右
        "course_distance",   # 距離
        "weather",           # 天候
        "course_state",      # 馬場状態
        "rank",              # 着順
        "slot",              # 枠番
        "horse_name",        # 馬名
        "horse_gender",      # 性別
        "horse_age",         # 年齢
        "jockey_weight",     # 斤量
        "jockey_name",       # 騎手名
        "time",              # タイム
        "last_time",         # 上り
        "odds",              # 単勝のオッズ
        "popularity",        # 人気
        "horse_weight",      # 馬体重
        "horse_weight_diff", # 馬体重の増減
        "trainer"            # 調教師
    ]

    df = pd.DataFrame(columns = df_columns)

    years = list(range(1986, 2020))
    venues = list(range(1, 11))
    numbers = list(range(1, 11))
    days = list(range(1, 11))
    races = list(range(1, 13))

    for y in years:
        for v in venues:
            for n in numbers:
                for d in days:
                    for r in races:
                        race_id = f"{y}{v:02}{n:02}{d:02}{r:02}"
                        soup = get_html(race_id)
                        if soup is not None:
                            race_info = get_race_info(soup, v, r)
                            race_table = soup.find("table", "race_table_01 nk_tb_common").find_all("tr")
                            race_records = get_race_records(race_table)

                            records = []
                            for record in race_records:
                                records.append(race_info + record)

                            record_df = pd.DataFrame(records, columns = df.columns, index = [race_id] * len(records))
                            print(record_df)
                            df = df.append(record_df)
                        time.sleep(1)
        df.to_csv(f"{y}.csv")
        df = pd.DataFrame(columns = df_columns)

if __name__ == '__main__':
    scraping()
