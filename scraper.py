#!/usr/bin/env python
# coding: utf-8

import re
import requests
import pandas as pd
from bs4 import BeautifulSoup

RACE_INFO_COLUMNS = [
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

RACE_DATA_COLUMNS = [
    "race_id",           # レースID
    "horse_id",          # 馬ID
    "rank",              # 着順
    "slot",              # 枠番
    "horse_num",         # 馬番
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
    "trainer",           # 調教師
    "prize",             # 獲得賞金
    "odds_place"         # 複勝のオッズ
]

def get_race_ids(start_year, end_year):
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

    return sorted(race_ids)

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
    race_date = soup.find("div", "data_intro").find("p", "smalltxt").get_text(strip=True)
    race_date = re.match(r"(\d+)年(\d+)月(\d+)日.+", race_date)
    venue_names = [None, "札幌", "函館", "福島", "新潟", "東京", "中山", "中京", "京都", "阪神", "小倉"]
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
    if course_type == "芝ダ":
        states = re.match(r"芝 : (.+)ダート : (.+)", conditions[2])
        course_state = states.group(1) + "/" + states.group(2)
    else:
        course_state = conditions[2].split(" : ")[1]

    race_info = {
        "race_id":          [race_id],
        "year":             [race_date.group(1)],
        "month":            [race_date.group(2)],
        "day":              [race_date.group(3)],
        "venue":            [venue_names[int(race_id[4:6])]],
        "race_number":      [race_id[10:]],
        "race_name":        [soup.find("dl", "racedata fc").find("h1").get_text(strip=True)],
        "course_type":      [course_type],
        "course_direction": [course_direction],
        "course_distance":  [re.match(r".+([0-9]{4})m", conditions[0]).group(1)],
        "weather":          [conditions[1].split(" : ")[1]],
        "course_state":     [course_state]
    }
    return pd.DataFrame.from_dict(race_info)

def get_oddses(table):
    odds_place_key = table[1].find_all("td")[0].get_text(" ").split()
    odds_place_value = table[1].find_all("td")[1].get_text(" ").split()
    odds_place_value = list(map(lambda x: int(x) / 100, odds_place_value))
    return set(zip(odds_place_key, odds_place_value))

def get_race_records(table, race_oddses, race_id):
    records = {column:[] for column in RACE_DATA_COLUMNS}

    for i in range(1, len(table)):
        row = table[i].find_all("td")
        rank = row[0].get_text(strip=True)
        if not rank.isdecimal():
            continue
        horse_num = row[2].get_text(strip=True)
        weight = re.match(r"(\d+)\((\D*\d+)\)", row[14].get_text(strip=True))
        horse_weight = ""
        horse_weight_diff = ""
        if weight is not None:
            horse_weight = weight.group(1)
            horse_weight_diff = weight.group(2)
        odds_place = ""
        for race_odd in race_oddses:
            if horse_num == race_odd[0]:
                odds_place = race_odd[1]

        records["race_id"] += [race_id]
        records["horse_id"] += [row[3].find("a").get("href").split("/")[2]]
        records["rank"] += [rank]
        records["slot"] += [row[1].get_text(strip=True)]
        records["horse_num"] += [horse_num]
        records["horse_name"] += [row[3].get_text(strip=True)]
        records["horse_gender"] += [row[4].get_text(strip=True)[0]]
        records["horse_age"] += [row[4].get_text(strip=True)[1:]]
        records["jockey_weight"] += [row[5].get_text(strip=True)]
        records["jockey_name"] += [row[6].get_text(strip=True)]
        records["goal_time"] += [to_sec(row[7].get_text(strip=True))]
        records["last_time"] += [row[11].get_text(strip=True)]
        records["odds"] += [row[12].get_text(strip=True)]
        records["popularity"] += [row[13].get_text(strip=True)]
        records["horse_weight"] += [horse_weight]
        records["horse_weight_diff"] += [horse_weight_diff]
        records["trainer"] += [row[18].get_text(strip=True)]
        records["prize"] += [row[20].get_text(strip=True)]
        records["odds_place"] += [odds_place]
    return pd.DataFrame.from_dict(records)

def collect_data(soup, race_id):
    race_info = get_race_info(soup, race_id)
    race_table = soup.find("table", "race_table_01 nk_tb_common").find_all("tr")
    odds_table = soup.find("table", "pay_table_01").find_all("tr")
    race_oddses = get_oddses(odds_table)
    race_records = get_race_records(race_table, race_oddses, race_id)
    return race_info, race_records

def to_sec(str_time):
    time = str_time.split(":")
    return float(time[0]) * 60 + float(time[1])
