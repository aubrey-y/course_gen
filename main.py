import re
import requests
import sqlite3
import logging
import pathlib
from bs4 import BeautifulSoup
from datetime import datetime
from time import sleep, perf_counter
from sqlite3 import Cursor
from typing import List


DB_PATH_FMT = "{}/db/classes.db"

PRIMARY_TABLE_NAME = "CLASSES_MASTER"

LATEST_TERM = "202008"

START_IDX = 80007

END_IDX = 93437

TARGET_URL_FMT = "https://oscar.gatech.edu/pls/bprod/bwckschd.p_disp_detail_sched?term_in={}&crn_in={}"


def check_if_table_exists(cursor: Cursor):
    cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{PRIMARY_TABLE_NAME}';")

    if not cursor.fetchall():
        cursor.execute(f"CREATE TABLE {PRIMARY_TABLE_NAME} ("
                           f"id integer NOT NULL,"
                           f"code text PRIMARY KEY,"
                           f"name text NOT NULL,"
                           f"credits real NOT NULL,"
                           f"seats_capacity integer NOT NULL,"
                           f"seats_actual integer NOT NULL,"
                           f"seats_remaining integer NOT NULL,"
                           f"waitlist_capacity integer NOT NULL,"
                           f"waitlist_actual integer NOT NULL,"
                           f"waitlist_remaining integer NOT NULL,"
                           f"restrictions text,"
                           f"prerequisites text,"
                           f"last_updated text NOT NULL"
                       f");")


def upsert_sqlite(cursor: Cursor, id: str, code: str, name: str, credits: float, seats: List, waitlist: List,
                  restrictions: str, prerequisites: str):
    cursor.execute(
        f"INSERT OR REPLACE INTO {PRIMARY_TABLE_NAME} (id, code, name, credits, seats_capacity, seats_actual, "
        f"seats_remaining, waitlist_capacity, waitlist_actual, waitlist_remaining, restrictions, prerequisites, "
        f"last_updated) "
        f"VALUES (\"{id}\", \"{code}\", \"{name}\", {credits}, {seats[0]}, {seats[1]}, {seats[2]}, {waitlist[0]}, "
        f"{waitlist[1]}, {waitlist[2]}, \"{restrictions}\", \"{prerequisites}\", "
        f"\"{datetime.now().isoformat()[:-3]}\");")


def main(data, context):
    log = logging.getLogger("course_gen")
    conn = sqlite3.connect(DB_PATH_FMT.format(pathlib.Path(__file__).parent.absolute()))
    cursor = conn.cursor()

    start_time = perf_counter()

    for i in range(START_IDX, END_IDX):
        log.info("Checking class with id {}", i)
        check_if_table_exists(cursor)

        pg = requests.get(TARGET_URL_FMT.format(LATEST_TERM, i))

        html_content = BeautifulSoup(pg.content, "html.parser")

        if "exceeded the bandwidth limits" in html_content.text:
            while "exceeded the bandwidth limits" in html_content.text:
                sleep(60)
                log.info("Sleeping for 60s")
                pg = requests.get(TARGET_URL_FMT.format(LATEST_TERM, i))
                html_content = BeautifulSoup(pg.content, "html.parser")
        if "-" not in html_content.text:
            log.info("skipping {}", i)
            continue

        class_general = html_content.find_all("th", {"scope": "row"}, class_="ddlabel")[0].text

        # For classes with dashes in the class name, replace them one by one with spaces
        # TODO retain dashes by using an alternative delimiter
        while len(re.findall("-", class_general)) != 3:
            class_general = re.sub("-", " ", class_general, 1)

        class_general_delimited = [s.strip() for s in class_general.split("-")]

        class_name = class_general_delimited[0]

        class_id = class_general_delimited[1]

        class_code = class_general_delimited[2]

        class_dddefault = " ".join(html_content.find_all("td", class_="dddefault")[0].text.replace("\n", " ").split())

        class_credits = float(re.search("\d+\.\d+(?=\s+Credits)", class_dddefault).group(0))

        class_seats = [re.search("Seats (\d+) (\d+) (\d+)", class_dddefault).group(x) for x in range(1, 4)]

        class_waitlist_seats = [re.search("Waitlist Seats (\d+) (\d+) (\d+)", class_dddefault).group(x) for x in
                                range(1, 4)]

        # Regex search method depends on prerequisites and restrictions combination
        if "Prerequisites" in class_dddefault:
            if "Restrictions" in class_dddefault:
                class_prerequisites = re.search("Prerequisites: (.*)", class_dddefault).group(1)
                class_restrictions = re.search("Restrictions: (.*) Prerequisites", class_dddefault).group(1)
            else:
                class_prerequisites = re.search("Prerequisites: (.*)", class_dddefault).group(1)
                class_restrictions = None
        else:
            if "Restrictions" in class_dddefault:
                class_prerequisites = None
                class_restrictions = re.search("Restrictions: (.*)", class_dddefault).group(1)
            else:
                class_prerequisites = None
                class_restrictions = None

        # Send all collected class metadata to Sqlite
        upsert_sqlite(cursor, class_id, class_code, class_name, class_credits, class_seats, class_waitlist_seats,
                      class_restrictions, class_prerequisites)

    # Commit changes (previously only in memory) to .db file
    conn.commit()

    # Terminate connection to Sqlite
    conn.close()

    log.info("Total seconds elapsed: {}", perf_counter() - start_time)


if __name__ == '__main__':
    main('data', 'context')
