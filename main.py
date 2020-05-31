import re
import requests
import sqlalchemy
import os
from google.cloud import logging
from google.cloud.logging.resource import Resource
from bs4 import BeautifulSoup
from datetime import datetime
from time import sleep, perf_counter
from typing import List

PRIMARY_TABLE_NAME = "CLASSES_MASTER"

LATEST_TERM = "202008"

START_IDX = 80007

END_IDX = 93437

TARGET_URL_FMT = "https://oscar.gatech.edu/pls/bprod/bwckschd.p_disp_detail_sched?term_in={}&crn_in={}"


def check_if_table_exists(db):
    with db.connect() as cursor:
        table_exists = cursor.execute(f"SHOW TABLES LIKE '{PRIMARY_TABLE_NAME}';")

        if not table_exists.rowcount:
            cursor.execute(f"CREATE TABLE {PRIMARY_TABLE_NAME} ("
                           f"id INT PRIMARY KEY,"
                           f"code VARCHAR(255) NOT NULL,"
                           f"name VARCHAR(255) NOT NULL,"
                           f"credits FLOAT NOT NULL,"
                           f"seats_capacity INT NOT NULL,"
                           f"seats_actual INT NOT NULL,"
                           f"seats_remaining INT NOT NULL,"
                           f"waitlist_capacity INT NOT NULL,"
                           f"waitlist_actual INT NOT NULL,"
                           f"waitlist_remaining INT NOT NULL,"
                           f"restrictions TEXT(65535) ,"
                           f"prerequisites TEXT(65535),"
                           f"last_updated TIMESTAMP NOT NULL"
                           f");")


def upsert_mysql(db, id: str, code: str, name: str, credits: float, seats: List, waitlist: List,
                 restrictions: str, prerequisites: str):
    with db.connect() as cursor:
        cursor.execute(
            f"REPLACE INTO {PRIMARY_TABLE_NAME} (id, code, name, credits, seats_capacity, seats_actual, "
            f"seats_remaining, waitlist_capacity, waitlist_actual, waitlist_remaining, restrictions, prerequisites, "
            f"last_updated) "
            f"VALUES (\"{id}\", \"{code}\", \"{name}\", {credits}, {seats[0]}, {seats[1]}, {seats[2]}, {waitlist[0]}, "
            f"{waitlist[1]}, {waitlist[2]}, \"{restrictions}\", \"{prerequisites}\", "
            f"\"{datetime.now()}\");")


def main(data, context):
    log_client = logging.Client()

    log_name = 'cloudfunctions.googleapis.com%2Fcloud-functions'

    res = Resource(type="cloud_function",
                   labels={
                       "function_name": "refresh_classes",
                       "region": os.environ.get("FUNC_REGION")
                   })
    logger = log_client.logger(log_name.format(os.environ.get("PROJECT_ID")))

    if os.environ.get("ENV") == "local":
        db = sqlalchemy.create_engine(
            sqlalchemy.engine.url.URL(
                drivername="mysql+pymysql",
                username=os.environ.get("DB_USER"),
                password=os.environ.get("DB_PASS"),
                host=os.environ.get("DB_HOST"),
                port=3306,
                database=PRIMARY_TABLE_NAME
            ),
            pool_size=5,
            max_overflow=2,
            pool_timeout=30,
            pool_recycle=1800
        )
    else:
        db = sqlalchemy.create_engine(
            sqlalchemy.engine.url.URL(
                drivername="mysql+pymysql",
                username=os.environ.get("DB_USER"),
                password=os.environ.get("DB_PASS"),
                database=PRIMARY_TABLE_NAME,
                query={"unix_socket": "/cloudsql/{}".format(os.environ.get("CLOUD_SQL_CONNECTION_NAME"))}
            ),
            pool_size=5,
            max_overflow=2,
            pool_timeout=30,
            pool_recycle=1800
        )
    start_time = perf_counter()

    check_if_table_exists(db)

    for i in range(START_IDX, END_IDX):
        print(i)
        logger.log_text(f"Checking class with id {i}", resource=res, severity="INFO")

        pg = requests.get(TARGET_URL_FMT.format(LATEST_TERM, i))

        html_content = BeautifulSoup(pg.content, "html.parser")

        if "exceeded the bandwidth limits" in html_content.text:
            while "exceeded the bandwidth limits" in html_content.text:
                logger.log_text("Sleeping for 60s", resource=res, severity="INFO")
                sleep(60)
                pg = requests.get(TARGET_URL_FMT.format(LATEST_TERM, i))
                html_content = BeautifulSoup(pg.content, "html.parser")
        if "-" not in html_content.text:
            print(f"skipping {i}")
            logger.log_text(f"skipping {i}", resource=res, severity="INFO")
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

        # Send all collected class metadata
        upsert_mysql(db, class_id, class_code, class_name, class_credits, class_seats, class_waitlist_seats,
                     class_restrictions, class_prerequisites)

    logger.log_text(f"Total seconds elapsed: {perf_counter() - start_time}", resource=res, severity="INFO")


if __name__ == '__main__':
    main('data', 'context')
