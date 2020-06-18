import re
import requests
import random
import firebase_admin
import os
import config
import google.cloud.logging
from firebase_admin import firestore
from google.cloud.logging.resource import Resource
from fake_useragent import UserAgent
from bs4 import BeautifulSoup
from datetime import datetime
from time import sleep, perf_counter


def gen_google_cloud_logger():
    log_client = google.cloud.logging.Client()

    res = Resource(type="cloud_function",
                   labels={
                       "function_name": "refresh_classes",
                       "region": os.environ.get("FUNC_REGION")
                   })

    return log_client.logger('cloudfunctions.googleapis.com%2Fcloud-functions'.format(os.environ.get("DEFAULT_PROJECT_ID"))), res


def requests_connectionerror_bypass(i, ua):
    pg = None
    while not pg:
        try:
            pg = requests.get(config.TARGET_URL_FMT.format(config.LATEST_TERM, i), headers={"User-Agent": ua.random})
        except requests.exceptions.ConnectionError:
            sleep(5)

    return pg


def requests_bandwith_bypass(pg, i, logger, res):
    html_content = BeautifulSoup(pg.content, "html.parser")

    if "exceeded the bandwidth limits" in html_content.text:
        while "exceeded the bandwidth limits" in html_content.text:
            logger.log_text("Sleeping for 60s", resource=res, severity="INFO")
            sleep(60)
            pg = requests.get(config.TARGET_URL_FMT.format(config.LATEST_TERM, i))
            html_content = BeautifulSoup(pg.content, "html.parser")

    return html_content


def fetch_proxies(ua):
    proxy_pg = requests.get("https://www.sslproxies.org/", headers={"User-Agent": ua.random})

    proxies = []

    proxy_bs = BeautifulSoup(proxy_pg.content, 'html.parser')
    proxies_table = proxy_bs.find(id='proxylisttable')

    for row in proxies_table.tbody.find_all('tr'):
        proxies.append({
            'ip': row.find_all('td')[0].string,
            'port': row.find_all('td')[1].string
        })

    return proxies


def find_new_proxy(proxies):
    return proxies[random.randint(0, len(proxies) - 1)]


def main(data, context):
    logger, res = gen_google_cloud_logger()

    cred = firebase_admin.credentials.ApplicationDefault()

    if not firebase_admin._apps:
        firebase_admin.initialize_app(cred, {"projectId": os.environ.get("PROJECT_ID")})

    firebase_db = firestore.client()

    user_agent = UserAgent()

    start_time = perf_counter()

    for i in range(config.START_IDX, config.END_IDX):
        print(i)
        logger.log_text(f"Checking class with id {i}", resource=res, severity="INFO")

        pg = requests_connectionerror_bypass(i, user_agent)

        html_content = requests_bandwith_bypass(pg, i, logger, res)

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

        class_id = int(class_general_delimited[1])

        class_code = class_general_delimited[2]

        class_dddefault = " ".join(html_content.find_all("td", class_="dddefault")[0].text.replace("\n", " ").split())

        class_credits = float(re.search("\d+\.\d+(?=\s+Credits)", class_dddefault).group(0))

        class_seats = [int(re.search("Seats (-*\d+) (-*\d+) (-*\d+)", class_dddefault).group(x)) for x in range(1, 4)]

        class_waitlist_seats = [int(re.search("Waitlist Seats (-*\d+) (-*\d+) (-*\d+)", class_dddefault).group(x)) for x in
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

        course_dict = {
            "id": class_id,
            "code": class_code,
            "name": class_name,
            "credits": class_credits,
            "seats": {
                "capacity": class_seats[0],
                "actual": class_seats[1],
                "remaining": class_seats[2]
            },
            "waitlist": {
                "capacity": class_waitlist_seats[0],
                "actual": class_waitlist_seats[1],
                "remaining": class_waitlist_seats[2]
            },
            "restrictions": class_restrictions,
            "prerequisites": class_prerequisites,
            "last_updated": datetime.now()
        }

        # Send all collected class metadata
        firebase_db.collection(u'{}'.format(config.PRIMARY_TABLE_NAME)).document(u'{}'.format(class_id)).set(course_dict)

        all_courses_doc = firebase_db.collection(u'{}'.format(config.SECONDARY_TABLE_NAME)).document(u'{}'.format("all_courses")).get()

        if all_courses_doc.exists:
            all_courses = all_courses_doc.to_dict()
            all_courses[str(class_id)] = course_dict
            firebase_db.collection(u'{}'.format(config.SECONDARY_TABLE_NAME)).document(u'{}'.format("all_courses")).set(all_courses)
        else:
            firebase_db.collection(u'{}'.format(config.SECONDARY_TABLE_NAME)).document(u'{}'.format("all_courses")).set({
                str(class_id): course_dict
            })

    logger.log_text(f"Total seconds elapsed: {perf_counter() - start_time}", resource=res, severity="INFO")


if __name__ == '__main__':
    main('data', 'context')
