import firebase_admin
import os
import config
import responses
from firebase_admin import firestore
from unittest import TestCase, mock
from datetime import datetime
from pytz import UTC
from google.api_core.datetime_helpers import DatetimeWithNanoseconds
from main import main


class TestMain(TestCase):

    def setUp(self) -> None:
        cred = firebase_admin.credentials.ApplicationDefault()

        firebase_admin.initialize_app(cred, {"projectId": os.environ.get("PROJECT_ID")})

        self.firebase_db = firestore.client()

        config.END_IDX = config.START_IDX + 2

    @responses.activate
    def test_main(self):
        self.prepare_responses()

        test_collection = self.firebase_db.collection(u'{}'.format(config.PRIMARY_TABLE_NAME)).stream()

        # Collection should be empty
        for _ in test_collection:
            assert False

        with mock.patch("main.datetime") as datetime_mock:
            custom_date = datetime(2020, 6, 1)
            datetime_mock.now.return_value = custom_date

            main("data", "context")

        test_collection = self.firebase_db.collection(u'{}'.format(config.PRIMARY_TABLE_NAME)).stream()

        actual = []
        # Collection should contain expected
        for document in test_collection:
            actual.append(document.to_dict())

        expected = [
            {
                'id': '80007',
                'seats': {
                    'remaining': '0',
                    'actual': '57',
                    'capacity': '58'
                },
                'waitlist': {
                    'remaining': '3',
                    'actual': '27',
                    'capacity': '30'
                },
                'name': 'Class A',
                'prerequisites': 'a prerequisite',
                'credits': 3.0,
                'code': 'ABC 123',
                'last_updated': DatetimeWithNanoseconds(2020, 6, 1, 0, 0, 0, 0, tzinfo=UTC),
                'restrictions': 'a restriction'
            }, {
                'id': '80008',
                'seats': {
                    'remaining': '0',
                    'actual': '55',
                    'capacity': '55'
                },
                'waitlist': {
                    'remaining': '28',
                    'actual': '2',
                    'capacity': '30'
                },
                'name': 'Class B',
                'prerequisites': 'b prerequisites',
                'credits': 3.0,
                'code': 'CBA 321',
                'last_updated': DatetimeWithNanoseconds(2020, 6, 1, 0, 0, 0, 0, tzinfo=UTC),
                'restrictions': 'b restrictions'
            }
        ]

        self.maxDiff = None
        self.assertCountEqual(expected, actual)

    @staticmethod
    def prepare_responses():
        responses.add_passthru("https://oauth2.googleapis.com/token")

        with open(f"{os.path.dirname(os.path.abspath(__file__))}/data/80007.html") as pg_file:
            responses.add(method=responses.GET,
                          url=config.TARGET_URL_FMT.format(config.LATEST_TERM, config.START_IDX),
                          body=pg_file.read(), status=200)
        with open(f"{os.path.dirname(os.path.abspath(__file__))}/data/80008.html") as pg_file:
            responses.add(method=responses.GET,
                          url=config.TARGET_URL_FMT.format(config.LATEST_TERM, config.START_IDX + 1),
                          body=pg_file.read(), status=200)
