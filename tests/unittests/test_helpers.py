import datetime
import unittest
from tap_dixa import helpers


def test_unix_ms_to_date():
    test_cases = [
        {"case": 1629181750735, "expected": "2021-08-16T23:29:10"},
        {"case": -145222249876, "expected": "1965-05-25T21:29:10"},
        {"case": 1441761672555, "expected": "2015-09-08T18:21:12"},
    ]

    for case in test_cases:
        assert case["expected"] == helpers.unix_ms_to_date(case["case"])


def test_datetime_to_unix_ms():
    test_cases = [
        {"case": datetime.datetime(2021, 8, 16, 23, 29, 10, 735719), "expected": 1629136750735},
        {"case": datetime.datetime(1965, 5, 25, 21, 29, 10, 123456), "expected": -145267249876},
        {"case": datetime.datetime(2015, 9, 8, 18, 21, 12, 555555), "expected": 1441716672555},
    ]

    for case in test_cases:
        assert case["expected"] == helpers.datetime_to_unix_ms(case["case"])


def test_create_csid_params():
    test_cases = [
        {"case": (i for i in range(5)), "expected": {"csids": "0,1,2,3,4"}},
        {"case": (i for i in "abcdef"), "expected": {"csids": "a,b,c,d,e,f"}},
    ]

    for case in test_cases:
        assert case["expected"] == helpers.create_csid_params(case["case"])


def test_chunks():
    test_cases = [
        {
            "case": [i for i in range(20)],
            "arg": 10,
            "expected": [[0, 1, 2, 3, 4, 5, 6, 7, 8, 9], [10, 11, 12, 13, 14, 15, 16, 17, 18, 19]],
        },
        {"case": [i for i in range(10)], "arg": 3, "expected": [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]]},
    ]

    for case in test_cases:
        assert case["expected"] == list(helpers.chunks(case["case"], chunk_size=case["arg"]))


def test_get_next_page_key():
    test_cases = [
        {"case": "https://www.example.com/api/v1/some/path?some=query_string", "expected": {"some": "query_string"}},
        {"case": "/api/v1/some/path?qs=abc123", "expected": {"qs": "abc123"}},
        {"case": None, "expected": {}},
    ]

    for case in test_cases:
        assert case["expected"] == helpers.get_next_page_key(case["case"])


class TestGetReplicationMethodFromMeta(unittest.TestCase):
    """
    class to test retrieving data from meta for a given stream
    """
    positive_test_cases = [
        {"case": [{"metadata": {"forced-replication-method": "INCREMENTAL"}}], "expected": "INCREMENTAL"},
        {"case": [{"metadata": {"forced-replication-method": "FULL TABLE"}}], "expected": "FULL TABLE"}
        ]
    negative_test_cases = [
        {"case": []},
        {"case": {}},
        {"case": [{"metadata": {"replication-method": "INCREMENTAL"}}]},
        {"case": [{"metadata1": {"forced-replication-method": "INCREMENTAL"}}]}
    ]

    def test_positive_scenarios(self):
        """
        fn covers unittests for all the positive scenarios
        """
        for case in self.positive_test_cases:
            self.assertEquals(case["expected"], helpers._get_replication_method_from_meta(case["case"]))

    def test_negative_scenarios(self):
        """
        fn covers unittests for all the negative scenarios
        """
        with self.assertRaises((KeyError, IndexError)):
            for case in self.negative_test_cases:
                helpers._get_replication_method_from_meta(case["case"])


class TestGetReplicationKeyFromMeta(unittest.TestCase):
    """
    class to test retrieving data from meta for a given stream
    """
    positive_test_cases = [
        {"case": [{"metadata": {"valid-replication-keys": ["ID"]}}], "expected": None},
        {"case": [{"metadata": {"forced-replication-method": "INCREMENTAL", "valid-replication-keys": ["UPDATED_AT"]}}],
         "expected": "UPDATED_AT"}
        ]
    negative_test_cases = [
        {"case": []},
        {"case": [{"metadata": {"valid-replication-keys": {"UPDATED_AT"}}}]},
        {"case": [{"metadata": {"replication-keys": "ID"}}]},
        {"case": [{"metadata1": {"valid-replication-keys": "ID"}}]}
    ]

    def test_positive_scenarios(self):
        """
        fn covers unittests for all the positive scenarios
        """
        for case in self.positive_test_cases:
            self.assertEquals(case["expected"], helpers._get_replication_key_from_meta(case["case"]))

    def test_negative_scenarios(self):
        """
        fn covers unittests for all the negative scenarios
        """
        with self.assertRaises((KeyError, IndexError)):
            for case in self.negative_test_cases:
                helpers._get_replication_key_from_meta(case['case'])


class TestGetKeyPropertiesFromMeta(unittest.TestCase):
    """
    class to test retrieving data from meta for a given stream
    """
    positive_test_cases = [
        {"case": [{"metadata": {"table-key-properties": ["ID"]}}], "expected": ["ID"]},
        {"case": [{"metadata": {"table-key-properties": ["ID", "UPDATED_AT"]}}], "expected": ["ID", "UPDATED_AT"]}
        ]
    negative_test_cases = [
        {"case": []},
        {"case": {}},
        {"case": [{"metadata": {"key-properties": ["ID"]}}]},
        {"case": [{"metadata1": {"table-key-properties": ["ID"]}}]}
    ]

    def test_positive_scenarios(self):
        """
        fn covers unittests for all the positive scenarios
        """
        for case in self.positive_test_cases:
            self.assertEquals(case["expected"], helpers._get_key_properties_from_meta(case["case"]))

    def test_negative_scenarios(self):
        """
        fn covers unittests for all the negative scenarios
        """
        with self.assertRaises((KeyError, IndexError)):
            for case in self.negative_test_cases:
                helpers._get_key_properties_from_meta(case["case"])




