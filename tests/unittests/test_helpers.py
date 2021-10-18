import datetime

import tap_dixa.helpers as helpers


def test_unix_ms_to_date():
    test_cases = [
        {'case': 1629181750735, 'expected': '2021-08-16T23:29:10'},
        {'case': -145222249876, 'expected': '1965-05-25T21:29:10'},
        {'case': 1441761672555, 'expected': '2015-09-08T18:21:12'},
    ]

    for case in test_cases:
        assert case['expected'] == helpers.unix_ms_to_date(case['case'])


def test_datetime_to_unix_ms():
    test_cases = [
        {'case': datetime.datetime(2021, 8, 16, 23, 29, 10, 735719), 'expected': 1629181750735},
        {'case': datetime.datetime(1965, 5, 25, 21, 29, 10, 123456), 'expected': -145222249876},
        {'case': datetime.datetime(2015, 9, 8, 18, 21, 12, 555555), 'expected': 1441761672555},
    ]

    for case in test_cases:
        assert case['expected'] == helpers.datetime_to_unix_ms(case['case'])


def test_create_csid_params():
    test_cases = [
        {'case': (i for i in range(5)), 'expected': {'csids': '0,1,2,3,4'}},
        {'case': (i for i in 'abcdef'), 'expected': {'csids': 'a,b,c,d,e,f'}},
    ]

    for case in test_cases:
        assert case['expected'] == helpers.create_csid_params(case['case'])


def test_chunks():
    test_cases = [
        {'case': [i for i in range(20)], 'arg': 10, 'expected': [[0, 1, 2, 3, 4, 5, 6, 7, 8, 9], [10, 11, 12, 13, 14, 15, 16, 17, 18, 19]]},
        {'case': [i for i in range(10)], 'arg': 3, 'expected': [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]]},
    ]

    for case in test_cases:
        assert case['expected'] == list(helpers.chunks(case['case'], chunk_size=case['arg']))


def test_get_next_page_key():
    test_cases = [
        {'case': 'https://www.example.com/api/v1/some/path?some=query_string', 'expected': {'some': 'query_string'}},
        {'case': '/api/v1/some/path?qs=abc123', 'expected': {'qs': 'abc123'}},
        {'case': None, 'expected': {}},
    ]

    for case in test_cases:
        assert case['expected'] == helpers.get_next_page_key(case['case'])
