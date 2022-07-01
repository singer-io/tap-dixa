from tap_dixa.exceptions import *
from unittest import TestCase


def test_5xx_exception_error():
    """
        tests all types of server side exception raises defined in exceptions.py
    """
    test_cases = [{"case": {"code": 500}}, {"case": {"code": 503}}]

    for each_test_case in test_cases:
        TestCase.assertRaises(DixaClient5xxError,
                              ERROR_CODE_EXCEPTION_MAPPING[each_test_case["case"]["code"]]["raise_exception"])


def test_5xx_error_message():
    """
        tests all types of server side error messages defined in exceptions.py
    """
    test_cases = [{"case": {"code": 500}}, {"case": {"code": 503}}]
    expected_response = "Server Error"

    for each_test_case in test_cases:
        assert expected_response == ERROR_CODE_EXCEPTION_MAPPING[each_test_case["case"]["code"]]["message"]


def test_4xx_exception_error():
    """
    tests all types of client side exception raises defined in exceptions.py
    """
    test_cases = [
        {"case": {"code": 400}, "expected": DixaClient400Error},
        {"case": {"code": 401}, "expected": DixaClient401Error},
        {"case": {"code": 422}, "expected": DixaClient422Error},
        {"case": {"code": 429}, "expected": DixaClient429Error}
    ]

    for each_test_case in test_cases:
        TestCase.assertRaises(each_test_case["expected"],
                              ERROR_CODE_EXCEPTION_MAPPING[each_test_case["case"]["code"]]["raise_exception"])


def test_4xx_error_message():
    """
    tests all types of client side error messages defined in exceptions.py
    """
    test_cases = [
        {"case": {"code": 400}, "expected": "Invalid query parameters"},
        {"case": {"code": 401}, "expected": "Invalid or missing credentials"},
        {"case": {"code": 422}, "expected": "Exceeded max allowed 10 csids per request"},
        {"case": {"code": 429}, "expected": "API limit has been reached"}
    ]

    for each_test_case in test_cases:
        assert each_test_case["expected"] == ERROR_CODE_EXCEPTION_MAPPING[each_test_case["case"]["code"]]["message"]



