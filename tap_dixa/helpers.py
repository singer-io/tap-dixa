""" helper methods required for tap-dixa"""
import datetime
import os
import pytz

from enum import Enum
from typing import Iterator
from urllib.parse import parse_qsl, urlparse

from singer import  utils

def unix_ms_to_date(timestamp_ms: int) -> str:
    """
    Converts unix timestamp in milliseconds to ISO 8601 date string.

    :param ms: unix timestamp in milliseconds
    :return: ISO 8601 date string
    """
    return datetime.datetime.fromtimestamp(timestamp_ms / 1000).replace(microsecond=0).isoformat()


def unix_ms_to_date_utc(timestamp_ms: int) -> datetime.datetime:
    """
    Converts unix timestamp in milliseconds to timezone aware datetime object.

    :param timestamp_ms: unix timestamp in milliseconds
    :return: datetime obj
    """
    return datetime.datetime.fromtimestamp(timestamp_ms / 1000).replace(microsecond=0).replace(tzinfo=pytz.UTC)

def datetime_to_unix_ms(datetime_obj: datetime.datetime) -> int:
    """
    Converts datetime object to unix timestamp in milliseconds

    :param datetime_obj: A datetime object to convert to unix timestamp
    :return: integer representing unix timestamp in milliseconds
    """
    return int(datetime_obj.timestamp() * 1000)


def create_csid_params(csids: Iterator) -> dict:
    """
    Creates params for activity logs endpoint.

    Takes iterator and creates a comma-separated string list of the items
    in the iterator. Returns a dictionary with `csids` as key and
    comma-separated string list of conversation IDs as values.

    :param csids: Iterator containing conversation IDs
    :return: dictionary to be used with activity logs endpoint
    """
    return {"csids": ",".join(map(str, csids))}


def chunks(arr, chunk_size=10):
    """
    Takes an n-sized array and chunks it into smaller sizes that are
    yielded until full array has been iterated over.

    :param arr: n-sized list of conversation IDs
    :param chunk_size: the list size of each chunk
    :return: a chunk sized array
    """
    for i in range(0, len(arr), chunk_size):
        yield arr[i: i + chunk_size]


def date_to_rfc3339(date: str) -> str:
    """Converts date to rfc 3339"""
    date_utc = utils.strptime_to_utc(date)

    return date_utc.strftime("%Y-%m-%dT%H:%M:%SZ")


def get_next_page_key(url_part: str) -> dict:
    """
    Parses out and returns the `pageKey` value returned in the meta
    key from the API
    """
    parsed_url = urlparse(url_part)

    return dict(parse_qsl(parsed_url.query))


def get_abs_path(path):
    """
    Gets the absolute path of the provided relative path.
    """
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def _get_key_properties_from_meta(schema_meta):
    """
    Retrieves the 'table-key-properties' value from the schema metadata.
    """
    return schema_meta[0].get("metadata").get("table-key-properties")


def _get_replication_method_from_meta(schema_meta):
    """
    Retrieves the 'forced-replication-method' value from the schema metadata.
    """
    return schema_meta[0].get("metadata").get("forced-replication-method")


def _get_replication_key_from_meta(schema_meta):
    """
    Retrieves the 'valid-replication-keys' value from the schema metadata.
    """
    if _get_replication_method_from_meta(schema_meta) == "INCREMENTAL":
        return schema_meta[0].get("metadata").get("valid-replication-keys")[0]
    return None


class Interval(Enum):
    """
    Enum representing time interval for making API calls.
    """

    HOUR = 1
    DAY = 24
    WEEK = 24 * 7
    MONTH = 24 * 31


class DixaURL(Enum):
    """
    Enum representing the Dixa base url API variants.
    """

    EXPORTS = "https://exports.dixa.io"
    INTEGRATIONS = "https://dev.dixa.io"
