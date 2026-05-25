""" helper methods required for tap-dixa"""
import datetime
import os
import pytz

from enum import Enum
from typing import Iterator
from urllib.parse import parse_qsl, urlparse

import singer
from singer import utils

LOGGER = singer.get_logger()


def check_stream_access(stream_name, probe_fn, auth_error_types, fallback_accessible=False):
    """
    Standard Singer tap stream access checker.

    Calls ``probe_fn()`` (a zero-argument callable that performs a lightweight
    API request) to verify whether the configured credentials can reach the
    stream's endpoint. Streams that raise an auth error are excluded from the
    catalog by returning False.

    :param stream_name: Stream name used in log messages.
    :param probe_fn: Zero-argument callable that performs the API probe request.
    :param auth_error_types: Exception type or tuple of exception types that
                             indicate an authentication / authorization failure
                             (e.g. 401 / 403). These cause the function to
                             return False and log a warning.
    :param fallback_accessible: When True, any exception *other* than
                                ``auth_error_types`` is treated as "endpoint
                                reachable, auth OK" (e.g. a 400 caused by
                                intentionally minimal probe params) and the
                                function returns True. When False (default)
                                unexpected exceptions are re-raised.
    :return: True if the stream is accessible, False if an auth error is raised.
    """
    try:
        probe_fn()
        LOGGER.info("Stream '%s' is accessible.", stream_name)
        return True
    except auth_error_types:
        LOGGER.warning(
            "Stream '%s' is not accessible with the provided credentials. "
            "It will be excluded from the catalog.",
            stream_name,
        )
        return False
    except Exception:  # pylint: disable=broad-except
        if fallback_accessible:
            LOGGER.info("Stream '%s' endpoint reachable (auth OK).", stream_name)
            return True
        raise

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
