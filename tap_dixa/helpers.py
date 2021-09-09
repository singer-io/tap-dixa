import datetime
from typing import Iterator

import singer


def unix_ms_to_date(ms: int) -> str:
    """
    Converts unix timestamp in milliseconds to ISO 8601 date string.

    :param ms: unix timestamp in milliseconds
    :return: ISO 8601 date string
    """
    return datetime.datetime.fromtimestamp(ms/1000).replace(microsecond=0).isoformat()


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
    return {
        'csids': ','.join(map(str, csids))
    }


def chunks(arr, chunk_size=10):
    """
    Takes an n-sized array and chunks it into smaller sizes that are
    yielded until full array has been iterated over.

    :param arr: n-sized list of conversation IDs
    :param chunk_size: the list size of each chunk
    :return: a chunk sized array
    """
    for i in range(0, len(arr), chunk_size):
        yield arr[i:i + chunk_size]


def date_to_rfc3339(date: str) -> str:
    """Converts date to rfc 3339"""
    d = singer.utils.strptime_to_utc(date)

    return d.strftime('%Y-%m-%dT%H:%M:%SZ')
