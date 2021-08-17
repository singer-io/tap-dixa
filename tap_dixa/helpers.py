import datetime
import time
from typing import Iterator

import pytz


def unix_ms_to_date(ms: int) -> datetime.datetime:
    return datetime.datetime.fromtimestamp(ms/1000).replace(microsecond=0).isoformat()


def datetime_to_unix_ms(datetime_obj: datetime.datetime) -> int:
    return int(datetime_obj.timestamp() * 1000)


def create_csid_params(csids: Iterator) -> dict:
    return {
        'csids': ','.join(map(str, csids))
    }


def chunks(arr, chunk_size=10):
    for i in range(0, len(arr), chunk_size):
        yield arr[i:i + chunk_size]
