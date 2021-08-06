import datetime
import time

import pytz


def unix_ms_to_date(ms: int) -> datetime.datetime:
    return datetime.datetime.fromtimestamp(ms/1000, pytz.UTC).isoformat()


def datetime_to_unix_ms(datetime_obj: datetime.datetime) -> int:
    return int(time.mktime(datetime_obj.timetuple()) * 1000)
