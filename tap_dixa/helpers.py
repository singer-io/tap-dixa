import datetime
import time


def unix_ms_to_datetime(ms: int) -> datetime.datetime:
    return datetime.datetime.fromtimestamp(ms/1000)


def datetime_to_unix_ms(datetime_obj: datetime.datetime) -> int:
    return int(time.mktime(datetime_obj.timetuple()) * 1000)
