import datetime

from ..helpers import date_to_rfc3339, get_next_page_key, DixaURL
from .abstracts import IncrementalStream


class ActivityLogs(IncrementalStream):
    """
    Get activity logs from the Dixa platform.
    """

    tap_stream_id = "activity_logs"
    key_properties = ["id"]
    replication_key = "activityTimestamp"
    valid_replication_keys = ["activityTimestamp"]
    base_url = DixaURL.INTEGRATIONS.value
    endpoint = "/v1/conversations/activitylog"

    # pylint: disable=signature-differs
    def get_records(self, start_date, config: dict = {}):
        max_limit = config.get("page_size", 10_000)
        loop = True
        page_key = None
        from_datetime = date_to_rfc3339(start_date.isoformat())
        to_datetime = date_to_rfc3339(datetime.datetime.utcnow().isoformat())

        params = {
            "fromDatetime": from_datetime,
            "toDatetime": to_datetime,
            "pageKey": page_key,
            "pageLimit": max_limit,
        }

        while loop:

            response = self.client.get(self.base_url, self.endpoint, params=params)

            # Extract data and pageKey
            data = response.get("data", [])
            meta = response.get("meta") or {}
            next_page = meta.get("next")
            page_key = get_next_page_key(next_page)

            # Update params with pageKey
            params.update({"pageKey": page_key.get("pageKey")})

            # Change switch to exit while loop if pageKey returns None
            loop = True if page_key.get("pageKey") else False

            yield from data
