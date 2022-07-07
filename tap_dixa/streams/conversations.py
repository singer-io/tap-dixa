import datetime

import singer

from tap_dixa.helpers import datetime_to_unix_ms, unix_ms_to_date, DixaURL
from tap_dixa.streams.abstracts import IncrementalStream


class Conversations(IncrementalStream):
    """
    Get conversations from the Dixa platform.
    """

    tap_stream_id = "conversations"
    key_properties = ["id"]
    replication_key = "updated_at_datestring"
    valid_replication_keys = ["updated_at_datestring"]
    base_url = DixaURL.EXPORTS.value
    endpoint = "/v1/conversation_export"

    # pylint: disable=signature-differs
    def get_records(self, start_date, config: dict = {}):
        updated_after = start_date
        end_dt = singer.utils.now()
        add_interval = datetime.timedelta(hours=self.get_interval())
        loop = True

        while loop:
            if (updated_after + add_interval) < end_dt:
                updated_before = updated_after + add_interval
            else:
                loop = False
                updated_before = end_dt

            start = datetime_to_unix_ms(updated_after)
            end = datetime_to_unix_ms(updated_before)

            params = {"updated_after": start, "updated_before": end}
            response = self.client.get(self.base_url, self.endpoint, params=params)
            for record in response:
                record["updated_at_datestring"] = unix_ms_to_date(record["updated_at"])

            yield from response

            updated_after = updated_before + datetime.timedelta(milliseconds=1)