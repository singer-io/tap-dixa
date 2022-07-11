import datetime

import singer

from tap_dixa.helpers import datetime_to_unix_ms,unix_ms_to_date_utc, DixaURL
from .abstracts import IncrementalStream


class Messages(IncrementalStream):
    """
    Get messages from the Dixa platform.
    """

    tap_stream_id = "messages"
    key_properties = ["id"]
    replication_key = "created_at"
    valid_replication_keys = ["created_at"]
    old_replication_key = "updated_at_datestring"
    base_url = DixaURL.EXPORTS.value
    endpoint = "/v1/message_export"

    # pylint: disable=signature-differs
    def get_records(self, start_date :int):
        add_interval = datetime.timedelta(hours=self.get_interval())
        created_after = unix_ms_to_date_utc(start_date)
        end_dt,loop = singer.utils.now(),True


        while loop:
            if (created_after + add_interval) < end_dt:
                created_before = created_after + add_interval
            else:
                created_before,loop = end_dt,False

            params = {"created_after": datetime_to_unix_ms(created_after),"created_before": datetime_to_unix_ms(created_before)}
            response = self.client.get(self.base_url, self.endpoint, params=params)

            yield from response

            created_after = created_before + datetime.timedelta(milliseconds=1)
