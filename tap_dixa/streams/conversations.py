import datetime


import singer
from tap_dixa.helpers import datetime_to_unix_ms,unix_ms_to_date_utc,DixaURL
from .abstracts import IncrementalStream


class Conversations(IncrementalStream):
    """
    Get conversations from the Dixa platform.
    """

    tap_stream_id = "conversations"
    key_properties = ["id"]
    replication_key = "updated_at"
    valid_replication_keys = ["updated_at"]
    old_replication_key = "updated_at_datestring"
    base_url = DixaURL.EXPORTS.value
    endpoint = "/v1/conversation_export"


    # pylint: disable=signature-differs
    def get_records(self, start_date :int):
        add_interval = datetime.timedelta(hours=self.get_interval())
        updated_after = unix_ms_to_date_utc(start_date)
        end_dt,loop = singer.utils.now(),True

        while loop:
            if (updated_after + add_interval) < end_dt:
                updated_before = updated_after + add_interval
            else:
                updated_before,loop = end_dt,False

            params = {"updated_after": datetime_to_unix_ms(updated_after),"updated_before": datetime_to_unix_ms(updated_before)}
            response = self.client.get(self.base_url, self.endpoint, params=params)

            yield from response

            updated_after = updated_before + datetime.timedelta(milliseconds=1)
