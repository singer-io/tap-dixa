import datetime


import singer
from ..helpers import datetime_to_unix_ms,unix_ms_to_date_utc,DixaURL
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

    def get_bookmark(self,state :dict,config: dict) ->int:
        """
        A wrapper for singer.get_bookmark to deal with backward compatibility for bookmark values.
        :param state: A dictionary representing singer state
        :param config: A dictionary containing tap config data
        :return: epoch timestamp in the form of a int datatype
        """
        bookmark = singer.get_bookmark(state, self.tap_stream_id, self.replication_key, False)
        if not bookmark:
            # get previous bookmark value if the current dosent exists or default to start date
            _ = singer.get_bookmark(state, self.tap_stream_id,self.old_replication_key, config["start_date"])   
            return datetime_to_unix_ms(singer.utils.strptime_to_utc(_))
        return bookmark

    def sync(self, state: dict, stream_schema: dict, stream_metadata: dict, config: dict, transformer: singer.Transformer) -> dict:
        """
        The sync logic for an incremental stream.

        :param state: A dictionary representing singer state
        :param stream_schema: A dictionary containing the stream schema
        :param stream_metadata: A dictionnary containing stream metadata
        :param config: A dictionary containing tap config data
        :param transformer: A singer Transformer object
        :return: State data in the form of a dictionary
        """
        if config.get("interval"):
            self.set_interval(config.get("interval"))
        start_date_epoch = self.get_bookmark(state,config)
        # bookmark_datetime = singer.utils.strptime_to_utc(start_date)
        max_datetime = bookmark_datetime = start_date_epoch

        with singer.metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records(bookmark_datetime, config = config):
                transformed_record = transformer.transform(
                    record, stream_schema, stream_metadata)
                record_datetime = transformed_record[self.replication_key]
                if record_datetime >= bookmark_datetime:
                    singer.write_record(self.tap_stream_id, transformed_record)
                    counter.increment()
                    max_datetime = max(record_datetime, max_datetime)

            bookmark_date = max_datetime

        state = singer.write_bookmark(state, self.tap_stream_id, self.replication_key, bookmark_date)
        singer.write_state(state)
        return state



    # pylint: disable=signature-differs
    def get_records(self, start_date :int, config: dict = {}):
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
