import datetime

from tap_dixa.helpers import date_to_rfc3339, get_next_page_key, DixaURL
from .abstracts import IncrementalStream
import singer
from singer import metrics, Transformer


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

    def sync(self, state: dict, stream_schema: dict, stream_metadata: dict, config: dict, transformer: Transformer) -> dict:
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
        start_date = singer.get_bookmark(
            state, self.tap_stream_id, self.replication_key, config["start_date"])
        bookmark_datetime = singer.utils.strptime_to_utc(start_date)
        max_datetime = bookmark_datetime

        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records(bookmark_datetime, config=config):
                transformed_record = transformer.transform(record, stream_schema, stream_metadata)
                record_datetime = singer.utils.strptime_to_utc(transformed_record[self.replication_key])
                if record_datetime >= bookmark_datetime:
                    singer.write_record(self.tap_stream_id, transformed_record)
                    counter.increment()
                    max_datetime = max(record_datetime, max_datetime)

            bookmark_date = singer.utils.strftime(max_datetime)

        state = singer.write_bookmark(
            state, self.tap_stream_id, self.replication_key, bookmark_date)
        singer.write_state(state)
        return state

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
