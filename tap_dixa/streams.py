import datetime
from enum import Enum

import singer
from singer import Transformer, metrics

from tap_dixa.client import Client, DixaURL
from tap_dixa.helpers import (chunks, create_csid_params, datetime_to_unix_ms,
                              unix_ms_to_date)

LOGGER = singer.get_logger()

class BaseStream:
    """
    A base class representing singer streams.

    :param client: The API client used extract records from the external source
    """
    tap_stream_id = None
    replication_method = None
    replication_key = None
    key_properties = []
    valid_replication_keys = []
    params = {}
    parent = None
    endpoint = None
    base_url = None

    def __init__(self, client: Client):
        self.client = client

    def get_records(self, start_date: str = None, is_parent: bool = False) -> list:
        """
        Returns a list of records for that stream.

        :param start_date: The start date the stream should use
        :param is_parent: If true, may change the type of data
            that is returned for a child stream to consume
        :return: list of records
        """
        raise NotImplementedError("Child classes of BaseStream require implementation")

    def set_parameters(self, params: dict) -> None:
        """
        Sets or updates the `params` attribute of a class.

        :param params: Dictionary of parameters to set or update the class with
        """
        self.params = params

    def get_parent_data(self, start_date: str = None) -> list:
        """
        Returns a list of records from the parent stream.

        :param start_date: The tap start date
        :return: A list of records
        """
        parent = self.parent(self.client)
        return parent.get_records(start_date, is_parent=True)


class Interval(Enum):
    HOUR = 1
    DAY = 24
    WEEK = 24 * 7
    MONTH = 24 * 31


class InvalidInterval(Exception):
    pass


class IncrementalStream(BaseStream):
    """
    A child class of a base stream used to represent streams that use the
    INCREMENTAL replication method.

    :param client: The API client used extract records from the external source
    """
    replication_method = 'INCREMENTAL'
    batched = False
    interval = None

    def __init__(self, client):
        super().__init__(client)

    def set_interval(self, value):
        self.interval = value.upper()

    def get_interval(self):
        """
        Retrieves interval enum. Defaults to MONTH if no interval provided.
        :return: An enum for the interval to be used with the API
        """
        if self.interval:
            if hasattr(Interval, self.interval):
                return getattr(Interval, self.interval).value

            # If interval not part of enum, log message and throw error
            valid_intervals = set()
            for interval in dir(Interval):
                if not interval.startswith("__"):
                    valid_intervals.add(interval)

            # pylint: disable=logging-fstring-interpolation
            LOGGER.critical(f"provided interval '{self.interval}' is not "
                            f"in Interval set: {valid_intervals}")

            raise InvalidInterval('invalid interval provided')

        return Interval.MONTH.value

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
        if config.get('interval'):
            self.set_interval(config.get('interval'))
        start_date = singer.get_bookmark(state, self.tap_stream_id, self.replication_key, config['start_date'])
        max_record_value = start_date

        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records(start_date):
                transformed_record = transformer.transform(record, stream_schema, stream_metadata)
                record_replication_value = singer.utils.strptime_to_utc(transformed_record[self.replication_key])
                if record_replication_value >= singer.utils.strptime_to_utc(max_record_value):
                    singer.write_record(self.tap_stream_id, transformed_record)
                    counter.increment()
                    max_record_value = record_replication_value.isoformat()

        state = singer.write_bookmark(state, self.tap_stream_id, self.replication_key, max_record_value)
        singer.write_state(state)
        return state


class FullTableStream(BaseStream):
    """
    A child class of a base stream used to represent streams that use the
    FULL_TABLE replication method.

    :param client: The API client used extract records from the external source
    """
    replication_method = 'FULL_TABLE'

    def __init__(self, client):
        super().__init__(client)

    def sync(self, state: dict, stream_schema: dict, stream_metadata: dict, config: dict, transformer: Transformer) -> dict:
        """
        The sync logic for an full table stream.

        :param state: A dictionary representing singer state
        :param stream_schema: A dictionary containing the stream schema
        :param stream_metadata: A dictionnary containing stream metadata
        :param config: A dictionary containing tap config data
        :param transformer: A singer Transformer object
        :return: State data in the form of a dictionary
        """
        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records(config):
                transformed_record = transformer.transform(record, stream_schema, stream_metadata)
                singer.write_record(self.tap_stream_id, transformed_record)
                counter.increment()

        singer.write_state(state)
        return state


class Conversations(IncrementalStream):
    """
    Get conversations from the Dixa platform.
    """
    tap_stream_id = 'conversations'
    key_properties = ['id']
    replication_key = 'updated_at_datestring'
    valid_replication_keys = ['updated_at_datestring']
    base_url = DixaURL.exports.value
    endpoint = '/v1/conversation_export'

    def get_records(self, start_date, is_parent=False):
        created_after = singer.utils.strptime_to_utc(start_date)
        end_dt = singer.utils.now()
        add_interval = datetime.timedelta(hours=self.get_interval())
        loop = True

        while loop:
            if (created_after + add_interval) < end_dt:
                created_before = created_after + add_interval
            else:
                loop = False
                created_before = end_dt

            start = datetime_to_unix_ms(created_after)
            end = datetime_to_unix_ms(created_before)

            params = {'created_before': end, 'created_after': start}
            response = self.client.get(self.base_url, self.endpoint, params=params)

            if is_parent:
                # Chunk into max 10 csids to avoid 422 error
                # on activity logs endpoint
                conversation_ids = [record['id'] for record in response]
                yield from chunks(conversation_ids)
            else:
                for record in response:
                    record['updated_at_datestring'] = unix_ms_to_date(record['updated_at'])

                yield from response

            created_after = created_before


class Messages(IncrementalStream):
    """
    Get messages from the Dixa platform.
    """
    tap_stream_id = 'messages'
    key_properties = ['id']
    replication_key = 'updated_at_datestring'
    valid_replication_keys = ['updated_at_datestring']
    base_url = DixaURL.exports.value
    endpoint = '/v1/message_export'

    def get_records(self, start_date, is_parent=False):
        created_after = singer.utils.strptime_to_utc(start_date)
        end_dt = singer.utils.now()
        add_interval = datetime.timedelta(hours=self.get_interval())
        loop = True

        while loop:
            if (created_after + add_interval) < end_dt:
                created_before = created_after + add_interval
            else:
                loop = False
                created_before = end_dt

            start = datetime_to_unix_ms(created_after)
            end = datetime_to_unix_ms(created_before)

            params = {'created_before': end, 'created_after': start}
            response = self.client.get(self.base_url, self.endpoint, params=params)

            for record in response:
                record['updated_at_datestring'] = unix_ms_to_date(end)

            yield from response

            created_after = created_before


class ActivityLogs(IncrementalStream):
    """
    Get messages from the Dixa platform.
    """
    tap_stream_id = 'activity_logs'
    key_properties = ['id']
    replication_key = 'activity_timestamp'
    valid_replication_keys = ['activity_timestamp']
    parent = Conversations
    base_url = DixaURL.integrations.value
    endpoint = '/v1/conversations/activitylog'

    def get_records(self, start_date, is_parent=False):

        for conversation_ids in self.get_parent_data(start_date):
            params = create_csid_params(conversation_ids)
            response = self.client.get(self.base_url, self.endpoint, params=params)

            yield from response.get('data', [])


STREAMS = {
    'conversations': Conversations,
    'messages': Messages,
    'activity_logs': ActivityLogs,
}
