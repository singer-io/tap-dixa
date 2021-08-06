import datetime

import singer
from singer import Transformer, metrics

from tap_dixa.client import Client
from tap_dixa.helpers import create_csid_params, datetime_to_unix_ms, unix_ms_to_date

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


class IncrementalStream(BaseStream):
    """
    A child class of a base stream used to represent streams that use the
    INCREMENTAL replication method.

    :param client: The API client used extract records from the external source
    """
    replication_method = 'INCREMENTAL'
    batched = False

    def __init__(self, client):
        super().__init__(client)

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

    def get_records(self, start_date, is_parent=False):
        start_dt = singer.utils.strptime_to_utc(start_date)
        end_dt = start_dt + datetime.timedelta(days=31)
        start = datetime_to_unix_ms(start_dt)
        end = datetime_to_unix_ms(end_dt)

        params = {'created_before': end, 'created_after': start}
        response = self.client.get_conversations(params=params)

        if is_parent:
            yield (record['id'] for record in response)
        else:
            for record in response:
                record['updated_at_datestring'] = unix_ms_to_date(record['updated_at'])

            yield from response


class Messages(IncrementalStream):
    """
    Get messages from the Dixa platform.
    """
    tap_stream_id = 'messages'
    key_properties = ['id']
    replication_key = 'updated_at_datestring'
    valid_replication_keys = ['updated_at_datestring']

    def get_records(self, start_date, is_parent=False):
        start_dt = singer.utils.strptime_to_utc(start_date)
        end_dt = start_dt + datetime.timedelta(days=31)
        start = datetime_to_unix_ms(start_dt)
        end = datetime_to_unix_ms(end_dt)

        params = {'created_before': end, 'created_after': start}
        response = self.client.get_messages(params=params)

        for record in response:
            record['updated_at_datestring'] = unix_ms_to_date(end)

        yield from response


class ActivityLogs(IncrementalStream):
    """
    Get messages from the Dixa platform.
    """
    tap_stream_id = 'activity_logs'
    key_properties = ['id']
    replication_key = 'activity_timestamp'
    valid_replication_keys = ['activity_timestamp']
    parent = Conversations

    def get_records(self, start_date, is_parent=False):

        for conversation_ids in self.get_parent_data(start_date):
            params = create_csid_params(conversation_ids)
            response = self.client.get_activity_logs(params=params)

            yield from response['data']


STREAMS = {
    'conversations': Conversations,
    'messages': Messages,
    'activity_logs': ActivityLogs,
}
