import datetime
from abc import ABC, abstractmethod

import singer
from singer import Transformer, metrics

from ..client import Client
from ..exceptions import InvalidInterval
from ..helpers import Interval

LOGGER = singer.get_logger()


class BaseStream(ABC):
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
    endpoint = None
    base_url = None

    def __init__(self, client: Client):
        self.client = client

    @abstractmethod
    def get_records(self, start_date: datetime.datetime = None) -> list:
        """
        Returns a list of records for that stream.

        :param start_date: The start date datetime object the stream should use
        :param is_parent: If true, may change the type of data
            that is returned for a child stream to consume
        :return: list of records
        """
        raise NotImplementedError(
            "Child classes of BaseStream require implementation")

    def set_parameters(self, params: dict) -> None:
        """
        Sets or updates the `params` attribute of a class.

        :param params: Dictionary of parameters to set or update the class with
        """
        self.params = params


class IncrementalStream(BaseStream):
    """
    A child class of a base stream used to represent streams that use the
    INCREMENTAL replication method.

    :param client: The API client used extract records from the external source
    """

    replication_method = "INCREMENTAL"
    batched = False
    interval = None

    def set_interval(self, value):
        """
        Sets the interval attribute.

        :param value: The interval string value
        """
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
            LOGGER.critical(
                f"provided interval '{self.interval}' is not " f"in Interval set: {valid_intervals}")

            raise InvalidInterval("invalid interval provided")

        return Interval.MONTH.value

    def sync(
        self, state: dict, stream_schema: dict, stream_metadata: dict, config: dict, transformer: Transformer
    ) -> dict:
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
            for record in self.get_records(bookmark_datetime):
                transformed_record = transformer.transform(
                    record, stream_schema, stream_metadata)
                record_datetime = singer.utils.strptime_to_utc(
                    transformed_record[self.replication_key])
                if record_datetime >= bookmark_datetime:
                    singer.write_record(self.tap_stream_id, transformed_record)
                    counter.increment()
                    max_datetime = max(record_datetime, max_datetime)

            bookmark_date = singer.utils.strftime(max_datetime)

        state = singer.write_bookmark(
            state, self.tap_stream_id, self.replication_key, bookmark_date)
        singer.write_state(state)
        return state


class FullTableStream(BaseStream):
    """
    A child class of a base stream used to represent streams that use the
    FULL_TABLE replication method.

    :param client: The API client used extract records from the external source
    """

    replication_method = "FULL_TABLE"

    def sync(
        self, state: dict, stream_schema: dict, stream_metadata: dict, config: dict, transformer: Transformer
    ) -> dict:
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
                transformed_record = transformer.transform(
                    record, stream_schema, stream_metadata)
                singer.write_record(self.tap_stream_id, transformed_record)
                counter.increment()

        singer.write_state(state)
        return state
