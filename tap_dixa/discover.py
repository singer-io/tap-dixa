""" Module providing disovery method of tap-dixa"""
import json
from datetime import datetime, timedelta, timezone
import singer
from singer import metadata
from singer.catalog import Catalog
from tap_dixa.streams import STREAMS
from tap_dixa.client import Client
from tap_dixa.exceptions import DixaClient401Error
from tap_dixa.helpers import (
    _get_key_properties_from_meta,
    _get_replication_key_from_meta,
    _get_replication_method_from_meta,
    datetime_to_unix_ms,
    date_to_rfc3339,
    get_abs_path,
    check_stream_access,
    DixaURL
)

LOGGER = singer.get_logger()


def _get_probe_params(stream_class):
    """
    Returns minimal params for probing a stream endpoint during discovery.
    Uses a 1-second window 1 day in the past to minimise data returned
    while still producing a valid request that exercises authentication.
    """
    end_dt = datetime.now(timezone.utc) - timedelta(days=1)
    start_dt = end_dt - timedelta(seconds=1)

    if stream_class.base_url == DixaURL.INTEGRATIONS.value:
        # ActivityLogs expects RFC-3339 string params
        return {
            "created_after": date_to_rfc3339(start_dt.isoformat()),
            "created_before": date_to_rfc3339(end_dt.isoformat()),
        }
    else:
        # Conversations / Messages expect unix-ms integer params
        return {
            "created_after": datetime_to_unix_ms(start_dt),
            "created_before": datetime_to_unix_ms(end_dt),
        }


def _check_stream_access(client, stream_name, stream_class) -> bool:
    """
    Probes a stream endpoint to verify the API token has access.
    Returns True if the stream is accessible, False if a 401 Unauthorized
    response is returned. Any other error (e.g. 400 / 422 from the minimal
    params) means the endpoint is reachable and auth is valid, so True is
    returned in those cases as well.
    """
    params = _get_probe_params(stream_class)
    return check_stream_access(
        stream_name,
        probe_fn=lambda: client.get(
            base_url=stream_class.base_url,
            endpoint=stream_class.endpoint,
            params=params,
        ),
        auth_error_types=DixaClient401Error,
        fallback_accessible=True,
    )


def get_schemas():
    """
    Builds the singer schema and metadata dictionaries.
    """

    schemas = {}
    schemas_metadata = {}

    for stream_name, stream_object in STREAMS.items():

        schema_path = get_abs_path(f"schemas/{stream_name}.json")
        with open(schema_path) as file:
            schema = json.load(file)

        if stream_object.replication_method == "INCREMENTAL":
            replication_keys = stream_object.valid_replication_keys
        else:
            replication_keys = None

        meta = metadata.get_standard_metadata(schema=schema,
                                              key_properties=stream_object.key_properties,
                                              replication_method=stream_object.replication_method,
                                              valid_replication_keys=replication_keys,)

        meta = metadata.to_map(meta)

        if replication_keys:
            for replication_key in replication_keys:
                meta = metadata.write(meta,
                                      ("properties", replication_key),
                                      "inclusion",
                                      "automatic")

        meta = metadata.to_list(meta)

        schemas[stream_name] = schema
        schemas_metadata[stream_name] = meta

    return schemas, schemas_metadata


def discover(config: dict):
    """
    Builds the singer catalog for all the streams in the schemas directory.
    Requires config credentials — raises ValueError if not provided.
    Probes each stream endpoint inline while building the catalog; streams
    that return 401 Unauthorized are excluded from the catalog.
    """

    if not config or not config.get("api_token"):
        raise ValueError("'api_token' is required in the config to run discovery.")

    schemas, schemas_metadata = get_schemas()
    streams = []
    client = Client(config["api_token"])

    for stream_name, stream_class in STREAMS.items():
        if not _check_stream_access(client, stream_name, stream_class):
            LOGGER.warning(
                "Skipping stream '%s' — not accessible with the provided credentials.",
                stream_name,
            )
            continue

        schema = schemas[stream_name]
        schema_meta = schemas_metadata[stream_name]

        catalog_entry = {
            "stream": stream_name,
            "tap_stream_id": stream_name,
            "schema": schema,
            "key_properties": _get_key_properties_from_meta(schema_meta),
            "replication_method": _get_replication_method_from_meta(schema_meta),
            "replication_key": _get_replication_key_from_meta(schema_meta),
            "metadata": schema_meta,
        }

        streams.append(catalog_entry)

    return Catalog.from_dict({"streams": streams})
