""" Module providing discovery method of tap-dixa"""
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

    stream_config = {
        "activity_logs": {
            "start_key": "fromDatetime",
            "end_key": "toDatetime",
            "formatter": lambda dt: date_to_rfc3339(dt.isoformat()),
        },
        "conversations": {
            "start_key": "updated_after",
            "end_key": "updated_before",
            "formatter": lambda dt: datetime_to_unix_ms(dt.replace(tzinfo=None)),
        },
        "default": {
            "start_key": "created_after",
            "end_key": "created_before",
            "formatter": lambda dt: datetime_to_unix_ms(dt.replace(tzinfo=None)),
        },
    }

    config = stream_config.get(
        stream_class.tap_stream_id,
        stream_config["default"],
    )

    formatter = config["formatter"]

    return {
        config["start_key"]: formatter(start_dt),
        config["end_key"]: formatter(end_dt),
    }


def check_stream_access(client, stream_class) -> bool:
    """
    Probes a stream endpoint to verify the API token has access.
    Returns True if the stream is accessible, False if a 401 Unauthorized
    response is returned.
    Any other error (e.g. 400/422 from minimal probe params) is treated as
    accessible — the server processed the request, so auth is valid.
    """
    params = _get_probe_params(stream_class)
    try:
        client.get(
            base_url=stream_class.base_url,
            endpoint=stream_class.endpoint,
            params=params,
        )
        return True
    except DixaClient401Error:
        return False
    except Exception:
        # Non-auth errors (e.g. 400 Bad Request, 422 Unprocessable) mean the
        # server responded — credentials are valid, stream is accessible.
        LOGGER.warning(
            "Stream '%s' probe returned a non-auth error; assuming accessible.",
            stream_class.tap_stream_id,
        )
        return True


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
        if not check_stream_access(client, stream_class):
            LOGGER.warning(
                "Stream '%s' will be excluded from the catalog due to insufficient permissions.",
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

    if not streams:
        raise Exception(
            "No streams are accessible with the provided API token. "
            "The token may be invalid, expired, or lack the required permissions."
        )

    return Catalog.from_dict({"streams": streams})
