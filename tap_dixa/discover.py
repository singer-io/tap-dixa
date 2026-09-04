""" Module providing discovery method of tap-dixa"""
import json
from datetime import datetime, timedelta, timezone
import singer
from singer import metadata
from singer.catalog import Catalog
from tap_dixa.streams import STREAMS
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
    """Return True if accessible, False on 401. Non-401 errors are re-raised."""
    params = _get_probe_params(stream_class)
    try:
        client.get(
            base_url=stream_class.base_url,
            endpoint=stream_class.endpoint,
            params=params,
        )
        return True
    except DixaClient401Error as e:
        LOGGER.warning(
                "Unauthorized Stream: %s, excluding from catalog. HTTP-Error-Message:'%s'",
                stream_class.tap_stream_id,
                str(e),
            )
        return False


def _is_redundant_probe(client, stream_class) -> bool:
    """Return True when stream probe duplicates a successful bootstrap access probe."""
    validated_probe = None
    if hasattr(client, "__dict__"):
        validated_probe = client.__dict__.get("_validated_probe")
    if not validated_probe:
        return False
    return validated_probe == (stream_class.base_url, stream_class.endpoint)


def _apply_access_checks(client, schemas: dict, schemas_metadata: dict) -> None:
    """Remove inaccessible streams from discovery results in place."""
    inaccessible_streams = []
    for stream_name, stream_class in STREAMS.items():
        if stream_name not in schemas:
            continue
        if _is_redundant_probe(client, stream_class):
            continue
        if not check_stream_access(client, stream_class):
            inaccessible_streams.append(stream_name)

    for stream_name in inaccessible_streams:
        schemas.pop(stream_name, None)
        schemas_metadata.pop(stream_name, None)

    accessible_streams = [s for s in STREAMS if s in schemas]

    if not accessible_streams:
        raise DixaClient401Error(
            "Error: The credentials do not have 'read' access to any supported streams."
        )
    if inaccessible_streams:
        LOGGER.warning(
            "Unauthorized streams excluded from catalog: %s",
            ", ".join(inaccessible_streams),
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


def discover(client):
    """
    Builds the singer catalog for all accessible streams.
    Access to each stream is verified using the provided client and streams
    the credentials cannot read are excluded from the returned catalog.
    """
    schemas, schemas_metadata = get_schemas()
    _apply_access_checks(client, schemas, schemas_metadata)

    streams = []

    for stream_name, stream_class in STREAMS.items():
        if stream_name not in schemas:
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
