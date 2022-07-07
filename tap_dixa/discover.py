""" Module providing disovery method of tap-dixa"""
import json
from singer import metadata
from singer.catalog import Catalog
from tap_dixa.streams import STREAMS, ActivityLogs
from tap_dixa.client import Client
from tap_dixa.helpers import (
    _get_key_properties_from_meta,
    _get_replication_key_from_meta,
    _get_replication_method_from_meta,
    get_abs_path,
    DixaURL
)
from datetime import datetime

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
    """

    schemas, schemas_metadata = get_schemas()
    streams = []

    if config:
        #Token Validation check before making any api request
        #params : mock parameter values are given for api token validation
        Client(config["api_token"]).get(base_url=DixaURL.INTEGRATIONS.value,
                                        endpoint=ActivityLogs.endpoint,
                                        params={"created_after": datetime.today(),
                                        "created_before": datetime.now()})

    for schema_name, schema in schemas.items():
        schema_meta = schemas_metadata[schema_name]

        catalog_entry = {
            "stream": schema_name,
            "tap_stream_id": schema_name,
            "schema": schema,
            "key_properties": _get_key_properties_from_meta(schema_meta),
            "replication_method": _get_replication_method_from_meta(schema_meta),
            "replication_key": _get_replication_key_from_meta(schema_meta),
            "metadata": schema_meta,
        }

        streams.append(catalog_entry)

    return Catalog.from_dict({"streams": streams})
