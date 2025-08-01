import re

from tap_tester import connections, menagerie
from base import DixaBaseTest


class DixaDiscoveryTest(DixaBaseTest):

    def name(self):
        return "tap_tester_discovery_test"

    def test_run(self):
        """
        Testing that discovery creates the appropriate catalog with valid metadata.

        • Verify number of actual streams discovered match expected
        • Verify the stream names discovered were what we expect
        • Verify stream names follow naming convention
          streams should only have lowercase alphas and underscores
        • Verify there is only 1 top level breadcrumb
        • Verify there are no duplicate/conflicting metadata entries.
        • Verify primary key(s) match expectations.
        • Verify replication key(s) match expectations.
        • Verify the actual replication matches our expected replication method.
        • Verify that if there is a replication key we are doing INCREMENTAL otherwise FULL.
        • Verify that primary keys and replication keys have inclusion of automatic
        • Verify that all fields have inclusion of available metadata.
        • Verify all streams have inclusion of automatic
        """
        streams_to_test = self.expected_streams()

        conn_id = connections.ensure_connection(self)
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Verify number of actual streams discovered match expected
        found_catalog_names = {c['tap_stream_id'] for c in found_catalogs}
        actual_streams = {name for name in found_catalog_names}
        self.assertEqual(actual_streams, streams_to_test, msg="Expected streams not found in the catalog")

        # Verify stream names follow naming convention
        # streams should only have lowercase alphas and underscores
        found_catalog_names = {c['tap_stream_id'] for c in found_catalogs}
        self.assertTrue(all([re.fullmatch(r"[a-z_]+",  name) for name in found_catalog_names]),
                        msg="One or more streams don't follow standard naming")

        for stream in streams_to_test:
            with self.subTest(stream=stream):
                # verify there is only 1 top level breadcrumb
                catalog = next(iter([catalog for catalog in found_catalogs
                                     if catalog["stream_name"] == stream]))
                self.assertIsNotNone(catalog)

                # collect expected values
                expected_primary_fields = self.expected_primary_keys()[stream]
                expected_replication_fields = self.expected_replication_keys()[stream]
                expected_automatic_fields = expected_primary_fields | expected_replication_fields
                expected_replcation_method = self.expected_replication_method()[stream]

                # collect actual values
                schema_and_metadata = menagerie.get_annotated_schema(conn_id, catalog["stream_id"])
                metadata = schema_and_metadata["metadata"]
                stream_properties = [data for data in metadata if data.get("breadcrumb") == []]

                actual_fields = []
                for md_entry in metadata:
                    if md_entry['breadcrumb'] != []:
                        actual_fields.append(md_entry['breadcrumb'][1])

                actual_primary_keys = set(stream_properties[0].get("metadata", {self.PRIMARY_KEYS: []}).get(self.PRIMARY_KEYS, []))
                actual_replication_keys = set(stream_properties[0].get("metadata", {self.REPLICATION_KEYS: []}).get(self.REPLICATION_KEYS, []))
                actual_automatic_fields = set(item.get("breadcrumb", ["properties", None])[1] for item in metadata
                                              if item.get("metadata").get("inclusion") == "automatic")
                actual_replication_method = stream_properties[0].get("metadata", {self.REPLICATION_METHOD: None}).get(self.REPLICATION_METHOD)

                ##########################################################################
                ### metadata assertions
                ##########################################################################

                # verify there is only 1 top level breadcrumb in metadata
                self.assertTrue(len(stream_properties) == 1,
                                msg="There is NOT only one top level breadcrumb for {}".format(stream) + \
                                "\nstream_properties | {}".format(stream_properties))

                # Verify there are no duplicate/conflicting metadata entries.
                self.assertEqual(len(actual_fields), len(set(actual_fields)), msg = "duplicates in the metadata entries retrieved")

                # Verify primary key(s) match expectations.
                self.assertSetEqual(expected_primary_fields, actual_primary_keys)

                # Verify the actual replication matches our expected replication method.
                self.assertEqual(expected_replcation_method, actual_replication_method, msg="Replication method does not match with expectations")

                # Verify primary key(s) match expectations.
                self.assertSetEqual(expected_replication_fields, actual_replication_keys)

                # Verify that if there is a replication key we are doing INCREMENTAL otherwise FULL
                if expected_replication_fields:
                    self.assertEqual(self.INCREMENTAL, actual_replication_method)
                else:
                    self.assertEqual(self.FULL_TABLE, actual_replication_method)

                # Verify primary key(s) match expectations.
                self.assertSetEqual(expected_automatic_fields, actual_automatic_fields)

                # verify that primary keys are given the inclusion of automatic
                self.assertTrue(actual_primary_keys.issubset(actual_automatic_fields))

                # verify that replication keys are given the inclusion of automatic
                self.assertTrue(actual_replication_keys.issubset(actual_automatic_fields))
                
                # verify that all fields have inclusion of available metadata.
                self.assertTrue(
                    all({item.get("metadata").get("inclusion") == "available"
                         for item in metadata
                         if item.get("breadcrumb", []) != []
                         and item.get("breadcrumb", ["properties", None])[1]
                         not in actual_automatic_fields}),
                    msg="Not all non key properties are set to available in metadata")
