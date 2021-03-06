"""
Test tap pagination of streams
"""

from math import ceil
from tap_tester import runner, connections

from base import DixaBaseTest

class DixaPaginationTest(DixaBaseTest):

    @staticmethod
    def name():
        return "tap_tester_dixa_pagination_test"

    def get_properties(self, original: bool = True):
        """Configuration properties required for the tap."""
        return_value = {
            'start_date' : self.start_date,
            'page_size' : 10
        }
        if original:
            return return_value

        return_value["start_date"] = self.start_date
        return return_value

    def test_run(self):
        page_size = int(self.get_properties().get('page_size', self.page_size))
        conn_id = connections.ensure_connection(self)

        # Checking pagination for streams with enough data
        expected_streams = ["activity_logs"]
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # table and field selection
        test_catalogs = [catalog for catalog in found_catalogs
                         if catalog.get('stream_name') in expected_streams]

        self.perform_and_verify_table_and_field_selection(conn_id, test_catalogs)

        record_count_by_stream = self.run_and_verify_sync(conn_id)

        synced_records = runner.get_records_from_target_output()

        for stream in expected_streams:
            with self.subTest(stream=stream):
                # expected values
                expected_primary_keys = self.expected_primary_keys()

                # collect information for assertions from syncs 1 & 2 base on expected values
                record_count_sync = record_count_by_stream.get(stream, 0)
                primary_keys_list = [tuple(message.get('data').get(expected_pk)
                                           for expected_pk in expected_primary_keys[stream])
                                     for message in synced_records.get(stream).get('messages')
                                     if message.get('action') == 'upsert']

                # verify records are more than page size so multiple page is working
                # Chunk the replicated records (just primary keys) into expected pages
                pages = []
                page_count = ceil(len(primary_keys_list) / page_size)
                for page_index in range(page_count):
                    page_start = page_index * page_size
                    page_end = (page_index + 1) * page_size
                    pages.append(set(primary_keys_list[page_start:page_end]))

                # Verify by primary keys that data is unique for each page
                for current_index, current_page in enumerate(pages):
                    with self.subTest(current_page_primary_keys=current_page):

                        for other_index, other_page in enumerate(pages):
                            if current_index == other_index:
                                continue  # don't compare the page to itself

                            self.assertTrue(current_page.isdisjoint(other_page), msg=f'other_page_primary_keys={other_page}')