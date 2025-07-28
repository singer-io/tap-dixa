import unittest
from tap_dixa.streams.abstracts import IncrementalStream
from tap_dixa.exceptions import InvalidInterval
from tap_dixa.helpers import Interval


class test_increemtalstream(unittest.TestCase):
    """
    Testing that valid value is passed in the interval attribute of config.json
    Allowed values in interval are Hour,Day, Week and Month
    """
    def test_invalid_value_of_interval(self):
        try :
            config = {'interval': "INVALID"}
            IncrementalStream.set_interval(self,value=config['interval'])
            response = IncrementalStream.get_interval(self)
        except InvalidInterval as e :
            expected_error_message = "invalid interval provided"
            self.assertEqual(str(e), expected_error_message)

    def test_valid_value_of_interval(self):
        try :
            config = {'interval': "MONTH"}
            IncrementalStream.set_interval(self,value=config['interval'])
            response = IncrementalStream.get_interval(self)
        except InvalidInterval as e :
            expected_error_message = "invalid interval provided"
        self.assertEqual(response,Interval.MONTH.value)
