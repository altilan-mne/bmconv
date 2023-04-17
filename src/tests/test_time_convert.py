import sys
import datetime
import exceptions
from time_convert import stamp_to_object
from time_convert import stamp_to_string
from time_convert import object_to_stamp


class TestTimeToObject:
    def test_epoch_type(self):
        """Return error if the epoch type is not valid"""
        self.timestamp = 13097921382951728
        self.epoch_type = 'Googl'   # incorrect epoch type, should be Google
        # testing
        try:
            self.dt = stamp_to_object(self.timestamp, self.epoch_type)
        except exceptions.BadEpochType as e:
            print('\nWrong input parameter:', e, file=sys.stderr)
        else:
            print('\nInput parameters are OK')

    def test_timestamp(self):
        """Return error if the timestamp is out of the valid Unix range"""
        self.timestamp = 1309792138295172   # incorrect timestamp
        self.epoch_type = 'Google'
        # testing
        try:
            self.dt = stamp_to_object(self.timestamp, self.epoch_type)
        except OSError as e:
            print('\nWrong input parameter: OSError', e.errno,
                  ', timestamp is out of Unix range', file=sys.stderr)
        else:
            print('\nInput parameters are OK')

    def test_stamp_to_object(self):
        """Return the calculated datetime object"""
        self.timestamp = 13097921382951728  # the Unix timestamp within the correct range
        self.epoch_type = 'Google'  # Right epoch type Google
        # testing
        try:
            self.dt = stamp_to_object(self.timestamp, self.epoch_type)
        except exceptions.BadEpochType as e:
            print('\nWrong input parameter:', e, file=sys.stderr)
        else:
            print('\nInput parameters are OK')
            assert str(self.dt) == '2016-01-22 07:29:42'

    def test_stamp_to_string(self):
        """Return a Unix format datestamp as a string"""
        self.timestamp = 13097921382951728  # the Unix timestamp within the correct range
        self.epoch_type = 'Google'  # Right epoch type Google

        self.date_string = stamp_to_string(self.timestamp, self.epoch_type)
        assert self.date_string == '2016-01-22T07:29:42'



class TestObjectToTime:
    def test_epoch_type(self):
        """Return error if the epoch type is not valid"""
        self.instance = datetime.datetime(2016, 1, 22, 10, 29, 42, 951728, tzinfo=datetime.timezone.utc)
        self.epoch_type = 'Googl'   # incorrect epoch type, should be Google
        # testing
        try:
            self.ts = object_to_stamp(self.instance, self.epoch_type)
        except exceptions.BadEpochType as e:
            print('\nWrong input parameter:', e, file=sys.stderr)
        else:
            print('\nInput parameters are OK')

    #  Timestamp is always in range for this converting, nothing to elem

    def test_object_to_stamp(self):
        """Return the calculated timestamp in the required format """
        self.instance = datetime.datetime(2016, 1, 22, 10, 29, 42, 951728, tzinfo=datetime.timezone.utc)
        self.epoch_type = 'Google'  # Right epoch type Google
        # testing
        try:
            self.ts = object_to_stamp(self.instance, self.epoch_type)
        except exceptions.BadEpochType as e:
            print('\nWrong input parameter:', e, file=sys.stderr)
        else:
            print('\nInput parameters are OK')
            assert self.ts == 13097932182951728
