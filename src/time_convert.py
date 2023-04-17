"""Module to convert a timestamp of different formats
to the object Datetime and visa versa.

The Windows Time epoch_type is the number of 100ns-es since January 1, 1601.
The Chrome epoch_type is the number of microseconds since the same date, and thus 1/10 as large.
JavaScript - Unix in milliseconds, PubNub (17 digits) - Unix / 10,000,000
To convert a Chrome timestamp to and from the Unix epoch,
you must convert to second and compensate for the difference
between the two base date-times (11644473600).
Information from https://stackoverflow.com/questions/539900/google-bookmark-export-date-format.
Supported formats (case-insensitive): Unix, JavaScript, Windows, Google, PubNub
"""
from datetime import datetime
from datetime import timezone

import exceptions


DELTA = 11644473600  # difference in sec between Unix and Windows base date-times


def stamp_to_object(timestamp: int, epoch_type: str) -> datetime:
    """Convert a timestamp of non-Unix formats to the Unix format

    :exceptions: raise BadEpochType if input format is wrong

    :param timestamp: timestamp of non-Unix formats
    :param epoch_type: input format: 'windows', 'google', 'javascript', 'unix'
    :return: object of the class datetime
    """
    epoch_type = epoch_type.lower()  # to lower case

    if epoch_type == 'windows':
        timestamp = timestamp // 10_000_000 - DELTA  # from Windows format to Unix
    elif epoch_type == 'google':
        timestamp = timestamp // 1_000_000 - DELTA   # from Google format to Unix
    elif epoch_type == 'javascript':
        timestamp = timestamp // 1_000   # from Javascript format to Unix
    elif epoch_type == 'unix':     # is already the Unix format
        pass
    else:
        """Raise bad epoch type error"""
        raise exceptions.BadEpochType(epoch_type)

    return datetime.utcfromtimestamp(timestamp)  # return not local but UTC datetime to avoid local settings

def stamp_to_string(timestamp: int, epoch_type: str) -> str:
    """Convert a timestamp of non-Unix formats to the Unix format

    :exceptions: raise BadEpochType if input format is wrong

    :param timestamp: timestamp of non-Unix formats
    :param epoch_type: input format: 'windows', 'google', 'javascript', 'unix'
    :return: a string representing the date and time in ISO 8601 format
    """
    full_datetime = stamp_to_object(timestamp, epoch_type)  # get the datetime object
    short_datatime = full_datetime.replace(microsecond=0)  # trim microseconds
    return datetime.isoformat(short_datatime)  # return the unix format string


def object_to_stamp(datetime_instance: datetime, epoch_type: str) -> int:
    """Convert a timestamp of the Unix format to non-Unix formats to the Unix formats

    :exceptions: raise BadEpochType if input format is wrong

    :param datetime_instance: object of the class datetime
    :param epoch_type: output format: 'windows', 'google', 'javascript', 'unix'
    :return: converted timestamp as integer
    """
    epoch_type = epoch_type.lower()  # to lower case

    if epoch_type == 'windows':
        timestamp = (int(datetime_instance.timestamp()) + DELTA) * 10_000_000 + \
                     datetime_instance.microsecond * 10  # from Unix format to Windows format
    elif epoch_type == 'google':
        timestamp = (int(datetime_instance.timestamp()) + DELTA) * 1_000_000 + \
                     datetime_instance.microsecond  # from Unix format to Google format
    elif epoch_type == 'javascript':
        timestamp = int(datetime_instance.timestamp()) * 1_000 + \
                    datetime_instance.microsecond // 1_000  # from Unix format to Javascript format
    elif epoch_type == 'unix':  # is already the Unix format
        timestamp = int(datetime_instance.timestamp())
    else:
        """Raise bad epoch type error"""
        raise exceptions.BadEpochType(epoch_type)
    return timestamp


def main():
    """Demonstration of the module functionality"""
    # to Unix format
    epoch_type = 'Google'
    timestamp = 13097921382951728
    res = stamp_to_object(timestamp, epoch_type)
    print(res)
    # from Unix format
    epoch_type = 'Google'
    datetime_instance = datetime(2016, 1, 22, 10, 29, 42, 951728, tzinfo=timezone.utc)
    print(datetime_instance.microsecond)
    result = object_to_stamp(datetime_instance, epoch_type)
    print(result)

if __name__ == '__main__':
    main()

"""13097921382951728 == 2016-01-22 07:29:42.951728 for UTC timezone"""
