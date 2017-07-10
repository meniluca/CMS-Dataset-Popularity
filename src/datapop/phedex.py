"""
Collection of Classes for CMS PheDEx datasets management
"""

from datetime import date
from re import match as regex_match
from pyspark.sql.types import StructType

def get_day_folder_path(suffix_format, prefix, year, month, day):
    """Constructs and returns the day path string"""
    assert regex_match(r'^(\d\d)?\d\d$', year), "%s is not a valid year" % year
    assert regex_match(r'^(\d)?\d$', month), "%s is not a valid month" % month
    assert regex_match(r'^(\d)?\d$', day), "%s is not a valid day" % day
    day = date(year, month, day)
    return suffix_format % (prefix, day.strftime("%Y-%m-%d"))

class Blocks(object):
    """docstring for Blocks."""
    def __init__(self, prefix):
        self.prefix = prefix

    @staticmethod
    def get_schema():
        """Returns Spark DataFrame schema for block-replicas-snapshots."""
        return StructType()\
            .add("transfer_ts", "double")\
            .add("dataset_name", "string")\
            .add("dataset_id", "integer")\
            .add("dataset_is_open", "string")\
            .add("dataset_time_create", "double")\
            .add("dataset_time_update", "string")\
            .add("block_name", "string")\
            .add("block_id", "integer")\
            .add("block_files", "integer")\
            .add("block_bytes", "long")\
            .add("block_is_open", "string")\
            .add("block_time_create", "double")\
            .add("block_time_update", "double")\
            .add("node_name", "string")\
            .add("node_id", "integer")\
            .add("is_active", "string")\
            .add("src_files", "integer")\
            .add("src_bytes", "long")\
            .add("dest_files", "integer")\
            .add("dest_bytes", "long")\
            .add("node_files", "integer")\
            .add("node_bytes", "long")\
            .add("xfer_files", "integer")\
            .add("xfer_bytes", "long")\
            .add("is_custodial", "string")\
            .add("user_group", "string")\
            .add("replica_time_create", "double")\
            .add("replica_time_update", "double")

    def get_day_folder_path(self, year, month, day):
        """Constructs and returns the day path string"""
        get_day_folder_path("%s/time=%s_*/*", self.prefix, year, month, day)

    def get_folder_list(self, days_list):
        """Returns list of folder from date objects list"""
        assert all(isinstance(day, date) for day in days_list)
        return [self.get_day_folder_path(year=day.strftime("%Y"), \
            month=day.strftime("%m"), day=day.strftime("%d")) for day in days_list]


class Catalog(object):
    """docstring for Catalog."""
    def __init__(self, prefix):
        self.prefix = prefix

    @staticmethod
    def get_schema():
        """Returns Spark DataFrame schema for catalog."""
        return StructType() \
            .add("dataset_name", "string") \
            .add("dataset_id", "integer") \
            .add("dataset_is_open", "string") \
            .add("dataset_time_create", "double") \
            .add("block_name", "string") \
            .add("block_id", "integer") \
            .add("block_time_create", "double") \
            .add("block_is_open", "string") \
            .add("file_lfn", "string") \
            .add("file_id", "integer") \
            .add("filesize", "long") \
            .add("checksum", "string") \
            .add("file_time_create", "double")

    def get_day_folder_path(self, year, month, day):
        """Constructs and returns the day path string"""
        get_day_folder_path("%s/date=%s/*", self.prefix, year, month, day)

    def get_folder_list(self, days_list):
        """Returns list of folder from date objects list"""
        assert all(isinstance(day, date) for day in days_list)
        return [self.get_day_folder_path(year=day.strftime("%Y"), \
            month=day.strftime("%m"), day=day.strftime("%d")) for day in days_list]
