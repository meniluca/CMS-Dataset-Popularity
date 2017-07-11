"""
Collection of Classes for CMS PheDEx datasets management
"""

from datetime import date, datetime
from re import match as regex_match
from pyspark import StorageLevel
from pyspark.sql.types import StructType
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType
from pyspark.sql.functions import Column as col

def get_day_folder_path(prefix, suffix_format, year, month, day):
    """Constructs and returns the day path string"""
    assert regex_match(r'^(\d\d)?\d\d$', year), "%s is not a valid year" % year
    assert regex_match(r'^(\d)?\d$', month), "%s is not a valid month" % month
    assert regex_match(r'^(\d)?\d$', day), "%s is not a valid day" % day
    day = date(year, month, day)
    return suffix_format % (prefix, day.strftime("%Y-%m-%d"))

#
# Blocks class
#

class Blocks(object):
    """docstring for Blocks."""

    def __init__(self, prefix, days_list, suffix_format="%s/time=%s_*/*"):
        assert all(isinstance(day, date) for day in days_list)
        self.prefix = prefix
        self.suffix_format = suffix_format
        self.folders_list = [get_day_folder_path(prefix=self.prefix, suffix_format=self.suffix_format,\
            year=day.strftime("%Y"), month=day.strftime("%m"), day=day.strftime("%d")) for day in days_list]
        self.dataframe = None
        self.total_rows = None

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

    def set_dataframe(self, dataframe):
        """Link a Spark DataFrame to this class instance"""
        assert isinstance(dataframe, DataFrame), "The method requires a DataFrame"
        if self.dataframe:
            self.dataframe.unpersist()
            print "Replacing existing dataframe"
        is_y = udf(lambda column: True if column == 'y' else False, BooleanType())
        parse_timestamp = udf(lambda ts: datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d %T"))
        parse_string_timestamp = udf(lambda ts: \
            datetime.utcfromtimestamp(float(ts)).strftime("%Y-%m-%d %T") if ts != 'null' else None)
        self.dataframe = dataframe.withColumn("dataset_is_open", is_y(dataframe.dataset_is_open)) \
            .withColumn("block_is_open", is_y(dataframe.block_is_open)) \
            .withColumn("dataset_timestamp_create", parse_timestamp(dataframe.dataset_time_create)) \
            .withColumn("dataset_timestamp_update", parse_string_timestamp(dataframe.dataset_time_update)) \
            .withColumn("block_timestamp_create", parse_timestamp(dataframe.block_time_create)) \
            .withColumn("block_timestamp_update", parse_timestamp(dataframe.block_time_update)) \
            .withColumn("replica_timestamp_create", parse_timestamp(dataframe.replica_time_create)) \
            .withColumn("replica_timestamp_update", parse_timestamp(dataframe.replica_time_update))
        self.dataframe.persist(storageLevel=StorageLevel.MEMORY_AND_DISK)
        self.total_rows = dataframe.count()
        print "Total rows: %d" % self.total_rows

    def print_stats(self, dataframe=None):
        """Calculate top statistics using the Spark DataFrame passed as argument"""
        if dataframe:
            self.set_dataframe(dataframe)
        if not self.dataframe:
            raise RuntimeError("Missing dataframe")
        print "Blocks Replica Snapshot statistics"
        print "Total rows: %d" % self.total_rows
        print "- Top 10 DataSets:"
        self.dataframe.groupBy("dataset_id").count().sort(col("count").desc()).show(10)
        print "- Total distinct dataset_id: %d" % self.dataframe.select("dataset_id").distinct().count()
        print "- Total distinct block_id: %d" % self.dataframe.select("block_id").distinct().count()
        groupby_dataset_block = self.dataframe.groupBy("dataset_id", "block_id").count().sort(col("count").desc())
        print "- Total distinct dataset_id,block_id: %d" % groupby_dataset_block.count()
        print "- Top 10 DataSet,Block:"
        groupby_dataset_block.show(10)

#
# Catalog class
#

class Catalog(object):
    """docstring for Catalog."""

    def __init__(self, prefix, days_list, suffix_format="%s/date=%s/*"):
        assert all(isinstance(day, date) for day in days_list)
        self.prefix = prefix
        self.suffix_format = suffix_format
        self.folders_list = [get_day_folder_path(prefix=self.prefix, suffix_format=self.suffix_format,\
            year=day.strftime("%Y"), month=day.strftime("%m"), day=day.strftime("%d")) for day in days_list]
        self.dataframe = None
        self.total_rows = None

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

    def set_dataframe(self, dataframe):
        """Link a Spark DataFrame to this class instance"""
        assert isinstance(dataframe, DataFrame), "The method requires a DataFrame"
        if self.dataframe:
            self.dataframe.unpersist()
            print "Replacing existing dataframe"
        is_y = udf(lambda column: True if column == 'y' else False, BooleanType())
        parse_timestamp = udf(lambda ts: datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d %T"))
        self.dataframe = dataframe.withColumn("dataset_is_open", is_y(dataframe.dataset_is_open)) \
            .withColumn("block_is_open", is_y(dataframe.block_is_open)) \
            .withColumn("dataset_timestamp_create", parse_timestamp(dataframe.dataset_time_create)) \
            .withColumn("block_timestamp_create", parse_timestamp(dataframe.block_time_create)) \
            .withColumn("file_timestamp_create", parse_timestamp(dataframe.file_time_create))
        self.dataframe.persist(storageLevel=StorageLevel.MEMORY_AND_DISK)
        self.total_rows = dataframe.count()
        print "Total rows: %d" % self.total_rows

    def print_stats(self, dataframe=None):
        """Calculate top statistics using the Spark DataFrame passed as argument"""
        if dataframe:
            self.set_dataframe(dataframe)
        if not self.dataframe:
            raise RuntimeError("Missing dataframe")
        print "Blocks Replica Snapshot statistics"
        print "Total rows: %d" % self.total_rows
        print "- Top 10 DataSets:"
        self.dataframe.groupBy("dataset_id").count().sort(col("count").desc()).show(10)
        print "- Total distinct dataset_id: %d" % self.dataframe.select("dataset_id").distinct().count()
        print "- Total distinct block_id: %d" % self.dataframe.select("block_id").distinct().count()
        print "- Total distinct file_id: %d" % self.dataframe.select("file_id").distinct().count()
        groupby_dataset_block = self.dataframe.groupBy("dataset_id", "block_id").count().sort(col("count").desc())
        print "- Total distinct dataset_id,block_id: %d" % groupby_dataset_block.count()
        print "- Top 10 DataSet,Block:"
        groupby_dataset_block.show(10)
        print "- Top 10 oldest dataset_id:"
        self.dataframe.select("dataset_id", "dataset_timestamp_create") \
            .groupBy("dataset_id").agg({"dataset_timestamp_create":"min"}) \
            .orderBy(col("min(dataset_timestamp_create)")).show(10)

    def creation_histogram(self, arg):
        """Returns a histogram of days where each bin is the number of datasets created in that day
        and used during the time window specified with the --from and --to arguments"""
        return self.dataframe.select("dataset_id","dataset_timestamp_create")\
            .groupBy("dataset_id").agg({"dataset_timestamp_create":"min"})\
            .withColumn("creation_date", date_format(col("min(dataset_timestamp_create)"), "yyyy-MM-dd"))\
            .groupBy("creation_date").count().orderBy(col("creation_date"))

    def creation_histogram_hits(self, arg):
        """Like the previous method, create an histogram of days, but this time each bin is the count
        of total rows with datasets created in that day"""
        return self.dataframe.select("dataset_id","dataset_timestamp_create")\
            .groupBy("dataset_id").agg({"dataset_timestamp_create":"min", "*":"count"})\
            .withColumn("creation_date", date_format(col("min(dataset_timestamp_create)"),"yyyy-MM-dd"))\
            .withColumnRenamed("count(1)","hits")\
            .groupBy("creation_date").sum("hits").orderBy(col("creation_date"))
