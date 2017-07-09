"""

"""

from pyspark.sql.types import StructType

class Blocks(object):
    """docstring for Blocks."""
    def __init__(self, arg):
        self.arg = arg

    def get_schema(self):
        """Returns Spark DataFrame schema for block-replicas-snapshots."""
        return StructType()\
            .add("transfer_ts","double")\
        	.add("dataset_name","string")\
        	.add("dataset_id","integer")\
        	.add("dataset_is_open","string")\
        	.add("dataset_time_create","double")\
        	.add("dataset_time_update","string")\
        	.add("block_name","string")\
        	.add("block_id","integer")\
        	.add("block_files","integer")\
        	.add("block_bytes","long")\
        	.add("block_is_open","string")\
        	.add("block_time_create","double")\
        	.add("block_time_update","double")\
        	.add("node_name","string")\
        	.add("node_id","integer")\
        	.add("is_active","string")\
        	.add("src_files","integer")\
        	.add("src_bytes","long")\
        	.add("dest_files","integer")\
        	.add("dest_bytes","long")\
        	.add("node_files","integer")\
        	.add("node_bytes","long")\
        	.add("xfer_files","integer")\
        	.add("xfer_bytes","long")\
        	.add("is_custodial","string")\
        	.add("user_group","string")\
        	.add("replica_time_create","double")\
        	.add("replica_time_update","double")

class Catalog(object):
    """docstring for Catalog."""
    def __init__(self, arg):
        self.arg = arg

    def get_schema(self):
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
