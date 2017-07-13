"""
CMS Data Popularity:
    given a list of dataset names or IDs, displays statistics using PheDEx catalog data.

Author: Luca Menichetti (luca.menichetti@cern.ch)
"""

import argparse
from pyspark.sql import SparkSession
from datapop.phedex import Catalog, Blocks
from spark import util

if __name__ == '__main__':

    parser = argparse.ArgumentParser(prog="CMS Data Popularity", \
        description="Utility to run PySpark jobs performing aggregations and \
            statistics using CMS popularity data sources.")
    parser.add_argument("--from", \
        required=True, \
        dest="from_date",\
        action="store", \
        help="Process data from this date (format YYYY-MM-DD)")
    parser.add_argument("--to", \
        required=True, \
        dest="to_date",\
        action="store", \
        help="Process data until this date (format YYYY-MM-DD)")
    parser.add_argument("--dataset-id", \
        required=False, \
        dest="dataset_id",\
        action="store", \
        help="The ID of the dataset (data source: Phedex)")
    parser.add_argument("--blocks-path", \
        required=True, \
        dest="blocks_path",\
        action="store", \
        help="Phedex, block replica snapshot HDFS path")
    parser.add_argument("--catalog-path", \
        required=True, \
        dest="catalog_path",\
        action="store", \
        help="Phedex, catalog HDFS path")


    args = parser.parse_args()

    spark = SparkSession\
        .builder\
        .appName("CMS-datapop")\
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    blocks = Blocks(prefix=args.blocks_path, days_list=util.get_days_list(args.from_date, args.to_date))

    blocks_dataframe = spark.read.format("csv") \
        .schema(blocks.get_schema()) \
        .load(blocks.folders_list)

    catalog = Catalog(prefix=args.catalog_path, days_list=util.get_days_list(args.from_date, args.to_date))

    catalog_dataframe = spark.read.format("csv") \
        .schema(catalog.get_schema()) \
        .load(catalog.folders_list)

    blocks.set_dataframe(blocks_dataframe)
    catalog.set_dataframe(catalog_dataframe)

    if not args.dataset_id:
        blocks.print_stats()
        catalog.print_stats()
    else:
        pass
