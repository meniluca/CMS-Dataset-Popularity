"""
CMS Data Popularity:
    given a list of dataset names or IDs, displays statistics using PheDEx catalog data.

Author: Luca Menichetti (luca.menichetti@cern.ch)
"""

import argparse

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
        dest="dataset_ID",\
        action="store", \
        help="The ID of the dataset (data source: Phedex)")

    args = parser.parse_args()
