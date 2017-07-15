"""
Module for CMS job monitoring logs
"""
from re import match as regex_match
from pyspark import StorageLevel

def get_day_folder_path(prefix, year, month, day):
    """Constructs and returns the day path string"""
    assert regex_match(r'^(\d\d)?\d\d$', year), "%s is not a valid year" % year
    assert regex_match(r'^(\d)?\d$', month), "%s is not a valid month" % month
    assert regex_match(r'^(\d)?\d$', day), "%s is not a valid day" % day
    return "%s/year=%d/month=%d/day=%d/" % (prefix, int(year), int(month), int(day))

# class JobsMonitoring(object):
#     """docstring for JobsMonitoring."""
#     def __init__(self, prefix, days_list):
#         self.prefix = prefix
#         self.folders_list = [get_day_folder_path(prefix=self.prefix, \
#             year=day.strftime("%Y"), month=day.strftime("%m"), day=day.strftime("%d")) for day in days_list]
#         self.dataframe = None
#         self.total_rows = None

class JobsPopularity(object):
    """docstring for JobsPopularity."""
    def __init__(self, prefix, days_list):
        self.prefix = prefix
        self.folders_list = [get_day_folder_path(prefix=self.prefix, \
            year=day.strftime("%Y"), month=day.strftime("%m"), day=day.strftime("%d")) for day in days_list]
        self.dataframe = None
        self.total_rows = None
        self.num_days = len(days_list)

    def set_dataframe(self, dataframe):
        """Link a Spark DataFrame to this class instance"""
        assert isinstance(dataframe, DataFrame), "The method requires a DataFrame"
        if self.dataframe:
            self.dataframe.unpersist()
            print "Replacing existing dataframe"
        self.dataframe = dataframe
        self.total_rows = dataframe.count()
        print "Total JobsPopularity rows: %d" % self.total_rows
