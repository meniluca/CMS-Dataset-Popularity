"""
Collection of utility methods
"""
from re import match as regex_match
from datetime import datetime, timedelta

def get_days_list(from_date, to_date):
    """Returns a list of days included in the specified from-to period"""
    assert regex_match(r'^\d\d\d\d\-\d\d-\d\d$', from_date), "%s doesn't match format YYYY-MM-DD" % from_date
    assert regex_match(r'^\d\d\d\d\-\d\d-\d\d$', to_date), "%s doesn't match format YYYY-MM-DD" % to_date
    days = []
    from_date = datetime.strptime(from_date, "%Y-%m-%d").date()
    to_date = datetime.strptime(to_date, "%Y-%m-%d").date()
    oneday = timedelta(days=1)
    while from_date < to_date:
        from_date += oneday
        days.append(from_date)
    return days
