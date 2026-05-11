from datetime import date
from dateutil.relativedelta import relativedelta

def get_target_yyyymm(months_ago=2):
    """
    Returns the year-month string (yyyy-MM) for a given number of months ago from the current date.
    """
    target_date = date.today() - relativedelta(months=months_ago)
    return target_date.strftime("%Y-%m")


def get_month_start_n_months_ago(months_ago=2):
    """
    Returns the first day of the month for a given number of months ago from the current date.
    
    parameters:
    months_ago - the number of months to go back from the current date (default is 2)
    
    returns: a date object representing the first day of the month for the target month
    """
    return date.today().replace(day=1) - relativedelta(months=months_ago)