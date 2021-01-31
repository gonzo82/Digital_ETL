from datetime import datetime
from datetime import timedelta


def return_miliseconds(day_dif):
    date_time = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    time_deta = timedelta(days=day_dif)
    inicio_date = datetime(1970, 1, 1)
    a_timedelta = date_time - inicio_date - time_deta
    seconds = int(a_timedelta.total_seconds())
    miliseconds = seconds * 1000
    return miliseconds


def return_seconds(day_dif):
    date_time = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    time_deta = timedelta(days=day_dif)
    inicio_date = datetime(1970, 1, 1)
    a_timedelta = date_time - inicio_date - time_deta
    seconds = int(a_timedelta.total_seconds())
    return seconds


def return_seconds_from_date(date):
    date_time = datetime.strptime(date, '%Y-%m-%d')
    inicio_date = datetime(1970, 1, 1)
    a_timedelta = date_time - inicio_date
    seconds = int(a_timedelta.total_seconds())
    return seconds