from pytimekr import pytimekr
import datetime


# arg is datetime.date(2021, 1, 1) or datetime.datetime.today()
def check_holiday(date):
    year = date.strftime('%Y')
    holidays = pytimekr.holidays(int(year))
    if date in holidays:
        return True

    day_of_week = date.weekday()
    if day_of_week == 5 or day_of_week == 6:
        return True
    else:
        return False


def market_off():
    now = datetime.datetime.now()
    if int(now.strftime('%H')) >= 17:
        return True
    else:
        return False


def get_today():
    return datetime.date.today()


def get_latest_market_day(target=datetime.date.today()):
    for i in range(1, 10):
        date = target - datetime.timedelta(days=i)
        if check_holiday(date) is False:
            return date


def get_date(year, month, day):
    return datetime.date(year, month, day)


def get_dot_date(date):
    return '{}.{}.{}'.format(date.strftime('%Y'), date.strftime('%m'), date.strftime('%d'))
