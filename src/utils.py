from datetime import datetime

from dateutil.relativedelta import MO, SU, relativedelta


def get_start_end_dates(year, week_number):
    week_number = int(week_number)

    first_day = datetime(year, 1, 1).date()

    if first_day.weekday() > 0:
        first_day = first_day + relativedelta(weekday=MO(1))

    start_date = first_day + relativedelta(weeks=+week_number - 1)
    end_date = start_date + relativedelta(weekday=SU(1))
    if start_date.year != first_day.year:
        raise ValueError("Week number does not exist in this year")

    return start_date, end_date
