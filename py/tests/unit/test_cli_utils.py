import pytest

from datetime import datetime
from dateutil.relativedelta import relativedelta
from gaarf.cli.utils import convert_date

def test_convert_date():
    current_date = datetime.today()
    current_year = datetime(current_date.year, 1, 1)
    current_month = datetime(current_date.year, current_date.month, 1)
    last_year = current_year - relativedelta(years=1)
    last_month = current_month - relativedelta(months=1)
    yesterday = current_date - relativedelta(days=1)

    date_year = ":YYYY"
    date_month = ":YYYYMM"
    date_day = ":YYYYMMDD"
    date_year_minus_one = ":YYYY-1"
    date_month_minus_one = ":YYYYMM-1"
    date_day_minus_one = ":YYYYMMDD-1"

    new_date_year = convert_date(date_year)
    new_date_month = convert_date(date_month)
    new_date_day = convert_date(date_day)
    new_date_year_minus_one = convert_date(date_year_minus_one)
    new_date_month_minus_one = convert_date(date_month_minus_one)
    new_date_day_minus_one = convert_date(date_day_minus_one)

    assert new_date_year == current_year.strftime("%Y-%m-%d")
    assert new_date_month == current_month.strftime("%Y-%m-%d")
    assert new_date_day == current_date.strftime("%Y-%m-%d")
    assert new_date_year_minus_one == last_year.strftime("%Y-%m-%d")
    assert new_date_month_minus_one == last_month.strftime("%Y-%m-%d")
    assert new_date_day_minus_one == yesterday.strftime("%Y-%m-%d")


def test_wrong_convert_date():
    date_day = ":YYYYMMDD-N"
    with pytest.raises(ValueError):
        convert_date(date_day)
