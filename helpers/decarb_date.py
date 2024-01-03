import calendar
from datetime import datetime as dt


def get_now():
    return dt.now()


class DecarbDate:
    @staticmethod
    def start_time() -> str:
        """
        當一月時，設定為去年十二月一號
        其餘減一個月的第一天
        """
        return dt(get_now().year-1, 12, 1).strftime("%Y-%m-%d") if get_now().month == 1 \
            else dt(get_now().year, get_now().month-1, 1).strftime("%Y-%m-%d")

    @staticmethod
    def end_time() -> str:
        """
        當一月時，設定為去年十二月三一號
        其餘減一個月的最後一天
        """
        return dt(get_now().year-1, 12, 31).strftime("%Y-%m-%d") if get_now().month == 1 \
            else dt(get_now().year, get_now().month-1,
                    calendar.mdays[get_now().month-1]).strftime("%Y-%m-%d")
