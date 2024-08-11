from datetime import datetime


def get_int_dt(dt):
    return dt.year * 10000 + dt.month * 100 + dt.day


def get_int_tm(dt):
    return dt.hour * 100 + dt.minute


def pct_chg(p1, p2):
    return round((p1/p2 - 1) * 100, 2)


def abs_pct_chg(p1, p2):
    if p1 > p2:
        return pct_chg(p1, p2)
    else:
        return pct_chg(p2, p1)


def get_dttime_form_intdt(dt, tm=None):
    year, md = divmod(dt, 10000)
    month, day = divmod(md, 100)
    if tm is None:
        return datetime(year, month, day)
        
    hour, minute = divmod(tm, 100)
    return datetime(year, month, day, hour, minute)

