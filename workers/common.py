import json
from typing import Dict, Union, Tuple
import re
import pandas as pd

from datetime import date, timedelta
from datetime import datetime as dt
import pytz
import json
from json import JSONDecodeError


def read_json(file: str) -> Dict:
    """read json file
    ignores comments: everything from '//**' to eol"""
    try:
        with open(file, "r") as f:
            json_f = re.sub(
                "//\\*\\*.*$", "", "".join(f.readlines()), flags=re.MULTILINE
            )
    except IOError:
        raise Exception(f"FATAL: '{file}' is missing")

    try:
        return json.loads(json_f)
    except JSONDecodeError:
        return {}


def set_header(file: str, upd_header={}) -> dict:
    # read / write headers to file
    # including cookies
    header = read_json(file)
    cookies = header.get("cookie", "")

    def cookie2str(cookie: dict) -> str:
        return "; ".join([f"{k}={v}" for k, v in cookie.items()])

    def cookie2dict(cookie: str) -> dict:
        if cookie == "":
            return {}
        return {
            k: v for k, v in [cookie.split("=", 1) for cookie in cookie.split("; ")]
        }

    if upd_header != {}:
        upd_cookies = upd_header.get("cookie", "")
        header.update(upd_header)
        # cookies we add, not override
        c2d = cookie2dict(cookies)
        c2d.update(cookie2dict(upd_cookies))
        cookies = cookie2str(c2d)
        header["cookie"] = cookies

        with open(file, "w") as f:
            json.dump({**header, **{"cookie": cookies}}, f)

    return header


def biz_date(
    from_date: Union[dt, date, str], to_date: Union[dt, date, str]
) -> Tuple[date, date]:
    # first, make sure dates are datetime
    if isinstance(from_date, date):
        from_date = dt(from_date.year, from_date.month, from_date.day)
    if isinstance(to_date, date):
        to_date = dt(to_date.year, to_date.month, to_date.day) + timedelta(days=1)
    if isinstance(from_date, str):
        from_datePD = pd.to_datetime(from_date)
        from_date = dt(from_datePD.year, from_datePD.month, from_datePD.day)
    if isinstance(to_date, str):
        to_datePD = pd.to_datetime(to_date)
        to_date = dt(to_datePD.year, to_datePD.month, to_datePD.day) + timedelta(days=1)
    
    # convert date to MST and substract one day
    # this way we can be sure all stocks are already closed
    # end we got day closed values from web
    from_date = from_date.astimezone(pytz.timezone("Canada/Mountain"))

    # trick to move to previous bizday
    # trnsform such week is from Tu=0 to Mo=6
    from_date -= timedelta(max(1, (from_date.weekday() + 6) % 7 - 3))
    to_date = to_date.astimezone(pytz.timezone("Canada/Mountain"))
    if to_date > from_date:
        to_date = from_date

    return (from_date.date(), to_date.date())
