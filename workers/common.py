import locale
import json
import sys
import hashlib
from typing import Dict, Union, Tuple
import re
import pandas as pd

from contextlib import contextmanager

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
    date_chk: Union[dt, date, str], format="%d-%m-%Y"
) -> date:
    # first, make sure dates are datetime
    if isinstance(date_chk, date):
        date_chk = dt(date_chk.year, date_chk.month, date_chk.day)
    if isinstance(date_chk, str):
        from_datePD = pd.to_datetime(date_chk, format=format)
        date_chk = dt(from_datePD.year, from_datePD.month, from_datePD.day)
    
    # convert date to MST and substract one day
    # this way we can be sure all stocks are already closed
    # end we got day closed values from web
    date_chk = date_chk.astimezone(pytz.timezone("Canada/Mountain"))
    # trick to move to previous bizday
    # trnsform such week is from Tu=0 to Mo=6
    date_chk -= timedelta(max(1, (date_chk.weekday() + 6) % 7 - 3))

    return date_chk.date()


def convert_date(dates: pd.Series) -> pd.Series:
    # set date: it's in 'mmm d'(ENG) or 'd mmm'(PL) or 'hh:ss' for today and some more
    # return '' if format not known
    @contextmanager
    def setlocale(*args, **kwargs):
        # temporary change locale
        saved = locale.setlocale(locale.LC_ALL)
        yield locale.setlocale(*args, **kwargs)
        locale.setlocale(locale.LC_ALL, saved)

    def date_locale(date: pd.Series, local: str, format: str) -> pd.Series:
        with setlocale(locale.LC_ALL, local):  # type: ignore
            return pd.to_datetime(date, errors="coerce", format=format)

    year = dt.today().strftime("%Y")
    today = dt.today().strftime("%d %b %Y ")
    # ignore if no digits in date, probably group name
    # possibly also nan
    dates = dates.apply(lambda x: x if not pd.isna(x) else "1 Sty 1900")
    dates = dates.apply(lambda x: x if re.search(r"[0-9]", x) else "1 Sty 1900")

    d1 = date_locale(today + " " + dates, "en_GB.utf8", r"%d %b %Y %H:%M")  # hh:ss
    d2 = date_locale(year + " " + dates, "en_GB.utf8", r"%Y %d %b")  # 24 Feb
    d3 = date_locale(year + " " + dates, "en_GB.utf8", r"%Y %b %d")  # Jan 22
    d4 = date_locale(year + " " + dates, "en_GB.utf8", r"%Y %d %b")  # 25 Jan
    d5 = date_locale(dates, "en_GB.utf8", r"%d %b %Y")  # 25 Jan 2023
    d6 = date_locale(year + " " + dates, "pl_PL.utf8", r"%Y %d %b")  # 22 Lut
    d7 = date_locale(dates, "pl_PL.utf8", r"%d %b %Y")  # 22 Lut 2023
    d8 = date_locale(dates, "pl_PL.utf8", r"%Y-%m-%d")  # 2023-10-20

    d1 = d1.fillna(d2)
    d1 = d1.fillna(d3)
    d1 = d1.fillna(d4)
    d1 = d1.fillna(d5)
    d1 = d1.fillna(d6)
    d1 = d1.fillna(d7)
    d1 = d1.fillna(d8)
    d1 = d1.dt.date
    d1 = d1.fillna(" ")
    # if date not recognized
    if not d1.loc[d1 == " "].empty:
        sys.exit(f'FATAL: date format not recognized:\n{dates.loc[d1==" "]}')
    return d1


def hash_table(dat: pd.DataFrame, tab: str) -> Union[pd.Series, None]:
    if not all(el in dat.columns for el in ["symbol", "name"]):
        print("no columns to hash")
        return None
    d = dat.copy(deep=True)
    d["tab"] = tab
    d["hash"] = [
        hashlib.md5("".join(r).encode("utf-8")).hexdigest()
        for r in d.loc[:, ["symbol", "name", "tab"]].to_records(index=False)
    ]
    return d["hash"]


def read_currency(file: str) -> pd.DataFrame:
    cur = pd.read_csv(file)
    cur = cur.loc[cur["withdrawal_date"].isna(), :]
    # some cleaning
    # replace '(abc)' with ', abc'
    cur["Entity"] = cur["Entity"].apply(lambda x: re.sub(r"\s*\(", ", ", x))
    cur["Entity"] = cur["Entity"].apply(lambda x: re.sub(r"\)$", "", x))
    cur["Entity"] = cur["Entity"].apply(
        lambda x: re.sub("CAYMAN ISLANDS, THE", "CAYMAN ISLANDS", x)
    )
    cur["Entity"] = cur["Entity"].apply(
        lambda x: re.sub("ST.MARTIN, FRENCH PART", "ST.MARTIN (FRENCH PART)", x)
    )
    cur["Entity"] = cur["Entity"].apply(
        lambda x: re.sub("ST.MARTIN, FRENCH PART", "ST.MARTIN (FRENCH PART)", x)
    )

    cur.rename(
        columns={
            "symbol": "icon",
            "Entity": "country",
            "currency": "name",
            "code": "symbol",
            "numeric_code": "currency_code",
        },
        inplace=True,
    )
    cur.dropna(subset=['country','symbol','name'], inplace=True)

    return cur
