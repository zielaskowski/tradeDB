import locale
import re
from contextlib import contextmanager
from datetime import datetime as dt

import numpy as np
import pandas as pd
import requests as rq
from bs4 import BeautifulSoup as bs

from workers.common import get_cookie

"""download data from www.stooq.com
called only when missing info in local db
"""


STOOQ_COOKIE = "./assets/header_stooq.jsonc"
cookie = get_cookie(STOOQ_COOKIE)


def get(
    from_date: dt,
    end_date: dt,
    sector_id=0,
    sector_grp="",
    symbol="",
    components="",
) -> pd.DataFrame:
    """
    Args:
        [sector_id]: group id for web API
        [sector_grp]: some tables are divided into groups
        symbol: symbol name
        from_date: start date for search, is ignored for sector search
        end_date: end date for search, is ignored for sector search
    """

    if sector_id and not symbol:  # indexes
        url = f"https://stooq.com/t/?i={sector_id}&v=0&l=%page%&f=0&n=1&u=1"
        # n: long/short names
        # f: show/hide favourite column
        # l: page number for very long tables (table has max 100 rows)
        # u: show/hide if change empty (not rated today)
        data = __take_page__(url)
        if sector_grp != "":
            data = __split_groups__(data, sector_grp)

    elif symbol:  # or we search particular item
        url = f"https://stooq.com/q/?s={symbol}"
        data = __take_page__(url)

    elif components:  # if components included, download components
        url = f"https://stooq.com/q/i/?s={components}&l=%page%&i"
        data = __take_page__(url)

    return data


def __take_page__(url: str) -> pd.DataFrame:
    data = pd.DataFrame()
    for i in range(1, 100):
        resp = rq.get(url=re.sub("%page%", str(i), url).lower(), headers=cookie)

        if resp.status_code != 200:
            break

        page = bs(resp.content, "lxml")
        htmlTab = page.find(id="fth1")
        if htmlTab is None:
            break
        pdTab = pd.read_html(htmlTab.prettify())[0]  # type: ignore

        if pdTab.empty:
            break

        if pdTab.columns.nlevels > 1:
            pdTab = pdTab.droplevel(0, axis="columns")

        # some basic formatting: str_to_lower
        pdTab.rename(str.lower, axis="columns", inplace=True)
        # rename polish to english
        pdTab.rename(
            columns={"nazwa": "name", "kurs": "val", "data": "date", "wolumen": "vol"},
            inplace=True,
        )
        # rename columns (Last->val),
        pdTab.rename(columns={"last": "val"}, inplace=True)
        # convert dates
        pdTab["date"] = __convert_date__(pdTab["date"])

        data = pd.concat([data, pdTab], ignore_index=True)
    return data


def __convert_date__(dates: pd.Series) -> pd.Series:
    # set date: it's in 'mmm d'(ENG) or 'd mmm'(PL) or 'hh:ss' for today
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
    d1 = pd.to_datetime(dates, errors="coerce")  # hh:ss
    d2 = date_locale(dates + ' ' + year, "en_GB.utf8", "%d %b %Y")  # 24 Feb 2023
    d3 = date_locale(year + ' ' + dates, "en_GB.utf8", "%Y %b %d")  # Jan 22
    d4 = date_locale(year + ' ' + dates, "pl_PL.utf8", "%Y %d %b")  # 22 Lut

    d1 = d1.fillna(d2)
    d1 = d1.fillna(d3)
    d1 = d1.fillna(d4)
    d1 = d1.fillna(" ")
    return d1


def __split_groups__(data: pd.DataFrame, grp: str) -> pd.DataFrame:
    # extract 'grp' rows from 'data' DataFrame
    # if no groups, search for apendix
    grpNameRows = data.iloc[:, 1] == data.iloc[:, 2]
    grpName = data.loc[grpNameRows, "name"].to_frame()
    if grpName.empty:
        # we dont have groups so we have suffixes
        grps = grp.split(";")
        for i, g in enumerate(grps):
            if g[0] == "-":
                grps[i] = "^(?!.*" + g[1:] + "$).*$"  # i.e. '^(?!.*CANADA$).*$'
            else:
                grps[i] = ".*" + g + "$"  # i.e. '.*CANADA$'
        grp_rows = [
            data["name"].apply(lambda x: re.search(g, x) is not None) for g in grps
        ]
        grp_rows = np.logical_and.reduce(grp_rows)
        data = data.loc[grp_rows, :]
    else:
        start = list(map(lambda x: x + 1, grpName.index.to_list()))
        end = list(map(lambda x: x - 1, grpName.index.to_list()))  # shift by one
        end.append(len(data))

        grpName["Start"] = start
        grpName["End"] = end[1:]
        grpRow = grpName.loc[:, "name"] == grp
        data = data.loc[
            grpName["Start"].values[grpRow][0] : grpName["End"].values[grpRow][0], :
        ]
    return data
