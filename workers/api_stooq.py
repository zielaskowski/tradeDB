import re
from datetime import date
from datetime import datetime as dt

import numpy as np
import pandas as pd
import requests as rq
from bs4 import BeautifulSoup as bs

from workers.common import get_cookie

"""download data from www.stooq.com
called only when missing info in local db
"""


STOOQ_COOKIE = "./assets/header_stooq.json"
cookie = get_cookie(STOOQ_COOKIE)


def web_stooq(
    from_date: date,
    end_date: date,
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
        data = read_sector_url(url)
        if sector_grp != "":
            data = split_groups(data, sector_grp)
        return data
    elif symbol:  # or we search particular item
        url = f"https://stooq.com/q/?s={symbol}"
        data = read_sector_url(url)

    elif components:  # if components included, download components
        url = f"https://stooq.com/q/i/?s={components}&l=%page%&i"
        data = read_sector_url(url)

    return data


def read_sector_url(url: str) -> pd.DataFrame:
    data = pd.DataFrame()
    for i in range(1, 100):
        url = re.sub("%page%", str(i), url)
        resp = rq.get(url, headers=cookie)

        if resp.status_code != 200:
            break

        page = bs(resp.content, "lxml")
        htmlTab = page.find(id="fth1")
        pdTab = pd.read_html(htmlTab.prettify())[0]  # type: ignore

        if pdTab.empty:
            break

        if pdTab.columns.nlevels > 1:
            pdTab = pdTab.droplevel(0, axis="columns")

        # some basic formatting: str_to_lower
        pdTab.rename(str.lower, axis="columns", inplace=True)
        # rename polish to english
        pdTab.rename(
            columns={
                "nazwa": "name",
                "kurs": "val",
                "data": "date",
                "zmiana": "change",
            },
            inplace=True,
        )
        # remove 'change' col, rename other columns (Last->val),
        pdTab = pdTab.loc[
            :,
            [
                re.search("(change|kapitalizacja|warto|zysk|ttm|c/wk|stopa)", c) is None
                for c in pdTab.columns
            ],
        ]
        pdTab.rename(columns={"last": "val"}, inplace=True)
        # convert dates
        pdTab["date"] = convert_date(pdTab["date"])

        data = pd.concat([data, pdTab], ignore_index=True)
    return data


def convert_date(dates: pd.Series) -> pd.Series:
    # set date: it's in 'mmm d' or 'hh:ss'
    # return NaN if format not known
    # this will handle hh:ss, setting date to today
    d1 = pd.to_datetime(dates, errors="coerce")
    year = dt.today().strftime("%Y")
    # to handle 'mmm d' we need add current year
    d2 = pd.to_datetime(year + " " + dates, format="%Y %b %d", errors="coerce")
    d3 = pd.to_datetime(
        year + " " + dates, format="%Y %d %b", errors="coerce"
    )  # 2022 22 Jan
    d1 = d1.fillna(d2)
    d1 = d1.fillna(d3)
    return d1.dt.date


def split_groups(data: pd.DataFrame, grp: str) -> pd.DataFrame:
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
