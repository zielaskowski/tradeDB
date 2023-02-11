import pandas as pd
from datetime import datetime as dt
from bs4 import BeautifulSoup as bs
import requests as rq
from workers.common import get_cookie

"""download data from www.stooq.com
called only when missing info in local db
"""


STOOQ_COOKIE = "./assets/header_stooq.json"
cookie = get_cookie(STOOQ_COOKIE)


def web_stooq(
    sector_id: int,
    sector_grp: str,
    symbol: str,
    from_date: str,
    end_date: str,
) -> pd.DataFrame:
    """
    Args:
        [sector_id]: group id for web API
        [sector_grp]: some tables are divided into groups
        symbol: symbol name
        from_date: start date for search, is ignored for sector search
        end_date: end date for search, is ignored for sector search
    """

    # we take current data for sector
    if symbol == "%":
        data = read_sector_url(sector_id)
        if sector_grp != "":
            data = split_groups(data, sector_grp)
        return data
    else:
        # or we search particular item
        url = "https://stooq.com/q/?s=opus.hu"
        return data


def read_sector_url(id: int) -> pd.DataFrame:
    data = pd.DataFrame()
    for i in range(1, 100):
        url = f"https://stooq.com/t/?i={id}&v=0&l={i}&f=0&n=1&u=1"
        # n: long/short names
        # f: show/hide favourite column
        # l: page number for very long tables (table has max 100 rows)
        # u: show/hide if change empty (not rated today)
        resp = rq.get(url, headers=cookie)

        if resp.status_code != 200:
            break

        page = bs(resp.content, "lxml")
        htmlTab = page.find(id="fth1")
        pdTab = pd.read_html(htmlTab.prettify())[0]

        if pdTab.empty:
            break

        # some basic formatting
        # remove 'Change' col, rename other columns (Last->val), str_to_lower
        # convert dates
        pdTab = pdTab.loc[:, ~pdTab.columns.str.startswith("Change")]
        pdTab.rename(columns={"Last": "val"}, inplace=True)
        pdTab.rename(str.lower, axis="columns", inplace=True)
        pdTab["date"] = convert_date(pdTab["date"])
        pdTab.dropna(subset=["date"], inplace=True)  # in case unknown format

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
    return d1.fillna(d2).dt.date


def split_groups(data: pd.DataFrame, grp: str) -> pd.DataFrame:
    # extract 'grp' rows from 'data' DataFrame
    grpNameRows = data.iloc[:, 1] == data.iloc[:, 2]
    grpName = data.loc[grpNameRows, "Name"].to_frame()

    start = list(map(lambda x: x + 1, grpName.index.to_list()))
    end = list(map(lambda x: x - 1, grpName.index.to_list()))  # shift by one
    end.append(len(data))

    grpName["Start"] = start
    grpName["End"] = end[1:]
    grpRow = grpName.loc[:, "Name"] == grp
    data = data.loc[
        grpName["Start"].values[grpRow][0]: grpName["End"].values[grpRow][0], :
    ]
