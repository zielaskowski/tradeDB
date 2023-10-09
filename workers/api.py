import re
import sys
import os
import asyncio


from datetime import datetime as dt
from datetime import date
import time

from PIL import Image
import io

import numpy as np
import pandas as pd
import pandasdmx as sdmx
import requests as rq
from urllib.parse import urlparse
from bs4 import BeautifulSoup as bs
from playwright.async_api import async_playwright

from workers.common import set_header, convert_date

"""function to manage apis:
    - stooq: not really an API, but web scrapping
    - ECB, with usage of pandasSDMX library
"""


STOOQ_HEADER = "./assets/header_stooq.jsonc"
header = set_header(STOOQ_HEADER)


def stooq(
    from_date: date, to_date: date, sector_id=0, sector_grp="", symbol="", component=""
) -> pd.DataFrame:
    """
    Get data from stooq web page
    Args:
        [sector_id]: group id for web API
        [sector_grp]: some tables are divided into groups
        (when sector given, ignore dates, used only to initiate data)
        symbol: symbol name
        from_date: start date for search, is ignored for sector search
        end_date: end date for search, is ignored for sector search
    """
    data = pd.DataFrame([""])
    # convert dates
    to_dateS = dt.strftime(to_date, "%Y%m%d")  # type: ignore
    from_dateS = dt.strftime(from_date, "%Y%m%d")  # type: ignore

    if sector_id:  # indexes
        url = (
            f"https://stooq.pl/t/?i={sector_id}&v=0&l=%page%&f=0&n=1&u=1&d={from_dateS}"
        )
        # n: long/short names
        # f: show/hide favourite column
        # l: page number for very long tables (table has max 100 rows)
        # u: show/hide if change empty (not rated today)
        data = __scrap_stooq__(url)
        if sector_grp != "":
            data = __split_groups__(data, sector_grp)

    elif symbol:  # or we search particular item
        url = f"https://stooq.pl/q/d/?s={symbol}&d1={from_dateS}&d2={to_dateS}&l=%page%"
        data = __scrap_stooq__(url)
    elif component:
        url = f"https://stooq.pl/q/i/?s={component}&l=%page%"
        data = __scrap_stooq__(url)

    return data


def ecb(
    from_date: dt,
    end_date: dt,
    symbol="",
) -> pd.DataFrame:
    """takes data from European Central Bank.
    use pandas SDMX module
    currency denominator is EUR
    (there is no all pairs available, so use EUR as base for other conversions)
    Available dimensions in DB
    <Dimension FREQ>,
    <Dimension CURRENCY>,
    <Dimension CURRENCY_DENOM>,
    <Dimension EXR_TYPE>,
    <Dimension EXR_SUFFIX>,
    <TimeDimension TIME_PERIOD>
    """
    ecb = sdmx.Request("ECB")

    # available symbols
    exrDSD = ecb.dataflow("EXR").dataflow.EXR.structure # type: ignore
    exrCMP = exrDSD.dimensions.components
    all_symbols = sdmx.to_pandas(
        exrCMP[1].local_repesentation.enumerated
    ).index.to_list()
    if symbol not in all_symbols:
        print(f"Unknonw symbol: '{symbol}'")
        return pd.DataFrame([""])

    key = ".".join(["D", symbol, "", "", ""])
    params = {
        "startPeriod": dt.strftime(from_date, "%Y-%m-%d"),
        "endPeriod": dt.strftime(end_date, "%Y-%m-%d"),
    }
    datEXR = ecb.data("EXR", key=key, params=params)
    dat = sdmx.to_pandas(datEXR).reset_index()

    # df cleaning
    dat["TIME_PERIOD"] = pd.to_datetime(dat["TIME_PERIOD"])
    dat.rename(column={"TIME_PERIOD": "date", "value": "val"}, inplace=True)
    return dat.loc[:, ["date", "val"]]


def __captcha__(page: bs) -> bool:
    # check if we have captcha
    # captcha is trigered with bandwith limit or hit limit
    if all(
        [
            page.find(string=txt) is None
            for txt in ["The data has been hidden", "Dane zostały ukryte"]
        ]
    ):
        return False

    while True:
        # display captcha
        url = f"https://stooq.pl/q/l/s/i/?{int(time.time()*1000)}"
        resp = rq.get(url=url, headers=header)
        with Image.open(io.BytesIO(resp.content)) as img:
            img.save("./dev/captcha.png")  # DEBUG
            print(
                "Rewrite the above code\n(contains only uppercase letters and numbers)"
            )
            print("use ctr C to break")
            try:
                captcha_txt = input("?>")
            except KeyboardInterrupt:
                sys.exit(f"\nFATAL: user interuption")
        url = f"https://stooq.pl/q/l/s/?t={captcha_txt}"
        resp = rq.get(url=url, headers=header)

        if resp.content:
            return True
        print("captcha not accepted, try again")


async def __GDPR__(url: str):
    global header
    # GDPR stands for: GeneralDataProtectionRegulation
    # simulate browser behaviour: clisk consent button
    # to collect all headers, but more important cookies
    print("Setting https connection...\n")

    # we need to install playwright for the first time
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            await context.new_page()
    except:
        print("Installing Playwright....")
        os.system("playwright install")

    async def request(req):
        # we take whole header for request
        # so we can use it later to fake browser
        global header
        ah = await req.all_headers()
        header = set_header(file=STOOQ_HEADER, upd_header=ah)

    async def response(resp):
        # from response we take set-cookie only
        # and update headers
        global header
        ah = await resp.all_headers()
        set_cookie = ah.get("set-cookie", "")
        # multiple set-cookie are splited with new line
        # cookie attributes are split by ';'
        # first attribute is name=value
        set_cookie = ";".join([c.split(";")[0] for c in set_cookie.split("/n")])
        header = set_header(file=STOOQ_HEADER, upd_header={"cookie": set_cookie})

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        page.on(
            "request",
            lambda req: request(req)
            if urlparse(req.url).netloc == urlparse(url).netloc
            else None,
        )
        page.on(
            "response",
            lambda resp: response(resp)
            if urlparse(resp.url).netloc == urlparse(url).netloc
            else None,
        )
        await page.goto(url=url)
        await page.locator("button.fc-cta-consent").click()
        await page.wait_for_event("load")
        await page.reload()


def __scrap_stooq__(url: str) -> pd.DataFrame:
    global header
    data = pd.DataFrame()
    for i in range(1, 100):
        urli = re.sub("%page%", str(i), url).lower()
        while True:
            resp = rq.get(url=urli, headers=header, allow_redirects=False)

            if resp.status_code != 200:
                if resp.status_code == 302:  # redirection
                    # means the asset no longer available
                    # remove from DB as not usefulle to predict future anymore
                    data = pd.DataFrame(["asset removed"])
                return data

            page = bs(resp.content, "lxml")

            if page.body is None:
                # GDPR dialog
                # when no cookies present, first time use
                # will set proper headers and cookies
                asyncio.run(__GDPR__(url=urli))
                continue

            # do we have captcha?
            if __captcha__(page):
                continue
            break

        htmlTab = page.find(id="fth1")
        if htmlTab is None:
            break

        pdTab = pd.read_html(io.StringIO(bs.prettify(htmlTab)))[0]  # type: ignore

        if pdTab.empty:
            break

        if pdTab.columns.nlevels > 1:
            pdTab = pdTab.droplevel(0, axis="columns")

        # some basic formatting: str_to_lower
        pdTab.rename(str.lower, axis="columns", inplace=True)
        # rename columns
        pdTab.rename(
            columns={
                "nazwa": "name",
                "kurs": "val",
                "data": "date",
                "wolumen": "vol",
                "kapitalizacja (mln)": "vol",
                "zamknięcie": "val",
                "last": "val",
                "close": "val",
            },
            inplace=True,
        )
        # drop duplicated columns
        # i.e. change is in percent and in absolute value
        pdTab = pdTab.loc[:, list(~pdTab.columns.duplicated())]
        # convert dates
        pdTab["date"] = convert_date(pdTab["date"])

        data = pd.concat([data, pdTab], ignore_index=True)
    return data


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
                # i.e. '^(?!.*CANADA$).*$'
                grps[i] = "^(?!.*" + g[1:] + "$).*$"
            else:
                grps[i] = ".*" + g + "$"  # i.e. '.*CANADA$'
        grp_rows = [
            data["name"].apply(lambda x: re.search(g, x) is not None) for g in grps
        ]
        grp_rows = np.logical_and.reduce(grp_rows)
        data = data.loc[grp_rows, :]
    else:
        start = list(map(lambda x: x + 1, grpName.index.to_list()))
        # shift by one
        end = list(map(lambda x: x - 1, grpName.index.to_list()))
        end.append(len(data))

        grpName["Start"] = start
        grpName["End"] = end[1:]
        grpRow = grpName.loc[:, "name"] == grp
        data = data.loc[
            grpName["Start"].values[grpRow][0] : grpName["End"].values[grpRow][0], :
        ]
    return data
