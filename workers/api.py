import re
import sys
import os
import asyncio
from typing import Union

from datetime import datetime as dt
from datetime import date
import time
import tkinter as tk
from PIL import Image, ImageTk
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
    from_date: Union[date, None],
    to_date: Union[date, None],
    sector_id=0,
    sector_grp="",
    symbol="",
    component="",
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
        url = f"https://stooq.com/t/?i={sector_id}&v=0&l=%page%&f=0&n=1&u=1&d={from_dateS}"
        # n: long/short names
        # f: show/hide favourite column
        # l: page number for very long tables (table has max 100 rows)
        # u: show/hide if change empty (not rated today)
        data = __scrap_stooq__(url)
        if sector_grp != "":
            data = __split_groups__(data, sector_grp)

    elif symbol:  # or we search particular item
        url = f"https://stooq.com/q/d/l/?s={symbol}&d1={from_dateS}&d2={to_dateS}&l=%page%&i=d"
        # i: download data as csv
        data = __scrap_stooq__(url, n=2)
    elif component:
        url = f"https://stooq.com/q/i/?s={component}&i=0&l=%page%"
        # i: show indicators
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
    if symbol == "EUR":
        date_range = pd.date_range(from_date, end_date)
        dat = pd.DataFrame({"date": date_range, "val": 1})
        dat.date = dat.date.dt.date
        return dat
    ecb = sdmx.Request("ECB")

    # available symbols
    exrDSD = ecb.dataflow("EXR").dataflow.EXR.structure  # type: ignore
    exrCMP = exrDSD.dimensions.components
    all_symbols = sdmx.to_pandas(
        exrCMP[1].local_representation.enumerated
    ).index.to_list()
    if symbol not in all_symbols:
        print(f"Unknonw symbol: '{symbol}'")
        return pd.DataFrame([""])

    key = ".".join(["D", symbol, "", "", ""])  # D stands for daily
    params = {
        "startPeriod": dt.strftime(from_date, "%Y-%m-%d"),
        "endPeriod": dt.strftime(end_date, "%Y-%m-%d"),
    }
    datEXR = ecb.data("EXR", key=key, params=params)
    dat = sdmx.to_pandas(datEXR).reset_index()

    # df cleaning
    dat["TIME_PERIOD"] = pd.to_datetime(dat["TIME_PERIOD"]).dt.date
    dat.rename(columns={"TIME_PERIOD": "date", "value": "val"}, inplace=True)
    return dat[["date", "val"]]


def __captcha__(page: bs) -> bool:
    # check if we have captcha
    # captcha is trigered with bandwith limit or hit limit
    global header
    if all(
        page.find(string=txt) is None
        for txt in ["The data has been hidden", "Dane zostały ukryte"]
    ):
        return False

    while True:
        # display captcha
        url = f"https://stooq.com/q/l/s/i/?{int(time.time()*1000)}"
        resp = rq.get(url=url, headers=header)
        header = set_header(
            file=STOOQ_HEADER,
            upd_header={"cookie": resp.headers.get("set-cookie", "")},
        )

        with Image.open(io.BytesIO(resp.content)) as img:
            img.save("./dev/captcha.png")  # DEBUG

        captcha_txt = captcha_gui()
        if not captcha_txt:
            sys.exit(f"\nFATAL: user interuption")

        url = f"https://stooq.com/q/l/s/?t={captcha_txt}"
        resp = rq.get(url=url, headers=header)
        header = set_header(
            file=STOOQ_HEADER,
            upd_header={"cookie": resp.headers.get("set-cookie", "")},
        )

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


def __scrap_stooq__(url: str, n=100) -> pd.DataFrame:
    global header
    data = pd.DataFrame()
    for i in range(1, n):
        urli = re.sub("%page%", str(i), url).lower()
        while True:
            resp = rq.get(url=urli, headers=header, allow_redirects=False)

            if resp.status_code != 200:
                if resp.status_code == 302:  # redirection
                    # means the asset no longer available
                    # remove from DB as not usefulle to predict future anymore
                    data = pd.DataFrame(["asset removed"])
                return data
            # set cooki
            header = set_header(
                file=STOOQ_HEADER,
                upd_header={"cookie": resp.headers.get("set-cookie", "")},
            )

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

        if "/q/d/l/" in urli:
            pdTab = pd.read_csv(io.StringIO(page.body.p.text))  # type: ignore
        else:
            htmlTab = page.find(id="fth1")
            if htmlTab is None:
                break
            pdTab = pd.read_html(io.StringIO(bs.prettify(htmlTab)))[0]  # type: ignore

        if pdTab.empty:
            break

        data = pd.concat([data, pdTab], ignore_index=True)
    return __clean_tab__(data)


def __clean_tab__(pdTab: pd.DataFrame) -> pd.DataFrame:
    # some basic formatting:
    if pdTab.empty:
        return pdTab
    if pdTab.columns.nlevels > 1:
        pdTab = pdTab.droplevel(0, axis="columns")
    # str_to_lower
    pdTab.rename(str.lower, axis="columns", inplace=True)
    # info about dyvident (also others?) breaks table
    if "no." in pdTab.columns:
        pdTab = pdTab.dropna(subset="no.")
    # rename columns
    pdTab.rename(
        columns={
            "nazwa": "name",
            "kurs": "val",
            "data": "date",
            "wolumen": "vol",
            "zamknięcie": "val",
            "last": "val",
            "close": "val",
            "volume": "vol",
        },
        inplace=True,
    )
    # drop duplicated columns
    # i.e. change is in percent and in absolute value
    pdTab = pdTab.loc[:, list(~pdTab.columns.duplicated())]
    # drop rows without value
    pdTab = pdTab.dropna(subset=["val"])
    # convert dates
    pdTab["date"] = convert_date(pdTab["date"])
    return pdTab


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


def captcha_gui() -> str:
    """
    Displays a GUI with a captcha pic
    and a text box to enter the answer
    """
    ans = ""

    def submit(txt):
        nonlocal ans
        ans = txt
        root.destroy()

    root = tk.Tk()
    root.title("Captcha")
    # calculate position x and y coordinates
    sx = root.winfo_screenwidth()
    sy = root.winfo_screenheight()
    width = 400
    height = 200
    x = (sx / 2) - (width / 2)
    y = (sy / 2) - (height / 2)
    root.geometry("%dx%d+%d+%d" % (width, height, x, y))

    # Load the image from the file
    img = Image.open("./dev/captcha.png")
    img = ImageTk.PhotoImage(img)

    # Create a label to display the image
    label = tk.Label(root, image=img)
    label.pack()

    # add some explanation
    captcha_text = "Rewrite the above code to continue"
    captcha_label = tk.Label(root, text=captcha_text)
    captcha_label.pack()

    # Create a text entry widget for user input
    entry = tk.Entry(root)
    entry.pack()

    # Create a button to submit the user input
    submit_button = tk.Button(root, text="Submit", command=lambda: submit(entry.get()))
    submit_button.pack()
    # Create a button to stop
    submit_button = tk.Button(root, text="Quit", command=lambda: root.destroy())
    submit_button.pack()

    # Run the main loop
    root.mainloop()

    return ans.upper()
