import json
from typing import Dict, Union, Tuple
import re
from datetime import date, timedelta
from datetime import datetime as dt
import pytz


def read_json(file: str) -> Dict:
    """read json file
    ignores comments: everything from '//**' to eol"""
    try:
        with open(file, "r") as f:
            json_f = re.sub(
                "//\\*\\*.*$", "", "".join(f.readlines()),
                flags=re.MULTILINE
            )
    except IOError:
        raise Exception(f"FATAL: '{file}' is missing")
    return json.loads(json_f)


def get_cookie(file: str) -> Dict:
    # in web browser \ developer tools \ net tab:
    # right clik on req headers and copy all to file
    headerJSON = read_json(file)

    header = {}
    # the first key in JSON is somehow undefined
    firstKey = list(headerJSON.keys())[0]
    for i in headerJSON[firstKey]["headers"]:
        header[i["name"]] = i["value"]

    # update date in cookies
    cookDict = {}
    for c in header["Cookie"].split(";"):
        cdict = c.split("=")
        cookDict[cdict[0]] = cdict[1]

    cookDict["cookie_uu"] = date.today().strftime("%y%m%d") + "000"

    cookStr = ""
    for k, v in cookDict.items():
        cookStr += k + "=" + v + ";"

    header["Cookie"] = cookStr

    return header


def biz_date(from_date: Union[dt, date], to_date: Union[dt, date]) -> Tuple[date, date]:
    # first, make sure dates are datetime
    if isinstance(from_date, date):
        from_date = dt(from_date.year, from_date.month, from_date.day)
        to_date = dt(to_date.year, to_date.month, to_date.day+1)
    
    # convert date to MST and substract one day
    # this way we can be sure all stocks are already closed
    # end we got day closed values from web
    from_date = from_date.astimezone(pytz.timezone('Canada/Mountain'))
    
    # trick to move to previous bizday
    # trnsform such week is from Tu=0 to Mo=6
    from_date -= timedelta(max(1, (from_date.weekday()+6) % 7 - 3))
    to_date = to_date.astimezone(pytz.timezone('Canada/Mountain'))
    if to_date > from_date:
        to_date = from_date
    
    return (from_date.date(), to_date.date())
