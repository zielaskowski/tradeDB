import json
from typing import Dict
import re
from datetime import date


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
