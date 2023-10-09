import os
import re
import sys
import sqlite3
from datetime import datetime as dt
from datetime import date
from typing import Dict, List, Union

import pandas as pd
import wbdata as wb

from workers.common import read_json, hash_table

"""manages SQL db.
DB structure is described in ./asstes/sql_scheme.json
"""


SQL_file = "./assets/sql_scheme.jsonc"
CURR_file = "./assets/currencies.csv"


def query(
    db_file: str,
    tab: str,
    symbol: List[str],
    from_date: Union[None, date],
    to_date: Union[None, date],
    columns=["%"],
) -> Union[None, pd.DataFrame]:
    """get data from sql db about symbol,
    including relevant data from description tab
    (GEO tab treated separately)
    Usefull to outlook the data avaliable

    Args:
        db_file: db file
        tab: table in sql
        symbol: symbol from table:
        from_date: start date of data (including). if missing take only last date
        to_date: last date of data (including). If empty string, return only last available
        columns: columns to be included in response
    """
    if not check_sql(db_file):
        return

    desc = "_DESC"
    if tab == "GEO":
        desc = ""

    # get tab columns (omit hash)
    if "%" in columns:
        columns = [
            c
            for c in tab_columns(tab, db_file) + tab_columns(tab + desc, db_file)
            if c not in ["hash"]
        ]
    columns_txt = ",".join(set(columns))

    cmd = f"""SELECT {columns_txt}
	        FROM {tab+desc}"""
    if tab != "GEO":
        cmd += f""" INNER JOIN {tab} ON {tab}.hash={tab+desc}.hash
                WHERE """
        cmd += "("
        cmd += "".join([tab + desc + ".symbol LIKE '" + s + "' OR " for s in symbol])
        cmd += tab + desc + ".symbol LIKE 'none' "  # just to finish last OR
        cmd += ")"

        if not to_date or not from_date:
            cmd += f"""AND strftime('%s',date)=strftime('%s',{tab+desc}.to_date)"""
        else:
            cmd += f"""AND strftime('%s',date) BETWEEN
                        strftime('%s','{from_date}') AND strftime('%s','{to_date}')
                    """

    resp = __execute_sql__([cmd], db_file)
    if resp:
        return resp[cmd]
    else:
        return


def get_from_geo(db_file: str, tab: str, search: List, what: str) -> List[str]:
    """
    Return symbol from country/region.
    Limit components to given table only
    Args:
        what: which column to match
    """
    cmd = f"""SELECT
                s.symbol
            FROM
                {tab}_DESC s
            INNER JOIN GEO g ON s.country=g.iso2
                WHERE 
        """
    cmd += "("
    cmd += "".join([f"g.{what} LIKE '" + s + "' OR " for s in search])
    cmd += f"g.{what} LIKE 'none' "  # just to finish last OR
    cmd += ")"

    resp = __execute_sql__([cmd], db_file=db_file)
    if resp is None or resp[cmd].empty:
        return []
    return resp[cmd]["symbol"].to_list()


def get_from_component(db_file: str, search: List) -> list:
    """
    Return components of given index name.
    """
    cmd = f"""SELECT
                s.symbol
            FROM
                STOCK_DESC s
            INNER JOIN INDEXES_DESC i ON i.hash=c.indexes_hash
            INNER JOIN COMPONENTS c on s.hash = c.stock_hash
                WHERE 
        """
    cmd += "("
    cmd += "".join([f"i.name LIKE '" + s + "' OR " for s in search])
    cmd += f"i.name LIKE 'none' "  # just to finish last OR
    cmd += ")"
    resp = __execute_sql__([cmd], db_file=db_file)
    if resp is None or resp[cmd].empty:
        return []
    return resp[cmd]["symbol"].to_list()


def tab_exists(tab: str) -> bool:
    # check if tab exists!
    sql_scheme = read_json(SQL_file)
    if tab not in sql_scheme.keys():
        print("Wrong table name. Existing tables:")
        print(list(sql_scheme.keys()))
        return False
    return True


def put(dat: pd.DataFrame, tab: str, db_file: str) -> Union[Dict, None]:
    # put DataFrame into sql at table=tab
    # if description table exists, writes first to 'tab_desc'
    # takes from DataFrame only columns present in sql table
    # check if tab exists!
    if not tab_exists(tab):
        return 
    if dat.empty:
        return
    # all data shall be in capital letters!
    dat = dat.apply(
        lambda x: x.str.upper() if isinstance(x, str) else x  # type: ignore
    )

    sql_scheme = read_json(SQL_file)
    if tab + "_DESC" in sql_scheme.keys():
        # description must be first becouse HASH is primary key there
        tabL = [tab + "_DESC", tab]
    else:
        tabL = [tab]

    # merge with what we already know
    known = query(
        db_file=db_file,
        tab=tab,
        symbol=dat.loc[:, "symbol"].tolist(),
        from_date=date(1900, 1, 1),
        to_date=date.today(),
    )
    if known is not None and not known.empty:
        known["hash"] = hash_table(known, tab)  # add hash column

        # remove from DataFrame all rows where hash and date
        # column is equal to known DataFrame
        comp = dat.reindex(columns=known.columns)  # align columns
        comp = comp.merge(known, how="left", on=["hash"], suffixes=("", "_known"))
        # fill new data with what already known
        for c in known.columns:
            if not re.search("(date)|(val)|(symbol)|(name)|(hash)", c):
                comp[c] = comp[c].fillna(comp[c + "_known"])
        comp = comp.reindex(columns=known.columns)
        comp = comp.astype(known.dtypes)
        comp = comp.merge(known, how="outer", indicator=True)
        new = comp.loc[comp["_merge"] == "left_only"]
        rm = comp.loc[comp["_merge"] == "right_only"]
    else:
        new = dat
        rm = pd.DataFrame()

    if not new.empty:
        for t in tabL:
            if sql_columns := tab_columns(t, db_file):
                d = new.loc[:, [c in sql_columns for c in new.columns]]
            else:
                return None
            resp = __write_table__(
                dat=d,
                tab=t,
                db_file=db_file,
            )
            if not resp:
                return
            # delete asset, do not affect DESC items
            if not re.search("DESC", t) and not rm.empty:
                d = rm.loc[:, [c in sql_columns for c in rm.columns]]
                resp = rm_asset(tab=t, dat=d, db_file=db_file)

    ####
    # HANDLE INDEXES <-> STOCK: stock can be in many indexes!!!
    ####
    if "indexes" in dat.columns:
        hash = get(
            db_file=db_file,
            tab="INDEXES_DESC",
            get=["hash"],
            search=[dat.loc[0, "indexes"]],
            cols=["symbol"],
        )["symbol"]
        if not hash.empty:
            hash = hash.iloc[0, 0]
        else:
            return
        components = pd.DataFrame({"stock_hash": dat["hash"], "indexes_hash": hash})
        resp = __write_table__(dat=components, tab="COMPONENTS", db_file=db_file)
        return resp
    return {"put": "success"}


def __write_table__(
    dat: pd.DataFrame, tab: str, db_file: str
) -> Union[None, Dict[str, pd.DataFrame]]:
    """writes DataFrame to SQL table 'tab'"""
    records = list(dat.astype("string").to_records(index=False))
    cmd = [
        f"""INSERT OR REPLACE INTO {tab} {tuple(dat.columns)}
            VALUES {values_list}
        """
        for values_list in records
    ]
    cmd = [re.sub('<NA>','NULL',str(c)) for c in cmd]
    return __execute_sql__(cmd, db_file)


def get(
    tab: str, get: list, search: list, db_file: str, cols=["%"]
) -> Dict[str, pd.DataFrame]:
    """get info from table
    return as Dict:
    - each key for column searched,
    - value as DataFrame with columns selected by get
    return only unique values

    Args:
        tab: table to search
        get: column name to extract (use '%' for all columns)
        search: what to get (use '*' for everything)
        cols: columns used for searching
    """
    resp = {}
    all_cols = tab_columns(tab=tab, db_file=db_file)
    tab = tab.upper()
    search = [s.upper() for s in search]
    get = [g.lower() for g in get]
    cols = [c.lower() for c in cols]
    if cols[0] == "%":
        cols = all_cols
    if get[0] == "%":
        get = all_cols
    if not all(g in all_cols for g in get):
        print(f"Not correct get='{get}' argument.")
        print(f"possible options: {all_cols}")
        return {}

    # check if tab exists!
    if not tab_exists(tab):
        return {}

    for c in cols:
        cmd = f"SELECT {','.join(get)} FROM {tab} WHERE "
        cmd += " ".join([f"{c} LIKE '{s}' OR " for s in search])
        cmd += f"{c} LIKE 'none'"  # just to close last 'OR'
        if resp_col := __execute_sql__([cmd], db_file):
            resp[c] = resp_col[cmd].drop_duplicates()
    return resp


def __split_cmd__(script: list) -> List[List]:
    # split OR logic chain into 500 len elements
    # there is limit of 1000 tree depth
    resp = [[]]
    for cmd in script:
        cmd = re.sub(r"[\t\n\r]*", "", cmd)  # remove newline and tabs
        cmd1_re = (
            r"(^.*WHERE[^\(]*)"  # take all from begining to 'WHERE' just before '('
        )
        cmd2_re = (
            r"(\([^\()]*\))"  # everything (without parenthesis) between parenthesis
        )
        cmd3_re = r"((?<=\)).+$)"  # everything from last parenthesis to end
        res = re.findall(f"{cmd1_re}|{cmd2_re}|{cmd3_re}", cmd)
        res = ["".join(t) for t in res]  # remove empty elements from list
        if len(res) == 3:
            lenOR = len(re.findall(" OR ", res[1]))
            lenAND = len(re.findall(" AND ", res[1]))
            if lenAND != 0:
                # not possible to split AND chain
                if lenAND + lenOR > 500:
                    sys.exit("FATAL: sql cmd exceeded length limit")
            if lenOR > 500:
                cmd_new = [res[0] + c + res[2] for c in __split_list__(res[1], 500)]
                resp.append(cmd_new)
            else:
                resp.append([cmd])
        else:
            resp.append([cmd])
    return [r for r in resp if r]


def __split_list__(lst: str, nel: int) -> list:
    """
    Split list into parts with nel elements each (except last)
    """
    lst_split = re.split("OR", lst)
    n = (len(lst_split) // nel) + 1
    cmd_split = [" OR ".join(lst_split[i * nel : (i + 1) * nel]) for i in range(n)]
    # make sure each part starts and ends with parenthesis
    cmd_split = ["(" + re.sub(r"[\(\)]", "", s) + ") " for s in cmd_split]
    return cmd_split


def rm_asset(
    tab: str, dat: pd.DataFrame, db_file: str
) -> Union[None, Dict[str, pd.DataFrame]]:
    records = list(dat.astype("string").to_records(index=False))
    cmd = []
    for row in records:
        cmd.append(
                f"DELETE FROM {tab} WHERE ("+
                (' AND ').join([f"{dat.columns[i]} = '{row[i]}'" for i in range(len(row))])+
                ' )'
        )

    return __execute_sql__(cmd, db_file)


def rm_all(
    tab: str, symbol: str, db_file: str
) -> Union[None, Dict[str, pd.DataFrame]]:
    """
    Remove all instances to asset
    remove from given tab, from tab+_DESC and from COMPONENTS
    (so don't use tab='TAB_DESC'!)
    """
    symbol = symbol.upper()
    tab = tab.upper()
    # check if tab exists!
    if not tab_exists(tab):
        return
    hash = get(
        tab=tab + "_DESC",
        get=["hash"],
        search=[symbol],
        cols=["symbol"],
        db_file=db_file,
    )
    hash = hash["symbol"].loc[0, "hash"]

    cmd = [f"DELETE FROM {tab} WHERE hash='{hash}'"]
    cmd += [f"DELETE FROM COMPONENTS WHERE stock_hash='{hash}'"]
    cmd += [f"DELETE FROM {tab}_DESC WHERE hash='{hash}'"]

    return __execute_sql__(cmd, db_file)


def tab_columns(tab: str, db_file: str) -> List[str]:
    """return list of columns for table"""
    sql_cmd = f"pragma table_info({tab})"
    resp = __execute_sql__([sql_cmd], db_file)
    if not resp or resp[sql_cmd] is None or "name" not in list(resp[sql_cmd]):
        return []
    return resp[sql_cmd]["name"].to_list()


def check_sql(db_file: str) -> bool:
    """Check db file if aligned with scheme written in sql_scheme.json.
    Check if table exists and if has the required columns.
    Creates one if necessery

    Args:
        db_file (str): file location

    Returns:
        bool: True if correct file, False otherway
        (but before creates file and GEO table)
    """
    # make sure if exists
    if not os.path.isfile(db_file):
        print(f"DB file '{db_file}' is missing.")
        print(f"Creating new DB: {db_file}")
        __create_sql__(db_file=db_file)
        return False

    # check if correct sql
    sql_scheme = read_json(SQL_file)
    for i in range(len(sql_scheme)):
        tab = list(sql_scheme.keys())[i]
        scheme_cols = [k for k in sql_scheme[tab].keys() if k != "FOREIGN"]
        if tab_columns(tab, db_file) != scheme_cols:
            print(f"Wrong DB scheme in file '{db_file}'.")
            print(f"Problem with table '{tab}'")
            return False
    return True


def __execute_sql__(
    script: list, db_file: str
) -> Union[None, Dict[str, pd.DataFrame]]:
    """Execute provided SQL commands.
    If db returns anything write as dict {command: respose as pd.DataFrame}
    Split cmd if logic tree exceeds 500 (just in case as limit is 1000)

    Args:
        script (list): list of sql commands to execute
        db_file (string): file name

    Returns:
        Dict: dict of response from sql
            {command: response in form of pd.DataFrame (may be empty)}
            or
            None in case of failure
    """
    ans = {}
    cmd = ""
    # Foreign key constraints are disabled by default,
    # so must be enabled separately for each database connection.
    script = ["PRAGMA foreign_keys = ON"] + script
    script_split = __split_cmd__(script)
    try:
        con = sqlite3.connect(
            db_file, detect_types=sqlite3.PARSE_COLNAMES | sqlite3.PARSE_DECLTYPES
        )
        cur = con.cursor()
        for cmd_split in script_split:
            cmd = script[script_split.index(cmd_split)]
            for c in cmd_split:
                cur.execute(c)
                a = cur.fetchall()
                if a:
                    colnames = [c[0] for c in cur.description]
                    if cmd in ans.keys():
                        ans[cmd] = pd.concat(
                            [
                                ans[cmd].fillna(""),
                                pd.DataFrame(a, columns=colnames).fillna(""),
                            ],
                            ignore_index=True,
                        )
                    else:
                        ans[cmd] = pd.DataFrame(a, columns=colnames)
                else:
                    ans[cmd] = pd.DataFrame()
        con.commit()
        return ans
    except sqlite3.IntegrityError as err:
        print("In command:")
        print(cmd)
        print(err)
        return
    except sqlite3.Error as err:
        print("SQL operation failed:")
        print(err)
        return
    finally:
        cur.close()  # type: ignore
        con.close()  # type: ignore


def __create_sql__(db_file: str) -> bool:
    """Creates sql query based on sql_scheme.json and send to db.
    Perform check if created DB is aligned with scheme from sql.json file.
    add GEO tab

    Args:
        db_file (str): file name

    Returns:
        bool: True if success, False otherway
    """
    if os.path.isfile(db_file):
        # just in case the file exists
        os.remove(db_file)
    sql_scheme = read_json(SQL_file)
    # create tables query for db
    sql_cmd = []
    for tab in sql_scheme:
        tab_cmd = f"CREATE TABLE {tab}("
        for col in sql_scheme[tab]:
            if col != "FOREIGN":
                tab_cmd += f"{col} {sql_scheme[tab][col]}, "
            else:  # FOREIGN
                for foreign in sql_scheme[tab][col]:
                    k, v = list(foreign.items())[0]
                    tab_cmd += f"FOREIGN KEY({k}) REFERENCES {v}, "
        tab_cmd = re.sub(",[^,]*$", "", tab_cmd)  # remove last comma
        tab_cmd += ")"
        sql_cmd.append(tab_cmd)
    # last command to check if all tables were created
    sql_cmd.append("SELECT tbl_name FROM sqlite_master WHERE type='table'")
    status = __execute_sql__(sql_cmd, db_file)

    if status is None or status[sql_cmd[-1]]["tbl_name"].to_list() != list(
        sql_scheme.keys()
    ):
        if os.path.isfile(db_file):
            os.remove(db_file)
        print("DB not created")
        return False

    # write GEO info
    print("writing GEO info to db...")
    status = __write_table__(__geo_tab__(), tab="GEO", db_file=db_file)
    if not status:
        print("Problem with GEO data")
        return False

    print("new DB created")
    return True


def __geo_tab__() -> pd.DataFrame:
    """create input for GEO table.
    - countries with iso code and region come from world bank data (lib: wbdata)
    - currency of each country come from csv file"""
    sql_scheme = read_json(SQL_file)
    countries = [
        (c["iso2Code"], c["name"], c["region"]["iso2code"], c["region"]["value"])
        for c in wb.search_countries(".*")
        if c["region"]["value"] != "Aggregates"
    ]
    con = pd.DataFrame(countries, columns=["iso2", "country", "iso2_region", "region"])
    con = con.apply(lambda x: x.str.upper())
    con = con.apply(lambda x: x.str.strip())

    cur = pd.read_csv(CURR_file)
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

    geo = con.merge(right=cur, how="inner", left_on="country", right_on="Entity")
    geo = geo[
        ["iso2", "country", "iso2_region", "region", "currency", "code", "numeric_code"]
    ]
    # add 'unknown' just in case
    geo.loc[len(geo)] = ["UNKNOWN" for i in geo.columns]  # type: ignore
    geo["last_upd"] = date.today()
    geo = geo.set_axis(list(sql_scheme["GEO"].keys()), axis="columns")
    geo.fillna("", inplace=True)
    return geo
