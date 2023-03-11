import os
import re
import sqlite3
from datetime import datetime as dt
from typing import Dict, List

import pandas as pd
import wbdata as wb

from workers.common import read_json

"""manages SQL db.
By defoult the db is stooq.sqlite in app directory, will create if missing
DB structure is described in ./asstes/sql.json

Args:
    db_file: force to use different db, will create if missing
    from: table to read from [INDEXES, COMODITIES, STOCK, ETF]
    sector: each table is divided into sectors
            (countries, continents or industry)
            may help navigating, not
    symbol: symbol name, if no direct match, will search symbol
            in all names from table: symbol%.
            If none given will return all available for 'from' table
    start: start date for search
    end: end date for search
"""


SQL_file = "./assets/sql_scheme.jsonc"
CURR_file = "./assets/currencies.csv"


def query(
    db_file: str, region: str, symbol: str, from_date: dt, end_date: dt
) -> Dict:
    """get data from sql db about index
    (from tabs: INDEXES, INDEXES_DESC, GEO)
    may also translate currency

    using get_sql() (kind of wrapper)

    Args:
        db_file: db file
        tab: table in sql
        sector: group in table
        symbol: symbol from table:
        from_date: start date of data (including)
        end_date: last date of data (including)
    """
    if not check_sql(db_file):
        return {}

    cmd = [
        f"""SELECT * FROM {tab}
                WHERE name LIKE '{symbol}'
                AND strftime('%s',date) BETWEEN
                    strftime('%s',{from_date}) AND strftime('%s',{end_date})
                """,
        f"""SELECT * FROM {tab}_DESC
                WHERE name LIKE '{symbol}'
                AND sector LIKE '{sector}'""",
    ]
    resp = __execute_sql__(cmd, db_file)
    return resp


def get_component(db_file: str, search: str) -> list:
    """
    Return components of given sysmbol.
    Limit components to given table only
    """
    cmd = """SELECT
                s.symbol
            FROM
                STOCK_DESC s
            INNER JOIN INDEXES_DESC i ON i.hash=c.indexes_hash
            INNER JOIN COMPONENTS c on s.hash = c.stock_hash
                WHERE i.symbol LIKE 'WIG20'
        """
    resp = __execute_sql__([cmd], db_file=db_file)
    if not resp:
        return []
    else:
        return resp[cmd]['symbol'].to_list()


def tab_exists(tab: str) -> bool:
    # check if tab exists!
    sql_scheme = read_json(SQL_file)
    if tab not in sql_scheme.keys():
        print("Wrong table name. Existing tables:")
        print(list(sql_scheme.keys()))
        return False
    return True


def put(dat: pd.DataFrame, tab: str, db_file: str) -> Dict:
    # put DataFrame into sql at table=tab
    # if description table exists, writes first to 'tab_desc'
    # takes from DataFrame only columns present in sql table
    # check if tab exists!
    if not tab_exists(tab):
        return {}
    # all data shall be in capital letters!
    dat = dat.apply(lambda x: x.str.upper() if isinstance(x, str) else x)  # type: ignore

    sql_scheme = read_json(SQL_file)
    if tab + "_DESC" in sql_scheme.keys():
        # description must be first becouse HASH is primary key there
        tabL = [tab + "_DESC", tab]
    else:
        tabL = [tab]

    for t in tabL:
        sql_columns = tab_columns(t, db_file)
        resp = __write_table__(
            dat=dat.loc[:, [c in sql_columns for c in dat.columns]],
            tab=t,
            db_file=db_file,
        )
        if not resp:
            return resp

    ####
    # HANDLE INDEXES <-> STOCK: stock can be in many indexes!!!
    ####
    if "indexes" in dat.columns:
        hash = get(
            db_file=db_file,
            tab="INDEXES_DESC",
            get="hash",
            search=[dat.loc[0, "indexes"]],
            cols=["symbol"],
        )["symbol"].iloc[0, 0]
        components = pd.DataFrame({"stock_hash": dat["hash"], "indexes_hash": hash})
        resp = __write_table__(dat=components, tab="COMPONENTS", db_file=db_file)

    return resp


def __write_table__(dat: pd.DataFrame, tab: str, db_file: str) -> Dict[str, pd.DataFrame]:
    """writes DataFrame to SQL table 'tab'"""
    records = list(dat.astype("string").to_records(index=False))
    cmd = [
        f"""INSERT OR REPLACE INTO {tab} {tuple(dat.columns)}
            VALUES {values_list}
        """
        for values_list in records
    ]
    return __execute_sql__(cmd, db_file)


def get(
    tab: str, get: str, search: list, db_file: str, cols=["all"]
) -> Dict[str, pd.DataFrame]:
    """get info from table

    Args:
        tab: table to search
        get: column name to extract
        search: what to get (use '*' for everything)
        cols: columns used for searching
    """
    get = get.lower()
    all_cols = tab_columns(tab=tab, db_file=db_file)
    search = [s.upper() for s in search]
    if cols[0].lower() == "all":
        cols = all_cols
    else:
        cols = [c.lower() for c in cols]
    if get not in all_cols:
        print(f"Not correct get='{get}' argument.")
        print(f"possible options: {all_cols}")
        return {}

    # check if tab exists!
    if not tab_exists(tab):
        return {}

    cmd = []
    for c in cols:
        part_cmd = f"SELECT {get} FROM {tab} WHERE "
        part_cmd += " ".join([f"{c} LIKE '{s}' OR " for s in search])
        part_cmd += f"{c} LIKE 'none'"  # just to close last 'OR'
        cmd += [part_cmd]

    resp = __execute_sql__(cmd, db_file)
    # rename resp keys to column name
    resp = {cols[cmd.index(c)]: resp[c] for c in cmd}
    return resp


def tab_columns(tab: str, db_file: str) -> List[str]:
    """return list of columns for table"""
    sql_cmd = [f"pragma table_info({tab})"]
    resp = __execute_sql__(sql_cmd, db_file)[sql_cmd[0]]
    if "name" not in list(resp):
        # table dosen't exists
        return []
    return resp["name"].to_list()


def check_sql(db_file: str) -> bool:
    """Check db file if aligned with scheme written in sql_scheme.json.
    Check if table exists and if has the required columns.
    Creates one if necessery

    Args:
        db_file (str): file location

    Returns:
        bool: True if correct file, False otherway
    """
    # make sure if exists
    if not os.path.isfile(db_file):
        print(f"DB file '{db_file}' is missing.")
        print(f"Creating new DB: {db_file}")
        __create_sql__(db_file=db_file)
        return True

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


def __execute_sql__(script: list, db_file: str) -> Dict[str, pd.DataFrame]:
    """Execute provided SQL commands.
    If db returns anything write as dict {command: respose as pd.DataFrame}

    Args:
        script (list): list of sql commands to execute
        db_file (string): file name

    Returns:
        Dict: dict of response from sql
            {command: response in form of pd.DataFrame}
            or
            {} in case of failure
    """
    ans = {}
    # Foreign key constraints are disabled by default,
    # so must be enabled separately for each database connection.
    script = ["PRAGMA foreign_keys = ON"] + script
    try:
        con = sqlite3.connect(
            db_file, detect_types=sqlite3.PARSE_COLNAMES | sqlite3.PARSE_DECLTYPES
        )
        cur = con.cursor()
        for cmd in script:
            print(cmd)
            cur.execute(cmd)
            a = cur.fetchall()
            if a:
                colnames = [c[0] for c in cur.description]
                ans[cmd] = pd.DataFrame(a, columns=colnames)
            else:
                ans[cmd] = pd.DataFrame([""])
        con.commit()
        return ans
    except sqlite3.IntegrityError as err:
        print("In command:")
        print(cmd)
        print(err)
        return {}
    except sqlite3.Error as err:
        print("SQL operation failed:")
        print(err)
        return {}
    finally:
        cur.close()
        con.close()


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

    if status == {} or status[sql_cmd[-1]]["tbl_name"].to_list() != list(
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

    cur = pd.read_csv(CURR_file)
    cur = cur.loc[cur["withdrawal_date"].isna(), :]
    cur["Entity"] = cur["Entity"].apply(lambda x: re.sub(r"\\s*\(", ", ", x))
    cur["Entity"] = cur["Entity"].apply(lambda x: re.sub(r"\)$", "", x))

    geo = con.merge(right=cur, how="left", left_on="country", right_on="Entity")
    geo = geo[
        ["iso2", "country", "iso2_region", "region", "currency", "code", "numeric_code"]
    ]
    geo["last_upd"] = dt.today()
    geo = geo.set_axis(list(sql_scheme["GEO"].keys()), axis="columns")
    geo.fillna("", inplace=True)
    # add 'unknown' just in case
    geo.loc[len(geo)] = ["UNKNOWN" for i in geo.columns]  # type: ignore
    return geo
