from typing import Dict, Union
import os
import re
import sqlalchemy as db
import pandas as pd
import hashlib
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


JSON_file = "./assets/sql_scheme.json"


def get_sql(
    db_file: str, tab: str, sector: str, symbol: str, from_date: str, end_date: str
) -> Union[bool, pd.DataFrame]:
    """get data from sql db

    Args:
        db_file: db file
        tab: table in sql
        sector: group in table
        symbol: symbol from table:
        from_date: start date of data (including)
        end_date: last date of data (including)
    """
    if not check_sql(db_file):
        return False

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
    resp = execute_sql(cmd, db_file)
    # convert resp to panda
    return resp


def put_sql(dat: pd.DataFrame, tab: str, db_file: str) -> bool:
    dat["tab"] = tab
    dat["HASH"] = [
        hashlib.md5("".join(r).encode("utf-8")).hexdigest()
        for r in dat.loc[:, ["symbol", "name", "tab"]].to_records(index=False)
    ]
    dat.drop(columns=["tab"], inplace=True)

    # check if we have correct columns
    sql_columns = tab_columns(tab, db_file)
    match_columns = [col in sql_columns for col in dat.columns]
    if not all(match_columns):
        print(
            f"Not known column {list(dat.columns[[not c for c in match_columns]])} in table. Can not write to sql."
        )
        return False
    # write description (must be first becouse HASH is primary key)

    # then write table
    records = list(dat.astype('string').to_records(index=False))
    cmd = [
        f"""INSERT OR REPLACE INTO {tab} {tuple(dat.columns)}
            VALUES {values_list}
        """
        for values_list in records
    ]
    resp = execute_sql(cmd, db_file)
    # check resp if wrote correctly
    print('Data stored in sql.')
    return resp


def tab_columns(tab: str, db_file: str) -> list:
    """return list of columns for table"""
    sql_cmd = [f"pragma table_info({tab})"]
    resp = execute_sql(sql_cmd, db_file)[sql_cmd[0]]
    return resp["name"].to_list()


def check_sql(db_file: str) -> bool:
    """Check db file if aligned with scheme written in sql.json.
    Check if table exists and if has the required columns

    Args:
        db_file (str): file location

    Returns:
        bool: True if correct file, False otherway
    """
    if not os.path.isfile(db_file):
        print(f"DB file '{db_file}' is missing. Trying to create...")
        create_sql(db_file)
        return False

    sql_scheme = read_json(JSON_file)
    for i in range(len(sql_scheme)):
        tab = list(sql_scheme.keys())[i]
        scheme_cols = [k for k in sql_scheme[tab].keys() if k != "FOREIGN"]
        if tab_columns(tab, db_file) != scheme_cols:
            print(f"Wrong DB scheme in file '{db_file}'.")
            print(f"Problem with table '{tab}'")
            return False
    return True


def execute_sql(script: list, db_file: str) -> Dict:
    """Execute provided SQL commands.
    If db returns anything write as dict {command: respose as pd.DataFrame}

    Args:
        script (list): list of sql commands to execute
        db_file (string): file name

    Returns:
        Dict: dict of response from sql
            {command: response in form of pd.DataFrame}
    """
    engine = db.create_engine('sqlite:///'+db_file)
    ans = {}
    try:
        con = engine.connect()
        # db_file, detect_types=db.PARSE_COLNAMES | db.PARSE_DECLTYPES
        # cur = con.cursor()
        for cmd in script:
            resp = con.execute(db.text(cmd))
            a = resp.fetchall()
            if a:
                colnames = resp.keys()
                ans[cmd] = pd.DataFrame(a, columns=colnames)
        return ans
    except db.exc.ResourceClosedError as err:
        print("SQL operation failed:")
        print(err)
        return False
    finally:
        con.close()


def create_sql(db_file: str) -> bool:
    """Creates sql query based on sql.json and send to db.
    Perform check if created DB is aligned with scheme from sql.json file.

    Args:
        db_file (str): file name

    Returns:
        bool: True if success, False otherway
    """
    sql_scheme = read_json(JSON_file)
    # create tables query for db
    sql_cmd = []#["PRAGMA foreign_keys = ON"]
    for tab in sql_scheme:
        tab_cmd = f"CREATE TABLE {tab}("
        for col in sql_scheme[tab]:
            if col != "FOREIGN":
                tab_cmd += f"{col} {sql_scheme[tab][col]}, "
            else:  # FOREIGN
                for k, v in sql_scheme[tab][col].items():
                    tab_cmd += f"FOREIGN KEY({k}) REFERENCES {v}, "
        tab_cmd = re.sub(",[^,]*$", "", tab_cmd)  # remove last comma
        tab_cmd += ")"
        sql_cmd.append(tab_cmd)
    # last command to check if all tables were created
    sql_cmd.append("SELECT tbl_name FROM sqlite_master WHERE type='table'")
    status = execute_sql(sql_cmd, db_file)

    # write currency
    # write GEO info

    if status[sql_cmd[-1]]["tbl_name"].to_list() != list(sql_scheme.keys()):
        if os.path.isfile(db_file):
            os.remove(db_file)
        print("DB not created")
        return False
    else:
        print("empty DB created")
        return True
