from typing import Dict, Union, List
import os
import re
import sqlite3
import pandas as pd
import wbdata as wb
from datetime import date
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


SQL_file = "./assets/sql_scheme.json"
CURR_file = "./assets/currencies.csv"


def get_index(
    db_file: str,
    tab: str,
    sector: str,
    symbol: str,
    from_date: str,
    end_date: str
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
    resp = execute_sql(cmd, db_file)
    return resp


def put_sql(dat: pd.DataFrame, tab: str, db_file: str) -> Dict:

    # check if we have correct columns
    sql_columns = tab_columns(tab, db_file)
    sql_columns_desc = tab_columns(tab+'_DESC', db_file)
    match_columns = [col in sql_columns+sql_columns_desc
                     for col in dat.columns]
    if not all(match_columns):
        print(
            f"Not known column {list(dat.columns[[not c for c in match_columns]])} in table. Can not write to sql."
        )
        return {}

    # write description (must be first becouse HASH is primary key)
    resp = write_table(dat.loc[:, [c in sql_columns_desc for c in dat.columns]],
                       tab+'_DESC',
                       db_file)
    # then write table
    resp = write_table(dat.loc[:, [c in sql_columns for c in dat.columns]],
                       tab,
                       db_file)
    if resp:
        print('Data stored in sql.')
    return resp


def write_table(dat: pd.DataFrame,
                tab: str,
                db_file: str) -> Dict[str, pd.DataFrame]:
    """writes DataFrame to SQL table 'tab'
    """
    records = list(dat
                   .astype('string')
                   .to_records(index=False))
    cmd = [
        f"""INSERT OR REPLACE INTO {tab} {tuple(dat.columns)}
            VALUES {values_list}
        """
        for values_list in records
    ]
    return execute_sql(cmd, db_file)


def get_sql(tab: str,
            get: str,
            search: list,
            db_file: str,
            cols=['all']) -> Dict[str, pd.DataFrame]:
    """get info from geo table

    Args:
        tab: table to search
        get: column name to extract
        search: what to get (use '*' for everything)
        cols: columns used for searching
    """
    get = get.lower()
    all_cols = tab_columns(tab=tab, db_file=db_file)
    search = [s.upper() for s in search]
    if cols[0].lower() == 'all':
        cols = all_cols
    else:
        cols = [c.lower() for c in cols]
        cols = [re.sub(r'hash', 'HASH', c, flags=re.IGNORECASE)
                for c in cols]
    if get not in all_cols:
        print(f"Not correct get={get} argument.")
        print(f"possible options: {cols}")
        return {}

    # check if tab exists!
    sql_scheme = read_json(SQL_file)
    if tab not in sql_scheme.keys():
        print('Wrong table name. Existing tables:')
        print(list(sql_scheme.keys()))
        return {}

    cmd = []
    for c in cols:
        part_cmd = f"SELECT {get} FROM {tab} WHERE "
        part_cmd += ' '.join([f"{c} LIKE '{s}' OR " for s in search])
        part_cmd += f"{c} LIKE 'none'"  # just to close last 'OR'
        cmd += [part_cmd]
    resp = execute_sql(cmd, db_file)
    # rename resp keys to column name
    resp = {cols[cmd.index(c)]: resp[c]
            for c in cmd}
    return resp


def tab_columns(tab: str, db_file: str) -> List[str]:
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

    sql_scheme = read_json(SQL_file)
    for i in range(len(sql_scheme)):
        tab = list(sql_scheme.keys())[i]
        scheme_cols = [k for k in sql_scheme[tab].keys() if k != "FOREIGN"]
        if tab_columns(tab, db_file) != scheme_cols:
            print(f"Wrong DB scheme in file '{db_file}'.")
            print(f"Problem with table '{tab}'")
            return False
    return True


def execute_sql(script: list, db_file: str) -> Dict[str, pd.DataFrame]:
    """Execute provided SQL commands.
    If db returns anything write as dict {command: respose as pd.DataFrame}

    Args:
        script (list): list of sql commands to execute
        db_file (string): file name

    Returns:
        Dict: dict of response from sql
            {command: response in form of pd.DataFrame}
    """
    ans = {}
    # Foreign key constraints are disabled by default,
    # so must be enabled separately for each database connection.
    script = ["PRAGMA foreign_keys = ON"]+script
    try:
        con = sqlite3.connect(
            db_file, detect_types=sqlite3.PARSE_COLNAMES | sqlite3.PARSE_DECLTYPES
        )
        cur = con.cursor()
        for cmd in script:
            cur.execute(cmd)
            a = cur.fetchall()
            if a:
                colnames = [c[0] for c in cur.description]
                ans[cmd] = pd.DataFrame(a, columns=colnames)
            else:
                ans[cmd] = pd.DataFrame([''])
        con.commit()
        return ans
    except sqlite3.IntegrityError as err:
        print('In command:')
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


def create_sql(db_file: str) -> bool:
    """Creates sql query based on sql.json and send to db.
    Perform check if created DB is aligned with scheme from sql.json file.

    Args:
        db_file (str): file name

    Returns:
        bool: True if success, False otherway
    """
    sql_scheme = read_json(SQL_file)
    # create tables query for db
    sql_cmd = []
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

    if status[sql_cmd[-1]]["tbl_name"].to_list() != list(sql_scheme.keys()):
        if os.path.isfile(db_file):
            os.remove(db_file)
        print("DB not created")
        return False

    # write GEO info
    records = __create_geo__()
    cmd = [f"""INSERT OR REPLACE INTO GEO {tuple(sql_scheme['GEO'].keys())}
                            VALUES {g}
            """ for g in records]
    status = execute_sql(cmd, db_file)
    if not status:
        return False
    print("new DB created")
    return True


def __create_geo__() -> list:
    """create input for GEO table:
    - countries with iso code and region
    - currency of each country"""
    sql_scheme = read_json(SQL_file)
    countries = [(c['iso2Code'],
                  c['name'],
                  c['region']['iso2code'],
                  c['region']['value'])
                 for c in wb.search_countries(".*")
                 if c['region']['value'] != 'Aggregates']
    con = pd.DataFrame(countries, columns=[
                       "iso2", "country", "iso2_region", "region"])
    con = con.apply(lambda x: x.str.upper())

    cur = pd.read_csv(CURR_file)
    cur = cur.loc[cur['withdrawal_date'].isna(), :]
    cur["Entity"] = cur["Entity"].apply(lambda x: re.sub(r"\\s*\(", ", ", x))
    cur["Entity"] = cur["Entity"].apply(lambda x: re.sub(r"\)$", "", x))

    geo = con.merge(right=cur, how="left",
                    left_on="country", right_on="Entity")
    geo = geo[["iso2", "country", "iso2_region",
               "region", "currency", "code", "numeric_code"]]
    geo['last_upd'] = date.today()
    geo = geo.set_axis(list(sql_scheme['GEO'].keys()), axis='columns')
    geo.fillna("", inplace=True)
    return list(geo.astype('string').to_records(index=False))
