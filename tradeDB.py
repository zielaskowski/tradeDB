import hashlib
import os
import re
import sys
from datetime import date
from typing import Callable, List, Tuple

import pandas as pd

from workers import api_stooq, sql
from workers.common import read_json

"""manages getting stock data
use workers based on context
unpack provided arguments and check correctness or set defoults
remember last used db, can be also set during initialization
TODO:
    consider managing sql connection to db here

get_stooq()
Args:
    db_file: force to use different db, will create if missing
    from: table to read from [INDEXES, COMODITIES, STOCK, ETF]
    [sector]: each table is divided into sectors
            (countries, continents or industry)
            may help navigating, not
    symbol: symbol name, if no direct match, will search symbol
            in all names from table: symbol%.
            If none given will return all available for 'from' table
    start: start date for search
    end: end date for search

get_wbdata()
get data from WorldBank for country: GDP stock volume,
"""
DB_file = "./trader.sqlite"


class Trader:
    def __init__(self, db="") -> None:
        super().__init__()
        # read table sectors
        self.SECTORS = {
            "INDEXES": {"file": "./assets/indexes.json"},
            "STOCK": {"file": "./assets/stock.json"},
            "ETF": {"file": "./assets/etf.json"},
            "COMODITIES": {"file": "./assets/comodities.json"},
        }
        self.__read_sectors()
        # database location
        if not db:
            self.db = DB_file
        else:
            self.db = db

        # make sure path exists
        db = os.path.split(self.db)
        f = db[-1]
        p = db[0]
        if not os.path.exists(p):
            p = "./"
            print(f"path '{p}' dosen't exists. Using {os.path.abspath(p)}")
        self.db = os.path.join(p, f)
        # make sure file exists
        if not sql.check_sql(self.db):
            print(f"Creating new DB: {self.db}")
            if not sql.create_sql(self.db):
                sys.exit("Fatal error during DB creation")

            # populate indexes
            ##################
            print("writing INDEX info to db...")
            for region in self.SECTORS["INDEXES"]["data"]:
                api = self.SECTORS["INDEXES"]["data"][region]["api"]  # type: ignore
                dat = api_stooq.get(
                    sector_id=api["id"],  # type: ignore
                    sector_grp=api["group"],  # type: ignore
                    from_date=date.today(),
                    end_date=date.today(),
                )
                dat = self.describe_table(
                    dat=dat,
                    tab="INDEXES",
                    description=self.SECTORS["INDEXES"]["data"][region]["description"],  # type: ignore
                )
                resp = sql.put(dat=dat, tab="INDEXES", db_file=self.db)
                # write info on components
                # with STOOCK data
                ###############
                for s, c in dat.loc[:, ["symbol", "country"]].to_records(index=False):
                    datComp = api_stooq.get(
                        components=s,
                        from_date=date.today(),
                        end_date=date.today(),
                    )
                    if datComp.empty:
                        continue
                    datComp = self.describe_table(
                        dat=datComp,
                        tab="STOCK",
                        description={"country": c, "indexes": s},
                    )
                    resp = sql.put(dat=datComp, tab="STOCK", db_file=self.db)
                    if not resp:
                        sys.exit(f"FATAL: wrong info for {s}")
                if not resp:
                    sys.exit(f"FATAL: wrong {region}")

    def __read_sectors(self) -> None:
        try:
            for k in self.SECTORS:
                self.SECTORS[k]["data"] = read_json(  # type: ignore
                    self.SECTORS[k]["file"]
                )
        except Exception as err:
            sys.exit(str(err))

    def __check_sector__(self, tab: str, sector: str) -> List[str]:
        return [
            k
            for k in self.SECTORS[tab]["data"]
            if re.search(f"^{sector}.*", k, re.IGNORECASE) is not None
        ]

    def get_stooq(self, **kwargs):
        """get requested data from db or from web if missing in db"""
        # unpack arguments
        if "db_file" in kwargs.keys():
            self.db = kwargs["db_file"]
        tab = kwargs.get("tab", "")
        if not tab:
            print("Missing argument 'from'. Use -help for more info.")
            print(f"Possible tables are: {list(self.SECTORS.keys())}")
            return False
        sector = kwargs.get("sector", "ALL")
        sectors = self.__check_sector__(tab=tab, sector=sector)
        # if wrong sector: inform
        if len(sectors) > 1:
            print(f"Ambiguous sector name '{sector}' for table '{tab}'.")
            print("Possible matches:")
            [print(s) for s in sectors]
            return
        else:
            sector = sectors[0]
        symbol = kwargs.get("symbol", "")
        # ignore sector if symbol provided
        if symbol:
            sector = "ALL"
        # list all sectors if no symbol AND no sector
        if not symbol and (not sector or sector == "ALL"):
            print("Missing symbol or sector.")
            print("Possible sectors are:")
            [print(sec) for sec in self.SECTORS[tab]["data"] if sec != "ALL"]
            return
        from_date = kwargs.get("start", date.today())
        end_date = kwargs.get("end", date.today())

        dat = sql.get_indexes(
            db_file=self.db,
            tab=tab,
            sector=sector,
            symbol=symbol + "%",
            from_date=from_date,
            end_date=end_date,
        )
        if not dat:
            dat = api_stooq.get(
                sector_id=self.SECTORS[tab]["data"][sector]["api"]["id"],
                sector_grp=self.SECTORS[tab]["data"][sector]["api"]["group"],
                symbol=symbol + "%",
                from_date=from_date,
                end_date=end_date,
            )
            dat = self.describe_table(dat, tab)
            resp = sql.put(dat=dat, tab=tab, db_file=self.db)
            if resp:
                print(dat)
                return
        else:
            print(dat)
        return

    def describe_table(
        self, dat: pd.DataFrame, tab: str, description: dict
    ) -> pd.DataFrame:
        if dat.empty:
            return dat
        # extract countries
        ######
        if "country" not in description.keys():
            # for indexes, country is within name
            dat["name"], dat["country"] = self.country_txt(dat["name"])
        else:
            dat["country"] = description["country"]

        # hash table
        ######
        dat["tab"] = tab
        dat["hash"] = [
            hashlib.md5("".join(r).encode("utf-8")).hexdigest()
            for r in dat.loc[:, ["symbol", "name", "tab"]].to_records(index=False)
        ]
        dat.drop(columns=["tab"], inplace=True)

        # get dates
        ######
        def minmax(func: Callable, dat: pd.DataFrame) -> List:
            minmax_date = []
            if func.__name__ == "min":
                col = "from_date"
            else:
                col = "to_date"
            for h in dat["hash"]:
                date_sql = sql.get(
                    tab + "_DESC",
                    get=col,
                    search=[h],
                    cols=["hash"],
                    db_file=self.db,
                )["hash"].iloc[0, 0]
                HASHrows = dat["hash"] == h
                if date_sql:
                    minmax_date += [func(dat.loc[HASHrows, "date"].to_list() + [date_sql])]  # type: ignore
                else:
                    minmax_date += [func(dat.loc[HASHrows, "date"])]
            return minmax_date

        dat["from_date"] = minmax(min, dat)
        dat["to_date"] = minmax(max, dat)
        # convert currency (if not INDEX)
        #####
        if tab in ["STOCK"]:
            dat.to_csv("./dev/" + description["indexes"] + ".csv")

        # get info on components
        ######
        if "indexes" in description.keys():
            dat["indexes"] = description["indexes"]

        # get industry
        ######
        # ....
        return dat

    def country_txt(self, names: pd.Series) -> Tuple[List, List]:
        """Extract country from names
        expected is '^index name - <COUNTRY>$'
        returns tuple:
        - short name (after removing country)
        - country iso code if country found within name
        """
        # special cases
        names = names.apply(lambda x: re.sub(r"WIG.*$", x + r" - POLAND", x))
        names = names.apply(lambda x: re.sub(r"ATX.*$", r"ATX - AUSTRIA", x))

        countries = sql.get(tab="GEO", get="country", search=["%"], db_file=self.db)[
            "country"
        ]["country"]

        split = [re.split(" - ", n) for n in names]
        name_short = [s[0] for s in split]
        name_short = [
            re.sub(r"INDEX", "", n).strip() for n in name_short
        ]  # just small cleaning

        name_country = []
        for s in split:
            if len(s) > 1:
                name_country.append(s[1])
            else:
                name_country.append("null")

        # simplify countries - special cases
        name_country = [re.sub("SOUTH KOREA", "KOREA, REP.", c) for c in name_country]
        name_country = [
            re.sub("SLOVAKIA", "SLOVAK REPUBLIC", c) for c in name_country
        ]
        name_country = [re.sub("SWISS", "SWITZERLAND", c) for c in name_country]
        name_country = [re.sub("TURKEY", "TURKIYE", c) for c in name_country]
        name_country = [re.sub("U\\.S\\.", "UNITED STATES", c) for c in name_country]
        name_country = [
            re.sub("RUSSIA", "RUSSIAN FEDERATION", c) for c in name_country
        ]

        match = [re.search(c, r"-".join(countries)) for c in name_country]
        # handle what not found
        for i in range(len(match)):
            if not match[i]:
                name_short[i] = names[i]
                name_country[i] = "UNKNOWN"
        # search of iso codes needs to be in loop
        # otherway will be unique in alphabetical order
        resp = [
            sql.get(
                tab="GEO",
                get="iso2",
                search=[n + "%"],
                cols=["country"],
                db_file=self.db,
            )
            for n in name_country
        ]
        iso2 = [r["country"].iloc[0, 0] for r in resp]
        # for r in resp:
        #     i = r["country"].iloc[0, 0]
        #     if not i:
        #         iso2.append("")
        #     else:
        #         iso2.append(i)
        return (name_short, iso2)

    def world_bank(self, what: str, country: str):
        # "GDP": "INTEGER"
        # "stooq_vol": "INTEGER"
        pass
