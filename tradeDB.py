import hashlib
import os
import re
import sys
from datetime import datetime as dt
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

get_wbdata()
get data from WorldBank for country: GDP stock volume,
"""


class Trader:
    def __init__(self, db="") -> None:
        super().__init__()
        # read table sectors
        self.SECTORS = {
            "INDEXES": {"file": "./assets/indexes.jsonc"},
            "STOCK": {"file": "./assets/stock.jsonc"},
            "ETF": {"file": "./assets/etf.jsonc"},
            "COMODITIES": {"file": "./assets/comodities.jsonc"},
        }
        self.__read_sectors()
        # database location
        if not db:
            self.db = "./trader.sqlite"
        else:
            # check path
            db = os.path.split(db)[0]
            f = db[-1]
            p = db[0]
            if not os.path.exists(p):
                p = "./"
                print(f"path '{p}' dosen't exists. Using {os.path.abspath(p)}")
            self.db = os.path.join(p, f)
        # make sure file is corrcet, also create if missing
        if not sql.check_sql(self.db):
            # populate indexes
            ##################
            print("writing INDEX info to db...")
            [
                self.get(tab="INDEXES", region=region)
                for region in self.SECTORS["INDEXES"]["data"]
            ]

            [
                self.get(tab="STOCK", component=index)
                for index in self.get(tab="INDEXES", symbol="%")
            ]

    def __read_sectors(self) -> None:
        try:
            for k in self.SECTORS:
                self.SECTORS[k]["data"] = read_json(  # type: ignore
                    self.SECTORS[k]["file"]
                )
        except Exception as err:
            sys.exit(str(err))

    def __check_arg__(self,
                      arg: str,
                      arg_name: str,
                      opts: List,
                      strict=False) -> str:
        """
        check argumnt against possible values
        Display info if missing or argument in within opts
        pass check if arg equal "%"
        if strict = False, search all matching by adding *
        Returns "" if checks  fail or arg itself if all ok
        """
        if arg == "%":
            return arg
        arg = arg.upper()
        # make sure the opts are unique (region can have doubles)
        opts = list(set(opts))
        if not arg:
            print(f"Missing argument '{arg_name}'")
            print(f"Possible values are: {opts}")
            return ""
        if not strict:
            r = re.compile(arg+'.*$')
        else:
            r = re.compile(arg + '$')
        match = list(filter(r.match, opts))
        if not match:
            print(f"Wrong argument '{arg_name}' value: {arg}.")
            print(f"Possible values are: {opts}")
            return ""
        if len(match) > 1:
            print(f"Ambiguous '{arg_name}' value: '{arg}'.")
            print(f"Possible matches: {match}")
            return ""
        return match[0]

    def get(self, **kwargs) -> pd.DataFrame:
        """get requested data from db or from web if missing in db

        Args:
            db_file: force to use different db, will create if missing
            tab: table to read from [INDEXES, COMODITIES, STOCK, ETF]
            [region]: region to filter
            [components]: list all components of INDEXES
            [country]: filter results by iso2 of country
            [currency]: by defoult return in USD
            symbol: symbol name, if no direct match, will search symbol
                    in all names from table: symbol%.
                    If none given will return all available for 'from' table
            start: start date for search
            end: end date for search
        """
        # unpack arguments
        ##################

        # sql file location
        self.db = kwargs.get("db_file", self.db)

        # sql table
        if not (tab := self.__check_arg__(
            arg=kwargs.get("tab", ""),
            arg_name="tab",
            opts=list(self.SECTORS.keys())
        )
        ):
            return pd.DataFrame([""])

        # filter region
        opts = list(self.SECTORS[tab]["data"].keys())  # type: ignore
        opts += sql.get(db_file=self.db,
                        tab='GEO',
                        get=['region'],
                        search=['%'],
                        cols=['region'])['region']['region'].to_list()
        if not (region := self.__check_arg__(
                arg=kwargs.get("region", "%"),
                arg_name="region",
                opts=opts)):
            return pd.DataFrame([""])

        # filter countries
        opts = sql.get(db_file=self.db,
                       tab='GEO',
                       get=['iso2'],
                       search=['%'],
                       cols=['iso2'])['iso2']['iso2'].to_list()
        opts += sql.get(db_file=self.db,
                        tab='GEO',
                        get=['country'],
                        search=['%'],
                        cols=['country'])['country']['country'].to_list()
        if not (country := self.__check_arg__(
            arg=kwargs.get("country", "%"),
            arg_name='country',
            opts=opts
        )):
            return pd.DataFrame([""])

        # components
        if not (component := self.__check_arg__(
            arg=kwargs.get("component", "%"),
            arg_name='components',
            opts=sql.get(
                db_file=self.db,
                tab="INDEXES_DESC",
                get=["symbol"],
                search=["%"],
                cols=["symbol"])["symbol"]["symbol"].to_list())):
            return pd.DataFrame([""])

        # symbol
        symbol = kwargs.get("symbol", "")

        # COMBINATIONS LOGIC
        if tab != "STOCK" and component != "%":
            print("Argument 'component' valid only for tab='STOCK'. Ignoring.")
            component = "%"
        # no filters
        if not symbol and component == "%" and region == "%":
            print("Missing arguments. Provide at last one argument")
            return pd.DataFrame([""])

        # dates
        from_date = kwargs.get("start", dt.today())
        end_date = kwargs.get("end", dt.today())

        dat = sql.query(
            db_file=self.db,
            tab=tab,
            region=region,
            country=country,
            component=component,
            symbol=symbol + "%",
            from_date=from_date,
            end_date=end_date,
        )
        if dat:
            # convert currency
            pass
        else:
            # download from web
            api = self.SECTORS["INDEXES"]["data"][region]["api"]
            dat = api_stooq.get(
                sector_id=api["id"],  # type: ignore
                sector_grp=api["group"],  # type: ignore
                from_date=from_date,
                end_date=end_date,
            )
            dat = self.__describe_table__(
                dat=dat,
                tab="INDEXES",
                description=self.SECTORS["INDEXES"]["data"][region]["description"],
            )
            resp = sql.put(dat=dat, tab="INDEXES", db_file=self.db)
            if not resp:
                sys.exit(f"FATAL: wrong data for {region}")
            # write info on components
            # with STOOCK data
            ###############
            for s, c in dat.loc[:, ["symbol", "country"]].to_records(index=False):
                datComp = api_stooq.get(
                    components=s,
                    from_date=dt.today(),
                    end_date=dt.today(),
                )
                if datComp.empty:
                    continue
                datComp = self.__describe_table__(
                    dat=datComp,
                    tab="STOCK",
                    description={"country": c, "indexes": s},
                )
                resp = sql.put(dat=datComp, tab="STOCK", db_file=self.db)
                if not resp:
                    sys.exit(f"FATAL: wrong info for {s}")
        return dat

    def __describe_table__(
        self, dat: pd.DataFrame, tab: str, description: dict
    ) -> pd.DataFrame:
        if dat.empty:
            return dat
        # extract countries
        ######
        if "country" not in description.keys():
            # for indexes, country is within name
            dat["name"], dat["country"] = self.__country_txt__(dat["name"])
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
                    get=[col],
                    search=[h],
                    cols=["hash"],
                    db_file=self.db,
                )["hash"].iloc[0, 0]
                HASHrows = dat["hash"] == h
                if date_sql:
                    # type: ignore
                    minmax_date += [func(dat.loc[HASHrows,
                                         "date"].to_list() + [date_sql])]  # type: ignore
                else:
                    minmax_date += [func(dat.loc[HASHrows, "date"])]
            return minmax_date

        dat["from_date"] = minmax(min, dat)
        dat["to_date"] = minmax(max, dat)
        # convert currency (if not INDEX)
        #####

        # get info on components
        ######
        if "indexes" in description.keys() and tab in ["STOCK"]:
            dat["indexes"] = description["indexes"]

        # get industry
        ######
        # ....
        return dat

    def __country_txt__(self, names: pd.Series) -> Tuple[List, List]:
        """Extract country from names
        expected is '^index name - <COUNTRY>$'
        returns tuple:
        - short name (after removing country)
        - country iso code if country found within name
        """
        # special cases
        names = names.apply(lambda x: re.sub(r"WIG.*$", x + r" - POLAND", x))
        names = names.apply(lambda x: re.sub(r"ATX.*$", r"ATX - AUSTRIA", x))

        countries = sql.get(tab="GEO",
                            get=["country"],
                            search=["%"],
                            db_file=self.db)["country"]["country"]

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
        name_country = [re.sub("SOUTH KOREA", "KOREA, REP.", c)
                        for c in name_country]
        name_country = [re.sub("SLOVAKIA", "SLOVAK REPUBLIC", c)
                        for c in name_country]
        name_country = [re.sub("SWISS", "SWITZERLAND", c)
                        for c in name_country]
        name_country = [re.sub("TURKEY", "TURKIYE", c) for c in name_country]
        name_country = [re.sub("U\\.S\\.", "UNITED STATES", c)
                        for c in name_country]
        name_country = [re.sub("RUSSIA", "RUSSIAN FEDERATION", c)
                        for c in name_country]

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
                get=["iso2"],
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
