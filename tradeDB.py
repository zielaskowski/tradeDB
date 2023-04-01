import hashlib
import os
import re
import sys
from datetime import datetime as dt
from typing import Callable, List, Tuple, Union

import pandas as pd

from workers import api, sql
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
            "COMODITIES": {"file": "./assets/comodities.jsonc"}
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
            self.__initiate_sql__()

    def __initiate_sql__(self):
        print("writing INDEX info to db...")
        for key, region in self.SECTORS["INDEXES"]["data"].items():  # type: ignore
            print(f"...downloading indexes for {key}")
            dat = api.stooq(
                sector_id=region["api"]["id"],  # type: ignore
                sector_grp=region["api"]["group"],  # type: ignore
                from_date=dt.today(),
                end_date=dt.today(),
            )
            dat = self.__describe_table__(
                dat=dat,
                tab='INDEXES',
                description=region["description"],  # type: ignore
            )
            resp = sql.put(dat=dat, tab="INDEXES", db_file=self.db)
            if not resp:
                sys.exit(f"FATAL: wrong data for {region}")
            for s, c in dat.loc[:, ["symbol", "country"]].to_records(index=False):
                datComp = api.stooq(
                    component=s,
                    from_date=dt.today(),
                    end_date=dt.today(),
                )
                datComp = self.__describe_table__(
                    dat=datComp,
                    tab='STOCK',
                    description={'indexes': s, 'country': c}
                )
                resp = sql.put(dat=datComp, tab='STOCK', db_file=self.db)

    def __read_sectors(self) -> None:
        try:
            for k in self.SECTORS:
                self.SECTORS[k]["data"] = read_json(  # type: ignore
                    self.SECTORS[k]["file"])
        except Exception as err:
            sys.exit(str(err))

    def __check_arg__(self,
                      arg: str,
                      arg_name: str,
                      opts: Union[List, str],
                      tab='',
                      opts_direct=False,
                      strict=False) -> str:
        """
        check argumnt against possible values
        Display info if missing or argument in within opts
        pass check if arg equal "%"
        Args:
            arg: arg value
            arg_name: arg name
            opts: list of col names with possible options, or list of options
            tab: table where to search for opts
            opts_direct: opts given directly as list of options
            strict = False, search all matching by adding *
        Returns "" if checks  fail or arg itself if all ok
        """
        if arg == "%":
            return arg
        if not arg:
            print(f"Missing argument '{arg_name}'")
            print(f"Possible values are: {opts}")
            return ""
        arg = arg.upper()

        # collect options
        if not opts_direct:
            cols = opts
            opts = []
            for col in cols:
                opts += sql.get(db_file=self.db,
                                tab=tab,
                                get=[col],
                                search=['%'],
                                cols=[col]
                                )[col][col].to_list()
        # make sure the opts are unique
        opts = list(set(opts))

        if not strict:
            r = re.compile(arg+'.*$')
        else:
            r = re.compile(arg + '$')
        match = list(filter(r.match, opts))
        if arg in match:  # if we have direct match whatever strict arg is
            match = [arg]
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
            [name]: name of ticker
            [symbol]: symbol name, if no direct match, will search symbol
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
                opts=list(self.SECTORS.keys()),
                opts_direct=True)):
            return pd.DataFrame([""])

        # symbol
        if not (symbol := self.__check_arg__(
                arg=kwargs.get("symbol", "%"),
                arg_name='symbol',
                opts=['symbol'],
                tab=tab
            )):
                return pd.DataFrame([""])
        symbol = [symbol]

        # name
        if symbol[0] == '%':
            if not (name := self.__check_arg__(
                    arg=kwargs.get("name", "%"),
                    arg_name='name',
                    opts=['name'],
                    tab=tab
                )):
                    return pd.DataFrame([""])
            if name != '%':
                symbol = sql.get(db_file=self.db,
                                get=['symbol'],
                                search=[name],
                                cols=['name']
                                )['name']['symbol'].to_list()

        # components
        if symbol[0] == '%':
            if not (component := self.__check_arg__(
                    arg=kwargs.get("component", "%"),
                    arg_name='components',
                    opts=['name'],
                    tab='INDEXES_DESC')):
                return pd.DataFrame([""])
            if component != '%':
                if tab != "STOCK":
                    print("Argument 'component' valid only for tab='STOCK'. Ignoring.")
                else:
                    symbol = sql.get_from_component(db_file=self.db,
                                                    search=component)

        # filter countries
        if symbol[0] == '%':
            if not (country := self.__check_arg__(
                arg=kwargs.get("country", "%"),
                arg_name='country',
                opts=['iso2', 'country'],
                tab='GEO'
            )):
                return pd.DataFrame([""])
            if country != '%':
                symbol = sql.get_from_geo(db_file=self.db,
                                          tab=tab,
                                          search=country,
                                          what='country')

        # filter region
        if symbol[0] == '%':
            if not (region := self.__check_arg__(
                    arg=kwargs.get("region", "%"),
                    arg_name="region",
                    opts=['region'],
                    tab='GEO')):
                return pd.DataFrame([""])
            if region != '%':
                symbol = sql.get_from_geo(db_file=self.db,
                                          tab=tab,
                                          search=region,
                                          what='region')

        # currency
        if not (currency := self.__check_arg__(
            arg=kwargs.get('currency', '%'),
            arg_name='currency',
            opts=['currency_code'],
            tab='GEO'
        )):
            return pd.DataFrame([""])

        # dates
        from_date = kwargs.get("start", dt.today())
        to_date = kwargs.get("end", dt.today())

        # download missing data
        # assume all symbols are already in sql db
        min_date = min(sql.get(tab=tab+'_DESC',
            get=['from_date'],
            search=symbol,
            cols=['symbol']
            )['symbol']['from_date'])
        max_date=max(sql.get(tab=tab+'_DESC',
            get=['to_date'],
            search=symbol,
            cols=['symbol']
            )['symbol']['to_date'])
        if min_date > from_date or max_date < to_date:
            for s in symbol:
                dat = api.stooq(from_date=min_date, end_date=max_date,symbol=s)
                dat = self.__describe_table__(dat=dat,tab=tab,description={})
                resp = sql.put(dat=dat, tab=tab, db_file=self.db)
                if not resp:
                    sys.exit(
                        f"FATAL: wrong data for {region}{symbol}{component}")
        dat = sql.query(
            db_file=self.db,
            tab=tab,
            symbol=symbol,
            from_date=from_date,
            to_date=to_date,
        )

        if currency != '%':
            self.convert_currency(dat, currency)
        return dat

    def convert_currency(self, dat, currency):
        # download missing data
        # assume all symbols are already in sql db
        min_date = min(sql.get(tab=tab+'_DESC',
            get=['from_date'],
            search=symbol,
            cols=['symbol']
            )['symbol']['from_date'])
        max_date=max(sql.get(tab=tab+'_DESC',
            get=['to_date'],
            search=symbol,
            cols=['symbol']
            )['symbol']['to_date'])
        if min_date > from_date or max_date < to_date:
            for s in symbol:
                dat = api.stooq(from_date=min_date, end_date=max_date,symbol=s)
                dat = self.__describe_table__(dat=dat,tab=tab,description={})
                resp = sql.put(dat=dat, tab=tab, db_file=self.db)
                if not resp:
                    sys.exit(
                        f"FATAL: wrong data for {region}{symbol}{component}")
        
        country_from = list(set(dat['country']))
        from_date = min(dat['from_date'])
        to_date = max(dat['to_date'])

        for c in country_from:
            country_to = sql.get(tab='GEO',
                            get=['currency_code'],
                            search=[country],
                            cols=['iso2'],
                            db_file=self.db)['iso2']['iso2'][0]
            cur_dat = api.ecb(from_date=from_date,
                            end_date=to_date,
                            symbol=symbol)
            cur_dat = self.__describe_table__(dat=cur_dat,
                                            tab='CURRENCY',
                                            description={'iso2': symbol})

    def __describe_table__(
        self, dat: pd.DataFrame, tab: str, description: dict
    ) -> pd.DataFrame:
        if dat.empty:
            return dat
        if tab == 'CURRENCY':
            dat['iso2'] = description['iso2']
            return dat

        # extract countries
        ######
        if "country" not in description.keys():
            # for indexes, country is within name
            dat["name"], dat["country"] = self.__country_txt__(dat["name"])
        else:
            dat["country"] = description['country']

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
