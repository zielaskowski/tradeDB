import os
import re
import sys
from datetime import date
from typing import Callable, List, Tuple, Union, Dict

import pandas as pd
from alive_progress import alive_bar


from workers import api, sql
from workers.common import read_json, biz_date, hash_table

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
    def __init__(self, db="", update_symbols=True) -> None:
        super().__init__()
        # read table sectors
        self.SECTORS = self.__read_sectors__(
            {
                "INDEXES": {"file": "./assets/indexes.jsonc"},
                "STOCK": {"file": "./assets/stock.jsonc"},
                "ETF": {"file": "./assets/etf.jsonc"},
                "COMODITIES": {"file": "./assets/comodities.jsonc"},
            }
        )
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
            if update_symbols:
                self.__update_sql__(region=["%"])

    def __update_sql__(self, region: List):
        print("writing INDEX info to db...")
        from_date, to_date = self.__set_dates__({"today": True})

        sector_dat = self.SECTORS["INDEXES"]["data"]
        if "%" not in region:
            sector_dat = {
                k: v
                for k, v in sector_dat.items()
                if v["description"]["region"] in region
            }

            for key, region in sector_dat.items():  # type: ignore
                print(f"...downloading indexes for {key}")
                dat = api.stooq(
                    sector_id=region["api"]["id"],  # type: ignore
                    sector_grp=region["api"]["group"],  # type: ignore
                    from_date=from_date,
                    to_date=to_date,
                )
                dat = self.__describe_table__(
                    dat=dat,
                    tab="INDEXES",
                    description=region["description"],  # type: ignore
                )
                resp = sql.put(dat=dat, tab="INDEXES", db_file=self.db)
                if not resp:
                    sys.exit(f"FATAL: wrong data for {region}")
                with alive_bar(len(dat.index)) as bar:
                    for s, c in dat.loc[:, ["symbol", "country"]].to_records(
                        index=False
                    ):
                        datComp = api.stooq(
                            component=s,
                            from_date=from_date,
                            to_date=to_date,
                        )
                        if datComp.empty:
                            # no components for index
                            continue
                        datComp = self.__describe_table__(
                            dat=datComp,
                            tab="STOCK",
                            description={"indexes": s, "country": c},
                        )
                        resp = sql.put(dat=datComp, tab="STOCK", db_file=self.db)
                        if not resp:
                            sys.exit(f"FATAL: wrong data for {s}")
                        bar()

    def __read_sectors__(self, address: Dict) -> Dict:
        try:
            for k in address:
                address[k]["data"] = read_json(address[k]["file"])  # type: ignore
        except Exception as err:
            sys.exit(str(err))
        return address

    def __check_arg__(
        self,
        arg: str,
        arg_name: str,
        opts: Union[List, str],
        tab="",
        opts_direct=False,
        strict=False,
    ) -> List:
        """
        check argumnt against possible values
        Display info if missing or argument in within opts
        pass check if arg equal "%"
        Args:
            arg: arg value, possibly more values split with ';'
            arg_name: arg name
            opts: list of col names with possible options, or list of options
            tab: table where to search for opts
            opts_direct: opts given directly as list of options
            strict = False, search all matching by adding *
        Returns "" if checks  fail or arg itself if all ok
        """
        if arg == "%":
            return [arg]
        if not arg:
            print(f"Missing argument '{arg_name}'")
            print(f"Possible values are: {opts}")
            return []

        arg = arg.upper()

        args = arg.split(";")
        args = [a.strip() for a in args]

        # collect options
        if not opts_direct:
            cols = opts
            opts = []
            for col in cols:
                opts += sql.get(
                    db_file=self.db, tab=tab, get=[col], search=["%"], cols=[col]
                )[col][col].to_list()
        # make sure the opts are unique
        opts = list(set(opts))
        opts = [o.upper() for o in opts]
        if arg == "?":
            print(f"Possible values are: {opts}")
            return []

        args_checked = []
        for arg in args:
            if not strict:
                r = re.compile(arg + ".*$")
            else:
                r = re.compile(arg + "$")
            match = list(filter(r.match, opts))
            if arg in match:  # if we have match
                args_checked += [arg]
                continue
            if len(match) == 1:
                args_checked += [match[0]]
                continue
            if not match:
                print(f"Wrong argument '{arg_name}' value: {arg}.")
                print(f"Possible values are: {opts}")
            if len(match) > 1:
                print(f"Ambiguous '{arg_name}' value: '{arg}'.")
                print(f"Possible matches: {match}")
            return []
        return args_checked

    def get(
        self, update_symbols=False, update_dates=True, **kwargs
    ) -> Union[pd.DataFrame, str, None]:
        """get requested data from db or from web if missing in db

        Args:
            update_symbols: if True, update symbols from web, can be limited with region
                            may be time consuming and usually not needed
                            defoult False
            update_dates:   if requested dates not present in db - update
                            set to False for speed and to limit network transfer
                            defoult True
            db_file: force to use different db, will create if missing
            tab: table to read from [INDEXES, COMODITIES, STOCK, ETF, GEO]
        Symbol filters:
        check correctness of each filter and display available option if no match
        or matching possibilities if ambigues
        If many args given the last on below list will matter
        If none is given will list all symbols and cols for table 'tab'
        Use '?' to list allowed values
        Use ';' to split more values
            [symbol]: symbol of the ticker, defoult all
            [name]: name of ticker
            [components]: list all components of given INDEXES (names)
            [country]: filter results by iso2 of country
            [region]: region to filter

            [columns]: limit result to selected columns, defoult all
            [currency]: by defoult return in USD
            start: start date for search
            end: end date for search
        """
        if len(kwargs) == 0:
            return self.get.__doc__
        # unpack arguments
        ##################
        # warn if we have overlaping arguments
        args = [
            e
            for e in ["symbol", "name", "components", "country", "region"]
            if e in list(kwargs.keys())
        ]
        if len(args) > 1:
            print(f"Overlaping arguments '{args}'.")
            print("The most broad will be used. See help")

        # sql file location
        self.db = kwargs.get("db_file", self.db)

        # sql table
        opts = list(self.SECTORS.keys())
        opts.append("GEO")
        if not (
            tab := self.__check_arg__(
                arg=kwargs.get("tab", ""), arg_name="tab", opts=opts, opts_direct=True
            )
        ):
            return
        tab = tab[0]
        # GEO tab must be treated specially: update dosen't make sens
        if "GEO" in tab:
            update_dates = False
            update_symbols = False
        # symbol
        if not (
            symbol := self.__check_arg__(
                arg=kwargs.get("symbol", "%"),
                arg_name="symbol",
                opts=["symbol"],
                tab=tab,
            )
        ):
            return

        # name
        if not (
            name := self.__check_arg__(
                arg=kwargs.get("name", "%"),
                arg_name="name",
                opts=["name"],
                tab=tab + "_DESC",
            )
        ):
            return
        if "%" not in name:
            symbol = sql.get(
                db_file=self.db,
                tab=tab + "_DESC",
                get=["symbol"],
                search=name,
                cols=["name"],
            )["name"]["symbol"].to_list()

        # components
        if not (
            component := self.__check_arg__(
                arg=kwargs.get("component", "%"),
                arg_name="components",
                opts=["name"],
                tab="INDEXES_DESC",
            )
        ):
            return pd.DataFrame()
        if "%" not in component:
            if tab != "STOCK":
                print("Argument 'component' valid only for tab='STOCK'. Ignoring.")
                return pd.DataFrame()
            else:
                symbol = sql.index_components(db_file=self.db, search=component)

        # filter countries
        if not (
            country := self.__check_arg__(
                arg=kwargs.get("country", "%"),
                arg_name="country",
                opts=["iso2", "country"],
                tab="GEO",
            )
        ):
            return pd.DataFrame()
        if "%" not in country:
            symbol = sql.get_from_geo(
                db_file=self.db, tab=tab, search=country, what="country"
            )

        # filter region
        sector_dat = self.SECTORS["INDEXES"]["data"]
        opts = list(set([v["description"]["region"] for _, v in sector_dat.items()]))
        if not (
            region := self.__check_arg__(
                arg=kwargs.get("region", "%"),
                arg_name="region",
                opts=opts,
                opts_direct=True,
            )
        ):
            return
        if "%" not in region and not update_symbols:
            symbol = sql.get_from_geo(
                db_file=self.db, tab=tab, search=region, what="region"
            )

        # columns
        opts = sql.tab_columns(tab=tab, db_file=self.db)
        opts += sql.tab_columns(tab=tab + "_DESC", db_file=self.db)
        opts = [c for c in opts if c.upper() not in ["HASH"]]
        if not (
            columns := self.__check_arg__(
                arg=kwargs.get("columns", "%"),
                arg_name="columns",
                opts=opts,
                tab=tab,
                opts_direct=True,
            )
        ):
            return

        # currency
        if not (
            currency := self.__check_arg__(
                arg=kwargs.get("currency", "%"),
                arg_name="currency",
                opts=["currency_code"],
                tab="GEO",
            )
        ):
            return

        # dates
        # if not selected particular names, display last date only
        # and do not update
        if not any([a in ["name", "symbol"] for a in kwargs.keys()]):
            from_date, to_date = self.__set_dates__()
            if update_dates:
                print("Date range changed to last available data.")
                print(
                    "Select particular symbol(s) or name(s) if you want different dates."
                )
                update_dates = False
        else:
            from_date, to_date = self.__set_dates__(kwargs)

        if update_symbols:
            # set dates to today to limit trafic to avoid blocking
            # (done in __update_sql__)
            print("Date range changed to last working day when updating symbols.")
            self.__update_sql__(region=region)
            # new symbols may arrive so update
            symbol = sql.get_from_geo(
                db_file=self.db, tab=tab, search=region, what="region"
            )
            from_date, to_date = self.__set_dates__()
            update_dates = False

        if update_dates:
            self.__update_dates__(
                tab=tab, symbol=symbol, from_date=from_date, to_date=to_date
            )

        dat = sql.query(
            db_file=self.db,
            tab=tab,
            symbol=symbol,
            from_date=from_date,
            to_date=to_date,
            columns=columns,
        )

        if "%" not in currency:
            self.convert_currency(dat, currency)
        return dat

    def __set_dates__(self, dates={}) -> Tuple:
        if not dates:
            return None, None
        else:
            from_date = dates.get("start", date.today())
            to_date = dates.get("end", date.today())
            from_date, to_date = biz_date(from_date, to_date)
            return from_date, to_date

    def __update_dates__(
        self,
        tab: str,
        symbol: list,
        from_date: date,
        to_date: date,
    ) -> None:
        # download missing data only if dates outside min/max in db
        # assume all symbols are already in sql db
        if tab == "GEO":
            return

        min_dates = sql.get(
            db_file=self.db,
            tab=tab + "_DESC",
            get=["from_date", "symbol"],
            search=symbol,
            cols=["symbol"],
        )["symbol"]
        max_dates = sql.get(
            db_file=self.db,
            tab=tab + "_DESC",
            get=["to_date", "symbol"],
            search=symbol,
            cols=["symbol"],
        )["symbol"]

        symbolDF = sql.get(
            db_file=self.db,
            tab=tab + "_DESC",
            get=["symbol", "name"],
            search=symbol,
            cols=["symbol"],
        )["symbol"]
        info = print
        with alive_bar(len(symbolDF)) as bar:
            for s, n in symbolDF.itertuples(index=False, name=None):
                min_date = min_dates.loc[min_dates["symbol"] == s, "from_date"].min()  # type: ignore
                max_date = max_dates.loc[max_dates["symbol"] == s, "to_date"].max()  # type: ignore
                if min_date > from_date or max_date < to_date:  # type: ignore
                    if info:
                        info("...updating dates")
                        info = None
                    dat = api.stooq(
                        from_date=min(from_date, min_date),
                        to_date=max(to_date, max_date),
                        symbol=s,
                    )
                    if dat.empty:
                        print("no data on web")  # DEBUG
                        continue

                    if dat.iloc[0, 0] == "asset removed":
                        sql.rm_all(tab=tab, symbol=s, db_file=self.db)
                        print("symbol removed")  # DEBUG
                        continue

                    dat = self.__describe_table__(
                        dat=dat, tab=tab, description={"symbol": s, "name": n}
                    )
                    resp = sql.put(dat=dat, tab=tab, db_file=self.db)
                    if not resp:
                        sys.exit(f"FATAL: wrong data for '{n}'")
                    bar()

    def convert_currency(self, dat, currency):
        # download missing data
        # assume all symbols are already in sql db
        print("currency conversion not implemented yet")
        return
        min_date = min(
            sql.get(
                tab=tab + "_DESC", get=["from_date"], search=symbol, cols=["symbol"]
            )["symbol"]["from_date"]
        )
        max_date = max(
            sql.get(tab=tab + "_DESC", get=["to_date"], search=symbol, cols=["symbol"])[
                "symbol"
            ]["to_date"]
        )
        if min_date > from_date or max_date < to_date:
            for s in symbol:
                dat = api.stooq(from_date=min_date, to_date=max_date, symbol=s)
                dat = self.__describe_table__(dat=dat, tab=tab, description={})
                resp = sql.put(dat=dat, tab=tab, db_file=self.db)
                if not resp:
                    sys.exit(f"FATAL: wrong data for {region}{symbol}{component}")

        country_from = list(set(dat["country"]))
        from_date = min(dat["from_date"])
        to_date = max(dat["to_date"])

        for c in country_from:
            country_to = sql.get(
                tab="GEO",
                get=["currency_code"],
                search=[country],
                cols=["iso2"],
                db_file=self.db,
            )["iso2"]["iso2"][0]
            cur_dat = api.ecb(from_date=from_date, end_date=to_date, symbol=symbol)
            cur_dat = self.__describe_table__(
                dat=cur_dat, tab="CURRENCY", description={"iso2": symbol}
            )

    def __describe_table__(
        self, dat: pd.DataFrame, tab: str, description: dict
    ) -> pd.DataFrame:
        if dat.empty:
            return dat

        if tab == "CURRENCY":
            dat["iso2"] = description["iso2"]
            return dat
        # for some dates the asset value can be missing
        # i.e. when stock is closed due to holidays
        # fill with zero to keep STOCK or INDEX in sql
        dat["val"].fillna(0, inplace=True)
        # extract countries
        ######
        # for indexes, country may be within name
        if "name" in dat.columns:
            dat["name"], dat["country"] = self.__country_txt__(dat["name"])

        # get info from description
        # "country", "indexes", "name", "symbol"
        ######
        for k in description:
            dat[k] = description[k]

        # hash table
        ######
        dat["hash"] = hash_table(dat, tab)

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
                )["hash"]
                if date_sql.empty:
                    minmax_date += [func(dat.loc[dat["hash"] == h, "date"])]
                else:
                    minmax_date += [
                        func(
                            dat.loc[dat["hash"] == h, "date"].to_list()  # type: ignore
                            + [date_sql.iloc[0, 0]]
                        )
                    ]
            return minmax_date

        dat["from_date"] = minmax(min, dat)
        dat["to_date"] = minmax(max, dat)

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

        countries = sql.get(
            tab="GEO", get=["country"], search=["%"], db_file=self.db, cols=["country"]
        )["country"]['country'].to_list()

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
        name_country = [re.sub("SLOVAKIA", "SLOVAK REPUBLIC", c) for c in name_country]
        name_country = [re.sub("SWISS", "SWITZERLAND", c) for c in name_country]
        name_country = [re.sub("TURKEY", "TURKIYE", c) for c in name_country]
        name_country = [re.sub("U\\.S\\.", "UNITED STATES", c) for c in name_country]
        name_country = [re.sub("RUSSIA", "RUSSIAN FEDERATION", c) for c in name_country]

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
        return (name_short, iso2)

    def world_bank(self, what: str, country: str):
        # "GDP": "INTEGER"
        # "stooq_vol": "INTEGER"
        pass
