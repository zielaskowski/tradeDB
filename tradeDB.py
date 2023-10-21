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
        # global variables
        self.update_dates = True
        self.update_symbols = False
        self.tab = ""
        self.region = ["%"]
        self.country = ["%"]
        self.components = ["%"]
        self.name = ["%"]
        self.symbol = ["%"]
        self.columns = ["%"]
        self.currency = "%"
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
        check argumnt against possible values\n
        Display info if missing or argument in within opts\n
        pass check if arg equal "%"\n
        Args:\n
            arg: arg value, possibly more values split with ';'\n
            arg_name: arg name\n
            opts: list of col names with possible options, or list of options\n
            tab: table where to search for opts\n
            opts_direct: opts given directly as list of options\n
            strict = False, search all matching by adding *\n
        Raise 'ValueError' if checks  fail or arg itself if all ok\n
        """
        if arg == "%":
            return [arg]
        if not arg:
            raise (
                ValueError(
                    f"Missing argument '{arg_name}'\nPossible values are: {opts}"
                )
            )

        arg = arg.upper()

        args = arg.split(";")
        args = [a.strip() for a in args]

        if tab != 'GEO':
            tab +='_DESC'
        # collect options
        if not opts_direct:
            cols = opts
            opts = []
            for col in cols:
                if o := sql.get(
                    db_file=self.db, tab=tab, get=[col], search=["%"], cols=[col]
                ):
                    opts += o[col][col].to_list()
                else:
                    raise (ValueError(""))
        # make sure the opts are unique
        opts = list(set(opts))
        opts = [o.upper() for o in opts]
        if arg == "?":
            raise (ValueError(f"Possible values are: {opts}"))

        args_checked = []
        for arg in args:
            if not strict:
                r = re.compile(arg + ".*$")
            else:
                r = re.compile(arg + "$")
            match = list(filter(r.match, opts))
            if arg in match:  # we have direct match, possibly also others
                args_checked += [arg]
                continue
            if len(match) == 1:# match also partially if unique
                args_checked += match
                continue
            if len(match) > 1:
                raise (
                    ValueError(
                        f"Ambiguous '{arg_name}' value: {arg}.\nPossible values are: {match}"
                    )
                )
            raise (
                ValueError(
                    f"Wrong argument '{arg_name}' value: {arg}.\nPossible values are: {opts}"
                )
            )
        if arg_name == 'tab' and args_checked != [self.tab]:
            # reset all argument if tab changed
            self.__init__(update_symbols=False)
        return args_checked

    def __arg_tab__(self, arg: str) -> None:
        opts = list(self.SECTORS.keys())
        opts.append("GEO")
        self.tab = self.__check_arg__(
            arg=arg, arg_name="tab", opts=opts, opts_direct=True
        )[0]
        if self.tab == 'GEO':
            self.symbol = ['%']

    def __arg_symbol__(self, arg: str) -> None:
        if self.tab == 'GEO' and arg != '%':
            print("Argument 'SYMBOL' not valid for tab='GEO'. Ignoring.")
            return
        self.symbol = self.__check_arg__(
            arg=arg, arg_name="symbol", opts=["symbol"], tab=self.tab
        )

    def __arg_name__(self, arg: str) -> None:
        if self.tab == 'GEO' and arg != '%':
            print("Argument 'NAME' not valid for tab='GEO'. Ignoring.")
            return
        self.name = self.__check_arg__(
            arg=arg,
            arg_name="name",
            opts=["name"],
            tab=self.tab
        )
        if "%" not in self.name:
            self.symbol = sql.get(
                db_file=self.db,
                tab=self.tab,
                get=["symbol"],
                search=self.name,
                cols=["name"],
            )["name"]["symbol"].to_list()

    def __arg_component__(self, arg: str) -> None:
        if  self.tab != "STOCK" and arg != '%':
            print("Argument 'COMPONENT' valid only for tab='STOCK'. Ignoring.")
            return
        self.components = self.__check_arg__(
            arg=arg,
            arg_name="components",
            opts=["name"],
            tab="INDEXES",
        )
        if "%" not in self.components:
            self.country = ['%']
            self.region = ['%']
            self.symbol = sql.index_components(
                        db_file=self.db, search=self.components
                    )

    def __arg_country__(self, arg: str) -> None:
        self.country = self.__check_arg__(
            arg=arg,
            arg_name="country",
            opts=["iso2", "country"],
            tab="GEO",
        )
        if "%" not in self.country:
            self.components = ['%']
            self.region = ['%']
            if self.tab == 'GEO':
                self.symbol = self.country
            else:
                self.symbol = sql.get_from_geo(
                    db_file=self.db, tab=self.tab, search=self.country, what=["country","iso2"]
                )

    def __arg_region__(self, arg: str) -> None:
        sector_dat = self.SECTORS["INDEXES"]["data"]
        opts = list(set([v["description"]["region"] for _, v in sector_dat.items()]))
        self.region = self.__check_arg__(
            arg=arg,
            arg_name="region",
            opts=opts,
            opts_direct=True,
        )
        if "%" not in self.region:
            self.country = ['%']
            self.components = ['%']
            if self.tab == 'GEO':
                self.symbol = self.region
            else:
                self.symbol = sql.get_from_geo(
                    db_file=self.db, tab=self.tab, search=self.region, what=["region"]
                )

    def __arg_columns__(self, arg: str) -> None:
        opts = sql.tab_columns(tab=self.tab, db_file=self.db)
        opts += sql.tab_columns(tab=self.tab + "_DESC", db_file=self.db)
        opts = [c for c in opts if c.upper() not in ["HASH"]]
        self.columns = self.__check_arg__(
            arg=arg,
            arg_name="columns",
            opts=opts,
            tab=self.tab,
            opts_direct=True,
        )

    def __arg_currency__(self, arg: str) -> None:
        self.currency = self.__check_arg__(
            arg=arg,
            arg_name="currency",
            opts=["currency_code"],
            tab="GEO",
        )[0]

    def get(self, **kwargs) -> Union[pd.DataFrame, str, None]:
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
        Use ';' to split multiple values
            [symbol]: symbol of the ticker, defoult all
            [name]: name of ticker
            [components]: list all components of given INDEXES (names)
            [country]: filter results by iso2 of country
            [region]: region to filter

            [columns]: limit result to selected columns, defoult all
            [currency]: by defoult return in country currency
            start: start date for search
            end: end date for search
        """

        if len(kwargs) == 0:
            return self.get.__doc__

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

        # unpack arguments
        ##################
        try:
            # sql table
            self.__arg_tab__(arg=kwargs.get("tab", self.tab))

            # symbol
            self.__arg_symbol__(arg=kwargs.get("symbol", ";".join(self.symbol)))

            # name
            self.__arg_name__(arg=kwargs.get("name", ";".join(self.name)))

            # components
            self.__arg_component__(
                arg=kwargs.get("components", ";".join(self.components))
            )

            # filter countries
            self.__arg_country__(arg=kwargs.get("country", ";".join(self.country)))

            # filter region
            self.__arg_region__(arg=kwargs.get("region", ";".join(self.region)))

            # columns
            self.__arg_columns__(arg=kwargs.get("columns", ";".join(self.columns)))

            # currency
            self.__arg_currency__(arg=kwargs.get("currency", self.currency))
        except ValueError as e:
            print(e)
            return ""
        
        self.update_dates = kwargs.get("update_dates", True)
        self.update_symbols = kwargs.get("update_symbols", False)
        # GEO tab must be treated specially: update dosen't make sens
        if "GEO" in self.tab:
            self.update_dates = False
            self.update_symbols = False
        
        # dates
        # if not selected particular names, display last date only
        # and do not update
        if not any([a in ["name", "symbol"] for a in kwargs.keys()]):
            from_date, to_date = self.__set_dates__()
            if self.update_dates:
                print("Date range changed to last available data.")
                print(
                    "Select particular symbol(s) or name(s) if you want different dates."
                )
                self.update_dates = False
        else:
            from_date, to_date = self.__set_dates__(kwargs)

        if self.update_symbols:
            # set dates to today to limit trafic to avoid blocking
            # (done in __update_sql__)
            print("Date range changed to last working day when updating symbols.")
            self.__update_sql__(region=self.region)
            # new symbols may arrive so update
            self.symbol = sql.get_from_geo(
                db_file=self.db, tab=self.tab, search=self.region, what=["region"]
            )
            from_date, to_date = self.__set_dates__()
            self.update_dates = False

        if self.update_dates:
            self.__update_dates__(
                tab=self.tab, symbol=self.symbol, from_date=from_date, to_date=to_date
            )

        dat = sql.query(
            db_file=self.db,
            tab=self.tab,
            symbol=self.symbol,
            from_date=from_date,
            to_date=to_date,
            columns=self.columns,
        )

        # dat = self.convert_currency(dat, self.currency)

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

    def convert_currency(self, dat, currency: str):
        # download missing data
        # assume all symbols are already in sql db
        print("currency conversion not implemented yet")
        return
        if "%" in currency:
            return dat
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
        )["country"]["country"].to_list()

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
