import os
import re
import sys
import itertools
from datetime import date, timedelta
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
        self.args = {
            "update_dates": True,
            "update_symbols": False,
            "tab": "",
            "columns": ["%"],
            "currency": "%",
            "start_date": "",
            "end_date": "",
            "date_format": r"%d-%m-%Y",
            # filtering args start at pos.7
            "region": ["%"],
            "country": ["%"],
            "components": ["%"],
            "name": ["%"],
            "symbol": ["%"],
        }
        for arg, val in self.args.items():
            setattr(self, arg, val)

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
                self.__update_sql__()

    def __update_sql__(self):
        print("writing INDEX info to db...")
        start_date, end_date = self.__set_dates__({"today": True})

        sector_dat = self.SECTORS["INDEXES"]["data"]
        if "%" not in self.region:
            sector_dat = {
                k: v
                for k, v in sector_dat.items()
                if v["description"]["region"] in self.region
            }

            for key, region in sector_dat.items():  # type: ignore
                print(f"...downloading indexes for {key}")
                dat = api.stooq(
                    sector_id=region["api"]["id"],  # type: ignore
                    sector_grp=region["api"]["group"],  # type: ignore
                    from_date=start_date,
                    to_date=end_date,
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
                            from_date=start_date,
                            to_date=end_date,
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
        opts: List,
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
        Raise 'ValueError' if checks  fail or arg itself if all ok
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

        if tab != "GEO":
            tab += "_DESC"
        # collect options
        if not opts_direct:
            cols = opts
            opts = [
                v[k].to_list()
                for k, v in sql.get(
                    db_file=self.db, tab=tab, get=cols, search=["%"], where=cols
                ).items()
                if not v.empty
            ]
            opts = list(itertools.chain(*opts))  # flatten list
        # make sure the opts are unique
        opts = list(set(opts))
        opts = [o.upper() for o in opts]
        if arg == "?":
            raise (ValueError(f"Possible values are: {opts}"))

        args_checked = []
        for arg in args:
            # remove possible '-' at begining of arg
            arg_strip = arg.lstrip("-")
            if not strict:
                r = re.compile(arg_strip + ".*$")
            else:
                r = re.compile(arg_strip + "$")
            match = list(filter(r.match, opts))
            if arg_strip in match:  # we have direct match, possibly also others
                args_checked += [arg]
                continue
            if len(match) == 1:  # match also partially if unique
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
        if arg_name == "tab" and args_checked != [self.tab]:
            # reset all argument if tab changed
            self.__init__(update_symbols=False)
        return args_checked

    def __arg_tab__(self, arg: str) -> None:
        opts = list(self.SECTORS.keys())
        opts.append("GEO")
        self.tab = self.__check_arg__(
            arg=arg, arg_name="tab", opts=opts, opts_direct=True
        )[0]
        if self.tab == "GEO":
            self.symbol = ["%"]

    def __arg_symbol__(self, arg: str) -> None:
        if self.tab == "GEO" and arg != "%":
            print("Argument 'SYMBOL' not valid for tab='GEO'. Ignoring.")
            return
        self.symbol = self.__check_arg__(
            arg=arg, arg_name="symbol", opts=["symbol"], tab=self.tab
        )
        if "%" not in self.symbol:
            self.name = ["%"]
            self.components = ["%"]
            self.country = ["%"]
            self.region = ["%"]

    def __arg_name__(self, arg: str) -> None:
        if self.tab == "GEO" and arg != "%":
            print("Argument 'NAME' not valid for tab='GEO'. Ignoring.")
            return
        self.name = self.__check_arg__(
            arg=arg, arg_name="name", opts=["name"], tab=self.tab
        )
        if "%" not in self.name:
            self.symbol = sql.getDF(
                db_file=self.db,
                tab=self.tab + "_DESC",
                get=["symbol"],
                search=self.name,
                where=["name"],
            )["symbol"].to_list()
            self.components = ["%"]
            self.country = ["%"]
            self.region = ["%"]

    def __arg_component__(self, arg: str) -> None:
        if self.tab != "STOCK" and arg != "%":
            print("Argument 'COMPONENTS' valid only for tab='STOCK'. Ignoring.")
            return
        self.components = self.__check_arg__(
            arg=arg,
            arg_name="components",
            opts=["name"],
            tab="INDEXES",
        )
        if "%" not in self.components:
            self.country = ["%"]
            self.region = ["%"]
            self.symbol = sql.index_components(db_file=self.db, search=self.components)

    def __arg_country__(self, arg: str) -> None:
        self.country = self.__check_arg__(
            arg=arg,
            arg_name="country",
            opts=["iso2", "country"],
            tab="GEO",
        )
        if "%" not in self.country:
            self.region = ["%"]
            if self.tab == "GEO":
                self.symbol = self.country
            else:
                self.symbol = sql.get_from_geo(
                    db_file=self.db,
                    tab=self.tab,
                    search=self.country,
                    what=["country", "iso2"],
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
            if self.tab == "GEO":
                self.symbol = self.region
            else:
                self.symbol = sql.get_from_geo(
                    db_file=self.db, tab=self.tab, search=self.region, what=["region"]
                )

    def __arg_columns__(self, arg: str) -> None:
        opts = sql.tab_columns(tab=self.tab, db_file=self.db)
        opts += sql.tab_columns(tab=self.tab + "_DESC", db_file=self.db)
        if self.tab == "STOCK":
            opts += ["indexes"]
        opts = [c for c in opts if c not in ["hash"]]
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
            opts=["symbol", "name"],
            tab="CURRENCY",
        )[0]

    def get(self, **kwargs) -> Union[pd.DataFrame, str]:
        """get requested data from db or from web if missing in db

        Args:
            Bool[update_symbols]: if True, update symbols from web, can be limited with region
                            may be time consuming and usually not needed
                            defoult False
            Bool[update_dates]:   if requested dates not present in db - update
                            set to False for speed and to limit network transfer
                            defoult True
            str[db_file]: force to use different db, will create if missing
            str[tab]: table to read from [INDEXES, COMODITIES, STOCK, ETF, GEO]
        Symbol filters:
        check correctness of each filter and display available option if no match
        or matching possibilities if ambigues
        If many args given the last on below list will matter
        If none is given will list all symbols and cols for table 'tab'
        Use '?' to list allowed values
        Use ';' to split multiple values
        Use '-' to exclude value
            List[symbol]: symbol of the ticker, defoult all
            List[name]: name of ticker
            List[components]: list all components of given INDEXES (names)
            List[country]: filter results by iso2 of country
            List[region]: region to filter

            List[columns]: limit result to selected columns, defoult all
            str[currency]: by defoult return in country currency
            Date[start_date]: start date for search
            Date[end_date]: end date for search
            str[date_format]: python strftime format, defoult is '%d-%m-%Y'
        """

        if len(kwargs) == 0:
            print(self.get.__doc__)
            return ""

        # warn if we have overlaping arguments or unknow argument
        args = []
        for a in kwargs.keys():
            try:
                idx = list(self.args.keys()).index(a)
                args += [a] if a in kwargs.keys() and idx > 7 else []
            except ValueError:
                print(f"Unknown argument '{a}'. Ignoring")
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
        self.date_format = kwargs.get("date_format", self.date_format)
        if not any([a in ["name", "symbol"] for a in kwargs.keys()]):
            self.start_date, self.end_date = self.__set_dates__()
            if self.update_dates:
                print("Date range changed to last available data.")
                print(
                    "Select particular symbol(s) or name(s) if you want different dates."
                )
                self.update_dates = False
        else:
            self.start_date, self.end_date = self.__set_dates__(kwargs)

        if self.update_symbols:
            # set dates to today to limit trafic to avoid blocking
            # (done in __update_sql__)
            print("Date range changed to last working day when updating symbols.")
            self.__update_sql__()
            # new symbols may arrive so update
            self.symbol = sql.get_from_geo(
                db_file=self.db, tab=self.tab, search=self.region, what=["region"]
            )
            self.start_date, self.end_date = self.__set_dates__()
            self.update_dates = False

        if self.update_dates:
            self.__update_dates__()

        dat = sql.query(
            db_file=self.db,
            tab=self.tab,
            symbol=self.symbol,
            from_date=self.start_date,
            to_date=self.end_date,
            columns=self.columns,
        )
        if dat is not None:
            self.__update_currency__(dat)
            dat = self.convert_currency(dat)
            print(dat)
            return ""

        return ""

    def convert_currency(self, dat: pd.DataFrame) -> pd.DataFrame:
        if self.currency == "%":
            return dat
        cols = dat.columns  # so we can restore
        curFrom = sql.currency_of_country(
            db_file=self.db, country=dat["country"].to_list()
        )
        curFrom.rename(columns={"symbol": "cur_from"}, inplace=True)
        dat = dat.merge(
            curFrom, left_on="country", right_on="iso2", how="left", suffixes=("", "_y")
        )
        dat["cur_to"] = self.currency
        dat["val_from"] = sql.currency_rate(
            db_file=self.db,
            dat=dat[["cur_from", "date"]].rename(columns={"cur_from": "symbol"}),
        )
        dat["val_to"] = sql.currency_rate(
            db_file=self.db,
            dat=dat[["cur_to", "date"]].rename(columns={"cur_to": "symbol"}),
        )
        dat["val"] = dat["val"] / dat["val_from"] * dat["val_to"]
        dat = dat.reindex(columns=cols)
        return dat

    def __set_dates__(self, dates={}) -> Tuple:
        if not dates:
            return None, None
        else:
            start_date = dates.get("start_date", date.today())
            end_date = dates.get("end_date", date.today())
            start_date, end_date = biz_date(
                start_date, end_date, format=self.date_format
            )
            return start_date, end_date

    def __missing_dates__(self, dat: pd.DataFrame) -> pd.DataFrame:
        # compare avialable date range with requested dates
        # leave only symbols that needs update
        min_dates = dat["from_date"] > self.start_date
        max_dates = dat["to_date"] < self.end_date
        dat = dat.loc[min_dates | max_dates]
        # avoid 'holes' in date series
        # adjust from_date or to_date to begining/end existing period
        dat.loc[:, "from_date"] = min(dat["from_date"].to_list() + [self.start_date])
        dat.loc[:, "to_date"] = max(dat["to_date"].to_list() + [self.end_date])
        return dat

    def __update_dates__(self) -> None:
        # download missing data
        # assume all symbols are already in sql db
        if self.tab == "GEO":
            return

        symbolDF = sql.getDF(
            tab=self.tab + "_DESC",
            search=self.symbol,
            where=["symbol"],
            db_file=self.db,
        )
        symbolDF = self.__missing_dates__(symbolDF)

        if not symbolDF.empty:
            info = print
            with alive_bar(len(symbolDF)) as bar:
                for row in symbolDF.itertuples(index=False):
                    if info:
                        info("...updating dates")
                        info = None
                    dat = api.stooq(
                        from_date=row.from_date,
                        to_date=row.to_date,
                        symbol=row.symbol,
                    )
                    if dat.empty:
                        print("no data on web")  # DEBUG
                        continue

                    if dat.iloc[0, 0] == "asset removed":
                        sql.rm_all(tab=self.tab, symbol=row.symbol, db_file=self.db)
                        print("symbol removed")  # DEBUG
                        continue

                    dat = self.__describe_table__(
                        dat=dat,
                        tab=self.tab,
                        description=row._asdict(),
                    )
                    resp = sql.put(dat=dat, tab=self.tab, db_file=self.db)
                    if not resp:
                        sys.exit(f"FATAL: wrong data for '{row.name}'")
                    bar()

    def __update_currency__(self, dat) -> None:
        # download missing data
        # assume all symbols are already in sql db
        if "%" in self.currency:
            return dat
        curDF = sql.currency_of_country(db_file=self.db, country=set(datgit ["country"]))
        # include also final currency in update
        curDest = sql.getDF(
            tab="CURRENCY_DESC",
            search=[self.currency],
            db_file=self.db,
            where=["symbol"],
        )
        curDF = pd.concat(
            [curDF, curDest.reindex(columns=curDF.columns)], ignore_index=True
        )
        curDF = self.__missing_dates__(curDF)

        if not curDF.empty:
            for row in curDF.itertuples(index=False):
                cur_val = api.ecb(
                    from_date=row.from_date, end_date=row.to_date, symbol=row.symbol
                )
                cur_val = self.__describe_table__(
                    dat=cur_val,
                    tab="CURRENCY",
                    description=row._asdict(),
                )
                resp = sql.put(dat=cur_val, tab="CURRENCY", db_file=self.db)
                if not resp:
                    sys.exit(f"FATAL: wrong data for '{row.symbol}'")

    def __describe_table__(
        self, dat: pd.DataFrame, tab: str, description: dict
    ) -> pd.DataFrame:
        if dat.empty:
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
                date_sql = sql.getDF(
                    tab=tab + "_DESC",
                    get=[col],
                    search=[h],
                    where=["hash"],
                    db_file=self.db,
                )
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

        countries = sql.getL(
            tab="GEO",
            get=["country"],
            db_file=self.db,
            where=["country"],
        )

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
            sql.getL(
                tab="GEO",
                get=["iso2"],
                search=[n + "%"],
                where=["country"],
                db_file=self.db,
            )
            for n in name_country
        ]
        iso2 = [r[0] for r in resp]
        return (name_short, iso2)

    def world_bank(self, what: str, country: str):
        # "GDP": "INTEGER"
        # "stooq_vol": "INTEGER"
        pass
