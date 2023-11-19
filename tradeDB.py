import os
import re
import sys
import itertools
from datetime import date, timedelta
from typing import Callable, List, Tuple, Union, Dict, Self

import pandas as pd
from sklearn import preprocessing as prep

# import matplotlib
# matplotlib.use("Agg")  # necessery for debuging plot in VS code (not-interactive mode)
from matplotlib import pyplot as plt
from alive_progress import alive_bar
from technical_analysis import candles

from workers import api, sql
from workers.common import read_json, biz_date, hash_table


class Trader:
    """Collects financial data.
    Source of financial data is stooq.pl, which is then stored in SQL db.
    This is to speed up and limit the internet traffic
    (web data is scrapped, there is no dedicated API).
    By defoult trader.sqlite file is used as DB.
    DB file can be changed during class initialization with 'db' argument
    Currency info is taken from European bank

    Whole data is divided into groups (tables):
        - STOCK
        - INDEXES
        - ETF - to be implemented
        - COMODITIES - to be implemented
        - economic parameters - to be implemented
    All values are stored by defoult in country currency, can be converted to any currency.
    Data can be searched by regions or countries. Stocks also an be grouped by indexes
    Data is also categorized by industries (to be implemented)

    METHODS:
        - data - collected data (as pandas DataFrame)
                usefull pandas methods are to_csv and pivot
        - get - collect data and stores inside class
        - + - can add data from different queries
        - pivot - 'excell' like table
        - plot - quick plots
        - candle_apatterns - calculate bullish/bearish trend based on candles
    """

    def __init__(self, db="", update_symbols=True) -> None:
        # global variables - defoult
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
        # set dates to today, but do not inform
        self.date_change_print = False
        self.__set_dates__({"today": True})
        self.data = pd.DataFrame()
        self.kwargs = {}  # store last arguments
        self.candle_pattern_kwargs = {}  # store cp arguments
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
        if not sql.check_sql(self.db) and update_symbols:
            self.__update_sql__()

    def __join__(self, arg: Union[list, str, bool, date]) -> Union[bool, str, date]:
        return ";".join(arg) if isinstance(arg, list) else arg

    def __add__(self, trader: Self) -> Self:
        if self is trader:
            print("Cannot add Trader instance to itself")
            return self
        # align currency
        trader.kwargs["currency"] = self.__join__(getattr(self, "currency"))
        # align technical analysis
        trader.candle_pattern_kwargs = self.candle_pattern_kwargs
        # make sure dates will be updated
        trader.kwargs["update_dates"] = True
        # dates must be injected, other way biz_date will shift it
        trader.start_date = self.start_date
        trader.end_date = self.end_date
        trader.get()
        if self.data.empty:
            self.data = trader.data
            return self
        if trader.data.empty:
            return self
        self.data = pd.concat([self.data, trader.data])
        return self

    def __str__(self) -> str:
        if self.data.empty:
            return ""
        if not self.is_pivot:
            dat = self.data.reindex(columns=self.columns)
            dat = dat.drop_duplicates()
        else:
            dat = self.data
        return dat.__str__()

    def __update_sql__(self):
        print("writing INDEX info to db...")

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
                from_date=self.start_date,
                to_date=self.end_date,
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
                for row in dat.itertuples(index=False):
                    datComp = api.stooq(
                        component=row.symbol,
                        from_date=self.start_date,
                        to_date=self.end_date,
                    )
                    if datComp.empty:
                        # no components for index
                        continue
                    datComp = self.__describe_table__(
                        dat=datComp,
                        tab="STOCK",
                        description={"indexes": row.symbol, "country": row.country},
                    )
                    resp = sql.put(
                        dat=datComp, tab="STOCK", db_file=self.db, index=row.symbol
                    )
                    if not resp:
                        sys.exit(f"FATAL: wrong data for {row.symbol}")
                    bar()

    def __read_sectors__(self, address: Dict) -> Dict:
        try:
            for k in address:
                address[k]["data"] = read_json(address[k]["file"])  # type: ignore
        except Exception as err:
            sys.exit(str(err))
        return address

    def __escape_regex__(self, arg: str) -> str:
        """
        escape regex special characters
        """
        return re.sub(r"([.^$*+?{}()|[\]\\])", r"\\\1", arg)

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
            arg = self.__escape_regex__(arg)
            r = re.compile(arg + "$") if strict else re.compile(arg + ".*$")
            match = list(filter(r.match, opts))
            if arg in match:  # we have direct match, possibly also others
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
        if arg_name == "tab" and args_checked != [self.tab] and self.tab != "":
            # restore defoult arguments
            [setattr(self, a, v) for a, v in self.args.items()]
            self.date_change_print = False
            self.__set_dates__({"today": True})
            self.candle_pattern_kwargs = {}
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
        opts = list({v["description"]["region"] for v in sector_dat.values()})
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
        argL = arg.split(";")
        if "%" in argL:
            argL.remove("%")
            argL += opts
        self.__check_arg__(
            arg=";".join({a.lstrip("-") for a in argL}),
            arg_name="columns",
            opts=opts,
            tab=self.tab,
            opts_direct=True,
        )
        # handle columns names with '-' (exclude)
        minus_cols = [c.lstrip("-") for c in argL if re.match(r"^-.*", c)]
        plus_cols = [c for c in argL if re.match(r"^[^-].*", c)] or opts
        [plus_cols.remove(c) for c in minus_cols if c in plus_cols]
        self.columns = [c.strip() for c in plus_cols]

    def __arg_currency__(self, arg: str) -> None:
        self.currency = self.__check_arg__(
            arg=arg,
            arg_name="currency",
            opts=["symbol"],
            tab="CURRENCY",
        )[0]

    def get(self, **kwargs) -> Self:
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
            str[symbol]: symbol of the ticker, defoult all
            str[name]: name of ticker
            str[components]: list all components of given INDEXES (names)
            str[country]: filter results by iso2 of country
            str[region]: region to filter

            str[columns]: limit result to selected columns, defoult all
            str[currency]: by defoult return value in country currency
            Date[start_date]: start date for search
            Date[end_date]: end date for search
            str[date_format]: python strftime format, defoult is '%d-%m-%Y'
        """
        if not kwargs:
            kwargs = self.kwargs
        else:
            self.kwargs = kwargs
        if not kwargs and not self.kwargs:
            print(self.get.__doc__)
            return self
        self.is_pivot = False
        # trick to not duplicate info
        self.date_change_print = True

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
            return self

        self.update_symbols = kwargs.get("update_symbols", False)
        # block dates updating when not symbol or name selected
        if any(k in ["name", "symbol"] for k in kwargs.keys()):
            self.update_dates = kwargs.get("update_dates", True)
        else:
            self.update_dates = False

        # GEO tab must be treated specially: update dosen't make sens
        if "GEO" in self.tab:
            self.update_dates = False
            self.update_symbols = False

        # dates
        # if not selected particular names, display last date only
        # and do not update
        self.date_format = kwargs.get("date_format", self.date_format)
        if self.update_dates:
            self.__set_dates__(kwargs)
            self.__update_dates__()

        if self.update_symbols:
            # set dates to today to limit trafic to avoid blocking
            self.__set_dates__({"today": True})
            self.__update_sql__()
            # new symbols may arrive so update
            self.symbol = sql.get_from_geo(
                db_file=self.db, tab=self.tab, search=self.region, what=["region"]
            )
            self.update_dates = False

        self.data = sql.query(
            db_file=self.db,
            tab=self.tab,
            symbol=self.symbol,
            # query will return last available data if date==None
            from_date=self.start_date if self.update_dates else None,
            to_date=self.end_date if self.update_dates else None,
        )

        self.__update_currency__()
        self.convert_currency()
        if self.candle_pattern_kwargs:
            self.candle_pattern(**self.candle_pattern_kwargs)
        return self

    def to_str(self, col_name: str) -> Union[None, str]:
        """return column as string with ';' as separator"""
        if self.data.empty:
            return
        try:
            return ";".join(self.data[col_name].drop_duplicates().astype(str))
        except KeyError:
            print(f"Unknown column name. Use: {self.data.columns}")
            return

    def to_dict(self, **kwargs) -> Union[None, Dict]:
        """wrapper around pandas.DataFrame.to_dict function
        Uses orient='list' by default and limits to selected columns.
        Can forward to to_dict function with custom arguments.
        """
        if self.data.empty:
            return
        kwargs.setdefault("orient", "list")  # Set default orient if not provided
        dat = self.data.reindex(columns=self.columns).drop_duplicates()
        return dat.to_dict(**kwargs)

    def plot(self, normalize=True) -> None:
        if self.data.empty:
            return
        dat = self.pivot()

        fig, axs = plt.subplots(2, 1, sharex=True)
        fig.subplots_adjust(hspace=0.5)

        # create new dataframe from columns with suffix '_cp' in dat dataframe
        cp_cols = [col for col in dat.columns if "_cp" in col]
        if cp_cols:
            dat_cp = dat.loc[:, cp_cols]
            dat.drop(columns=cp_cols, inplace=True)
            dat_cp_np = dat_cp.to_numpy()
            for i in range(len(dat_cp_np[0])):
                axs[1].plot(dat_cp_np[:, i], label=dat_cp.columns[i])

        dat_np = dat.to_numpy()
        if normalize:
            dat_np = prep.StandardScaler().fit_transform(dat_np)
        for i in range(len(dat_np[0])):
            axs[0].plot(dat_np[:, i], label=dat.columns[i])

        fig.legend()
        plt.show()

    def __date_resample__(self, df: pd.DataFrame, date_period: str) -> pd.DataFrame:
        """recalculate OLHCV data to new period
        assume input data is daily
        can translate to weekly and monthly
        """
        periods = {"daily": "D", "weekly": "W", "monthly": "M"}
        if date_period.lower() in periods.keys():
            date_period = periods[date_period.lower()]

        resample_agg = {
            "open": "first",
            "high": "max",
            "low": "min",
            "val": "last",
            "vol": "sum",
        }

        df = df.resample(rule=date_period, on="date").agg(resample_agg)  # type: ignore
        df.reset_index(inplace=True)

        return df

    def candle_pattern(self, date_period="daily") -> pd.DataFrame:
        """recognize cnadle pattern and add column with prediction:
        - bullish are positive number (the higher value the more bullish)
        - bearisch is negative (the lower value the more bearish)
        dates must be arranged ascending (from older to newer)
        using technical-analysis library
        https://github.com/trevormcguire/technical-analysis
        """
        if self.data.empty:
            return self.data
        req_cols = ["open", "high", "low", "val", "vol", "symbol", "date"]
        if not all([c in self.data.columns for c in req_cols]):
            print("candle pattern requires open, high, low, close, volume")
            return self.data
        if 'candle_pattern' in self.data.columns:
            self.data.drop(columns=['candle_pattern'], inplace=True)
        self.candle_pattern_kwargs = {"date_period": date_period}
        self.data["date"] = pd.to_datetime(self.data["date"])
        candle_pattern = {
            "bearish_engulfing": -1,
            "dark_cloud": -1,
            "bearish_star": -1,
            "bearish_island": -1,
            "bearish_tasuki_gap": -1,
            "bullish_engulfing": +1,
            "bullish_island": 1,
            "bullish_star": 1,
            "bullish_tasuki_gap": 1,
        }

        def calc_cp(grp):
            symbol = grp.iloc[0]["symbol"]
            grp = self.__date_resample__(grp, date_period)
            grp["candle_pattern"] = 0
            for cp, cv in candle_pattern.items():
                ta_func = getattr(candles, cp)
                cp_rows = ta_func(
                    open=grp["open"],
                    low=grp["low"],
                    high=grp["high"],
                    close=grp["val"],
                )
                grp.loc[cp_rows, "candle_pattern"] += cv  # type: ignore
            grp["candle_pattern"] = grp["candle_pattern"].cumsum()
            grp["symbol"] = symbol
            return grp

        self.data.sort_values(by=["symbol", "date"], inplace=True)
        cp_DF = self.data.groupby("symbol", group_keys=False).apply(calc_cp)
        # both DFs must be sorted before merge_asof
        self.data.sort_values(by=["date"], inplace=True)
        cp_DF.sort_values(by=["date"], inplace=True)
        self.data = pd.merge_asof(
            left=self.data,
            right=cp_DF.reindex(columns=["symbol", "date", "candle_pattern"]),
            on="date",
            by="symbol",
        )
        self.data.sort_values(by=["symbol", "date"], inplace=True, ignore_index=True)
        return self.data

    def pivot(self, **kwargs) -> pd.DataFrame:
        """wrapper around pandas.DataFrame.pivot_table function
        if no args given, will use column 'symbol' for new columns
        and column 'val' as values.
        also handles if technical analysis column exists (see pivot_longer())
        Other way will forward to pivot_table function
        """
        if self.data.empty:
            return self.data

        self.data.reset_index(drop=True, inplace=True)
        if not kwargs:
            if not all(
                True for c in self.data.columns if c in ["date", "val", "symbol"]
            ):
                print('One of columns: "[date, val, symbol]" not found in dataframe')
                return self.data
            dat = self.__pivot_longer__(self.data.copy())
            dat = dat.pivot_table(columns="symbol", values="val", index="date")
        else:
            dat = self.data.pivot_table(**kwargs)
        return dat

    def __pivot_longer__(self, dat: pd.DataFrame) -> pd.DataFrame:
        """move selected columns to bottom of DataFrame, so pivot_table works"""
        if "candle_pattern" in dat.columns:
            dat["cp_col"] = dat["symbol"] + "_cp"
            # move cp_col, date, and candle_pattern cols to new dataframe
            dat_cp = dat.loc[:, ["date", "candle_pattern", "cp_col"]]
            dat_cp.rename(
                columns={"candle_pattern": "val", "cp_col": "symbol"}, inplace=True
            )
            # remove cp_col and candle_pattern cols
            dat.drop(columns=["candle_pattern", "cp_col"], inplace=True)
            dat = pd.concat([dat, dat_cp])
        return dat.reset_index(drop=True)

    def convert_currency(self) -> None:
        if self.currency == "%" or self.data.empty:
            return

        def get_currency_rate(column_symb: str, column_val: str) -> pd.DataFrame:
            return sql.currency_rate(
                db_file=self.db,
                dat=self.data[[column_symb, "date"]].rename(
                    columns={column_symb: "symbol"}
                ),
            ).rename(columns={"val": column_val, "symbol": column_symb})

        def get_currency_of_country():
            return sql.currency_of_country(
                db_file=self.db, country=set(self.data["country"].to_list())
            ).rename(columns={"symbol": "cur_from"})

        cols = self.data.columns  # so we can restore
        curFrom = get_currency_of_country()
        self.data = self.data.merge(
            curFrom, left_on="country", right_on="iso2", how="left", suffixes=("", "_y")
        )
        self.data["cur_to"] = self.currency
        self.data = self.data.merge(
            get_currency_rate(column_symb="cur_to", column_val="val_to"),
            on=["date", "cur_to"],
            how="left",
        )
        self.data = self.data.merge(
            get_currency_rate(column_symb="cur_from", column_val="val_from"),
            on=["date", "cur_from"],
            how="left",
        )

        for col in ["val", "low", "high", "open", "vol"]:
            if col in cols:
                self.data[col] = (
                    pd.to_numeric(self.data[col], errors="coerce")
                    / self.data["val_from"]
                    * self.data["val_to"]
                )

        self.data = self.data.reindex(columns=cols)
        return

    def __set_dates__(self, dates: Dict) -> None:
        """set start and end date for collecting data
        if 'today' in dict keys, will set to today (considering working days)
        other way search 'start_date' and/or 'end_date' in dict and set accordingly
        if no dates in dict, set 'self.update_dates' to None so will give last available
        """
        if "start_date" in dates.keys():
            self.start_date = biz_date(dates["start_date"], format=self.date_format)
            self.date_change_print = False
        if "end_date" in dates.keys():
            self.end_date = biz_date(dates["end_date"], format=self.date_format)
            self.date_change_print = False
        bz_today = biz_date(date.today())
        if "today" in dates.keys():
            self.start_date = bz_today
            self.end_date = bz_today
        if self.start_date == bz_today and self.end_date == bz_today:
            self.__date_change_info__()

        self.end_date = max(self.end_date, self.start_date)
        return

    def __date_change_info__(self) -> None:
        if self.tab == "GEO" or not self.date_change_print:
            return
        if self.update_symbols:
            print("Date range changed to last working day when updating symbols.")
        if not self.update_dates:
            print("Date range changed to last locally available date.")
            print("Select particular symbol, name if you want different dates.")
        if self.update_dates:
            print("Date range set to last working day.")
            print("Select start_date and/or end_date if you want different dates.")
        self.date_change_print = False

    def __missing_dates__(
        self, dat: pd.DataFrame, date_source="self_date"
    ) -> pd.DataFrame:
        # compare avialable date range with requested dates
        # requested dates can come from 'self_date' or 'self_data'
        # leave only symbols that needs update
        if date_source == "self_data" and not self.data.empty:
            start_date = self.data["date"].min()
            end_date = self.data["date"].max()
        else:
            start_date = self.start_date
            end_date = self.end_date
        min_dates = dat["from_date"] > start_date
        max_dates = dat["to_date"] < end_date
        dat = dat.loc[min_dates | max_dates]
        # avoid 'holes' in date series
        # adjust from_date or to_date to begining/end existing period
        dat.loc[:, "from_date"] = min(dat["from_date"].to_list() + [start_date])
        dat.loc[:, "to_date"] = max(dat["to_date"].to_list() + [end_date])
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
            info = True
            with alive_bar(len(symbolDF)) as bar:
                for row in symbolDF.itertuples(index=False):
                    if info:
                        print("...updating dates")
                        info = False
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

    def __update_currency__(self) -> None:
        # download missing data
        # assume all symbols are already in sql db
        if "%" in self.currency or self.data.empty:
            return
        curDF = sql.currency_of_country(
            db_file=self.db, country=set(self.data["country"])
        )
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
        curDF = self.__missing_dates__(curDF, date_source="self_data")

        if not curDF.empty:
            info = True
            with alive_bar(len(curDF)) as bar:
                for row in curDF.itertuples(index=False):
                    if info:
                        print("...updating currency")
                        info = False
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
                bar()

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
            col = "from_date" if func.__name__ == "min" else "to_date"
            for h in dat["hash"]:
                date_sql = sql.getDF(
                    tab=f"{tab}_DESC",
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
        # if start_date is smaller then ticker trade start, set 1900-1-1
        # this will prevent scraping of non existing data
        trade_start = [
            d - timedelta(days=30) > self.start_date for d in dat["from_date"]
        ]
        dat.loc[trade_start, "from_date"] = date(year=1900, month=1, day=1)
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
        names.reset_index(drop=True, inplace=True)
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
