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
import matplotlib.dates as mdate
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
        self.candle_pattern_kwargs = {}  # store candle_pattern arguments
        self.candle_pattern_file = "./assets/candle_pattern.jsonc"
        self.cp_cols = {"CP":"candle_pattern", "CF":"formation"}
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
            )  # without from_date and to_date will take last session
            dat = self.__describe_table__(
                dat=dat,
                tab="INDEXES",
                description=region["description"],  # type: ignore
            )
            resp = sql.put(dat=dat, tab="INDEXES", db_file=self.db)
            if not resp:
                sys.exit(f"FATAL: wrong data for {region}")
            # get components for index
            with alive_bar(len(dat.index)) as bar:
                for row in dat.itertuples(index=False):
                    datComp = api.stooq(
                        component=str(row.symbol),
                        from_date=self.start_date,
                        to_date=self.end_date,
                    )
                    if datComp.empty:
                        # no components for index
                        bar()
                        continue
                    datComp = self.__describe_table__(
                        dat=datComp,
                        tab="STOCK",
                        description={"indexes": row.symbol, "country": row.country},
                    )
                    resp = sql.put(
                        dat=datComp, tab="STOCK", db_file=self.db, index=str(row.symbol)
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
        # add candle pattern columns
        opts += list(self.cp_cols.values())     
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

        # possibly symbol =[] , i.e. when previously requested for region where no data
        if self.symbol == []:
            self.symbol = ["%"]

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
        if "GEO" not in self.tab:
            # dates
            # class initialize with 'start' and 'end' date == today
            ##################
            self.date_format = kwargs.get("date_format", self.date_format)

            if not self.update_dates and all(
                a not in kwargs.keys() for a in ["start_date", "end_date"]
            ):
                # take last available in SQL if not updating dates
                self.start_date = sql.get_end_date(
                    db_file=self.db, tab=self.tab + "_DESC", ticker=self.symbol
                )
                self.end_date = sql.get_end_date(
                    db_file=self.db, tab=self.tab + "_DESC", ticker=self.symbol
                )

            self.__set_dates__(kwargs)

            if self.update_dates:
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
            from_date=self.start_date,
            to_date=self.end_date,
        )

        self.__update_currency__()
        self.__convert_currency__()
        if self.candle_pattern_kwargs:
            self.candle_pattern(**self.candle_pattern_kwargs)
        if not self.update_dates and self.data.empty:
            print("No data found in local DB. Consider setting update_dates=True")
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

    def plot(self, normalize=True, xticks = 20) -> None:
        if self.data.empty:
            return
        dat = self.pivot()
        
        dates=dat.reset_index("date").loc[:,"date"]
        n_months = (dates.iloc[-1] - dates.iloc[0]).days / 30
        n_months = int(n_months / xticks)
        
        axes = [0, 1, 2]
        fig, axs = plt.subplots(len(axes), 1, sharex=True)
        fig.subplots_adjust(hspace=0.5)

        def plot_cols(cols, ax_n):
            nonlocal axs, dat
            dat_cols = dat.loc[:, cols]
            dat.drop(columns=cols, inplace=True)
            dat_cols_np = dat_cols.to_numpy()
            for i in range(len(dat_cols_np[0])):
                axs[ax_n].plot(dates, dat_cols_np[:, i], label=dat_cols.columns[i])
                axs[ax_n].xaxis.set_major_formatter(mdate.DateFormatter('%Y-%m'))
                axs[ax_n].xaxis.set_major_locator(mdate.MonthLocator(interval=n_months))

        # candle pattern plot
        cp_cols = [col for col in dat.columns if "_cp" in col]
        if cp_cols:
            plot_cols(cp_cols, 1)
            axs[1].set_title("candle pattern")
        else:
            fig.delaxes(axs[1])
            axes.remove(1)

        # volumen plot
        vol_cols = [col for col in dat.columns if "_vol" in col]
        if vol_cols:
            plot_cols(vol_cols, 2)
            axs[2].set_title("volumen")
        else:
            fig.delaxes(axs[2])
            axes.remove(2)

        # value plot
        # must be last!
        dat_np = dat.to_numpy()
        if normalize:
            dat_np = prep.StandardScaler().fit_transform(dat_np)
        axs[0].set_title("ticker value")
        for i in range(len(dat_np[0])):
            axs[0].plot(dates,dat_np[:, i], label=dat.columns[i])
        # in matplotlib, fill figure with remained axes after delete of one of axes

        for a in axes:
            lax = len(axes)
            i = axes.index(a)
            axs[a].set_position(
                [
                    axs[a].get_position().x0,                   # x0
                    (lax - i - 1) * (1 / lax) + (0.3 / lax),    # y0
                    axs[a].get_position().width,                # width
                    0.5 / lax,                                  # height
                ]
            )
        fig.legend()
        plt.xticks(rotation=55)
        plt.show()

    def __date_resample__(self, df: pd.DataFrame, date_period: str) -> pd.DataFrame:
        """recalculate OLHCV data to new period
        assume input data is daily
        can translate to weekly and monthly
        """
        periods = {"daily": "D", "weekly": "W", "monthly": "ME"}
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

    def candle_pattern(self, date_period="daily", file = "", **kwargs) -> Self:
        """recognize canadle pattern and add column with prediction:
        - bullish are positive number (the higher value the more bullish)
        - bearisch is negative (the lower value the more bearish)
        args:
        - date_period: daily, weekly, monthly
        - file: json file with candle patterns
        - columns: columns to display
        using technical-analysis library
        https://github.com/trevormcguire/technical-analysis
        """
        if self.data.empty:
            return self
        
        req_cols = ["open", "high", "low", "val", "vol", "symbol", "date"]
        cp_cols = ["symbol", "date"] + list(self.cp_cols.values())
        
        if not all([c in self.data.columns for c in req_cols]):
            print("candle pattern requires open, high, low, close, volume")
            return self
        
        if self.cp_cols['CP'] in self.data.columns:
            self.data.drop(columns=[self.cp_cols['CP']], inplace=True)
        if self.cp_cols['CF'] in self.data.columns:
            self.data.drop(columns=[self.cp_cols['CF']], inplace=True)
        
        self.candle_pattern_kwargs["date_period"] =  date_period
        if file != "":
            self.candle_pattern_file = file
        # columns
        self.__arg_columns__(arg=kwargs.get("columns", ";".join(self.columns)))
        
        self.data["date"] = pd.to_datetime(self.data["date"])
        candle_pattern = read_json(file=self.candle_pattern_file)

        def calc_cp(grp):
            symbol = grp.iloc[0]["symbol"]
            grp = self.__date_resample__(grp, date_period)
            grp[self.cp_cols['CP']] = 0
            grp[self.cp_cols['CF']] = ""
            for cp, cv in candle_pattern.items():
                ta_func = getattr(candles, cp)
                cp_rows = ta_func(
                    open=grp["open"],
                    low=grp["low"],
                    high=grp["high"],
                    close=grp["val"],
                    **cv["kwargs"],
                )
                grp.loc[cp_rows, self.cp_cols['CP']] += cv["ind"]  # type: ignore
                grp.loc[cp_rows, self.cp_cols['CF']] = cp
            grp[self.cp_cols['CP']] = grp[self.cp_cols['CP']].cumsum()
            grp["symbol"] = symbol
            return grp

        self.data.sort_values(by=["symbol", "date"], inplace=True)
        cp_DF = (
            self.data
            .reindex(columns=req_cols)
            .drop_duplicates()
            .groupby("symbol", group_keys=False)
            .apply(calc_cp)
        )
        # both DFs must be sorted before merge_asof
        self.data.sort_values(by=["date"], inplace=True)
        cp_DF.sort_values(by=["date"], inplace=True)
        
        self.data = pd.merge_asof(
            left=self.data,
            right=cp_DF.reindex(columns=cp_cols),
            on="date",
            by="symbol",
        )
        self.data.sort_values(by=["symbol", "date"], inplace=True, ignore_index=True)
        # fill missing candle_pattern values with last available value
        self.data["candle_pattern"] = (
                        self.data
                        .groupby("symbol")[self.cp_cols['CP']]
                        .ffill())
        # may left NaN at begining  if date_period != 'daily'
        self.data[self.cp_cols['CP']] = self.data[self.cp_cols['CP']].fillna(0)
        self.data[self.cp_cols['CF']] = self.data[self.cp_cols['CF']].fillna("")
        return self

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
        dat.index = pd.to_datetime(dat.index)
        return dat

    def __pivot_longer__(self, dat: pd.DataFrame) -> pd.DataFrame:
        """move selected columns to bottom of DataFrame, so pivot_table works"""

        def process_column(df, col_name, suffix):
            new_col = df["symbol"] + suffix
            df["new_col"] = new_col
            df_cp = df.loc[:, ["date", col_name, "new_col"]]
            df_cp.rename(columns={col_name: "val", "new_col": "symbol"}, inplace=True)
            df.drop(columns=[col_name, "new_col"], inplace=True)
            return pd.concat([df, df_cp])

        for col_name, suffix in {"candle_pattern": "_cp", "vol": "_vol"}.items():
            if col_name in dat.columns:
                dat = process_column(dat, col_name, suffix)
        return dat.reset_index(drop=True)

    def __convert_currency__(self) -> None:
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

    def __set_dates__(self, kwargs: Dict) -> None:
        """set start and end date for collecting data
        if 'today' in dict keys, will set to today (considering working days)
        other way search 'start_date' and/or 'end_date' in dict and set accordingly
        if no dates in dict, set 'self.update_dates' to None so will give last available
        """
        if "start_date" in kwargs.keys():
            self.start_date = biz_date(kwargs["start_date"], format=self.date_format)
            self.date_change_print = False
        if "end_date" in kwargs.keys():
            self.end_date = biz_date(kwargs["end_date"], format=self.date_format)
            self.date_change_print = False
        if "today" in kwargs.keys():
            bz_today = biz_date(date.today())
            self.start_date = bz_today
            self.end_date = bz_today

        self.__date_change_info__(kwargs)
        self.end_date = max(self.end_date, self.start_date)
        return

    def __date_change_info__(self, kwargs: Dict) -> None:
        if self.tab == "GEO" or not self.date_change_print:
            return
        self.date_change_print = False
        if self.update_symbols:
            print("Date range changed to last working day when updating symbols.")
            return
        if not any(k in ["name", "symbol"] for k in kwargs.keys()):
            print("Date range changed to last locally available date.")
            print("Select particular symbol or name if you want different dates.")
            return
        if not self.update_dates:
            print("Date range changed to locally available dates.")
            print("set update_dates=True if you want different dates.")
            return
        if (
            self.update_dates
            and self.start_date == biz_date(date.today())
            and self.end_date == biz_date(date.today())
        ):
            print("Date range set to last working day.")
            print("Select start_date and/or end_date if you want different dates.")
            return

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

        # remove symbols if start date before start_quote
        if "start_quote" in dat.columns:
            dat = dat.loc[dat["start_quote"] <= start_date]
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
                        from_date=row.from_date,  # type: ignore
                        to_date=row.to_date,  # type: ignore
                        symbol=str(row.symbol),
                    )
                    if dat.empty:
                        print("no data on web")  # DEBUG
                        continue

                    if dat.iloc[0, 0] == "asset removed":
                        sql.rm_all(
                            tab=self.tab, symbol=str(row.symbol), db_file=self.db
                        )
                        print("symbol removed")  # DEBUG
                        continue

                    dat = self.__describe_table__(
                        dat=dat,
                        tab=self.tab,
                        description=row._asdict(),  # type: ignore
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
                        from_date=row.from_date, end_date=row.to_date, symbol=row.symbol  # type: ignore
                    )
                    cur_val = self.__describe_table__(
                        dat=cur_val,
                        tab="CURRENCY",
                        description=row._asdict(),  # type: ignore
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
        dat["val"] = dat["val"].fillna(0)
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
        def minmax(func: Callable, dat: pd.DataFrame) -> None:
            col = "from_date" if func.__name__ == "min" else "to_date"
            for h in dat["hash"].unique():
                date_sql = sql.getDF(
                    tab=f"{tab}_DESC",
                    get=[col],
                    search=[h],
                    where=["hash"],
                    db_file=self.db,
                )
                if date_sql.empty:
                    minmax_date = func(dat.loc[dat["hash"] == h, "date"])
                else:
                    minmax_date = func(
                        dat.loc[dat["hash"] == h, "date"].to_list()  # type: ignore
                        + [date_sql.iloc[0, 0]]
                    )
                dat.loc[dat["hash"] == h, col] = minmax_date
            return

        minmax(min, dat)
        minmax(max, dat)
        # if start_date is smaller then ticker trade start, set start_quote
        # this will prevent scraping of non existing data
        dat.loc[:, "start_quote"] = date(1800,1,1)
        trade_start = [
            d - timedelta(days=30) > self.start_date 
            for d in dat["from_date"]
        ]
        dat.loc[trade_start, "start_quote"] = dat.loc[trade_start, "from_date"]
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
