import os
import sys
import re
from workers.common import read_json
from workers import web_stooq
from workers import sql_stooq
from datetime import date

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
            # make sure path exists
            # missing files are fine, will be created new db
            db = os.path.split(db)
            f = db[-1]
            p = db[0]
            if not os.path.exists(p):
                p = "./"
                print(f"path '{p}' dosen't exists. Using {os.path.abspath(p)}")
            self.db = os.path.join(p, f)

    def __read_sectors(self) -> None:
        try:
            for k in self.SECTORS:
                self.SECTORS[k]["data"] = read_json(self.SECTORS[k]["file"])
        except Exception as err:
            sys.exit(err)

    def __check_sector__(self, tab: str, sector: str) -> list:
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

        dat = sql_stooq.get_sql(
            db_file=self.db,
            tab=tab,
            sector=sector,
            symbol=symbol + "%",
            from_date=from_date,
            end_date=end_date,
        )
        if not dat:
            dat = web_stooq.web_stooq(
                sector_id=self.SECTORS[tab]["data"][sector][0],
                sector_grp=self.SECTORS[tab]["data"][sector][1],
                symbol=symbol + "%",
                from_date=from_date,
                end_date=end_date,
            )
            # extract countries
            # get info on components
            # get industry
            resp = sql_stooq.put_sql(dat=dat, tab=tab, db_file=self.db)
            if resp:
                print(dat)
        return

    def world_bank(self, what: str, country: str):
        # "GDP": "INTEGER"
        # "stooq_vol": "INTEGER"
        pass
