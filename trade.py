#!/home/mi/.backup/venv/ML/bin/python
# just starting file, for testing basically
from tradeDB import Trader


def main(**kwargs):
    akcje = Trader()
    akcje.get_stooq(**kwargs)


main(tab="STOCK")
main(tab="STOCK", sector="nyse")
main(tab="INDEXES", sector="asia")
