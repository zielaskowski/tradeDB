#!/home/mi/.backup/venv/ML/bin/python
# just starting file, for testing basically
from tradeDB import Trader


def main(**kwargs):
    akcje = Trader()
    akcje.get(**kwargs)


main(tab="STOCK")
main(tab="STOCK", component="nyse")
main(tab="INDEXES")
main(tab="INDEXES", region="east asia")
