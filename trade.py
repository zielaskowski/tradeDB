#!/home/mi/.backup/venv/ML/bin/python
# just starting file, for testing basically
from tradeDB import Trader


def main(**kwargs):
    akcje = Trader()
    return akcje.get(**kwargs)

main()
print(main(tab="STOCK"))
print(main(tab="STOCK", component="s&p 500 "))
print(main(tab="STOCK", component="s&p 500"))
print(main(tab="INDEXES"))
print(main(tab="INDEXES", region="east asia"))
print(main(tab="STOCK", component="wig20"))
print(main(tab="STOCK", name="ALLEGRO"))
print(main(tab="STOCK", name="pkn"))
print(main(tab='STOCK',region='east asia',currency='pln'))