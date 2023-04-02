#!/home/mi/.backup/venv/ML/bin/python
# just starting file, for testing basically
from tradeDB import Trader


trader = Trader()

print(trader.get(tab="STOCK"))
print(trader.get(tab="STOCK", component="s&p 500 ", region='east asia'))
print(trader.get(tab="STOCK", component="s&p 500"))
print(trader.get(tab="INDEXES"))
print(trader.get(tab="INDEXES", region="east asia"))
print(trader.get(tab="STOCK", component="wig20"))
print(trader.get(tab="STOCK", name="ALLEGRO"))
print(trader.get(tab="STOCK", name="pkn"))
print(trader.get(tab='STOCK', region='east asia', currency='pln'))
