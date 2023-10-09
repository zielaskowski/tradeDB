#!/home/mi/.backup/venv/ML/bin/python
# just starting file, for testing basically
from tradeDB import Trader


trader = Trader(update_symbols=False)

print(trader.get())
print(trader.get(tab='?'))
#print(trader.get(tab="stock",update_symbols=True,region='EUROPE & CENTRAL ASIA '))
print(trader.get(tab="stock",region='EUROPE & CENTRAL ASIA ', start='01-01-2023'))
print(trader.get(tab="GEO"))
print(trader.get(tab="GEO", columns="country"))
print(trader.get(tab="GEO", columns="country;  iso"))
print(trader.get(tab="GEO", columns="country;  iso2"))
print(trader.get(tab="stock",region='?'))
print(trader.get(tab="stock",update_symbols=True,region='africa'))
print(trader.get(tab="stock",update_symbols=True,region='sub-saharan '))
print(trader.get(tab="stock",update_symbols=True,region=''))
print(trader.get(tab="stock",start='01-01-2023'))
print(trader.get(tab="STOCK", columns="symbol"))
print(trader.get(tab="STOCK", columns="country"))
print(trader.get(tab="STOCK", component="wig20 "))
print(trader.get(tab="STOCK", component="s&p 500"))
print(trader.get(tab="INDEXES", component="wig20"))
print(trader.get(tab="INDEXES"))
print(trader.get(tab="INDEXES", region="east asia"))
print(trader.get(tab="STOCK", name="ALLEGRO"))
print(trader.get(tab="STOCK", name="ALLEGRO", start="01-01-2023"))
print(trader.get(tab="STOCK", name="pkn"))
print(trader.get(tab='STOCK', region='east asia', currency='pln'))
print(trader.get(tab="stock",update_symbols=True,region='east asia'))

# do not spliting long comands corectly: when 3rd part
# (after parenthesis) is missing
# i.e. when quering for index on long list of stocks in stock_index()

#1. query including index for stock 
# probably will couse duplicates in COMPONETS: do something with that
#2. check curencies

