#!/home/mi/.backup/venv/ML/bin/python
# just starting file, for testing basically
from tradeDB import Trader


trader = Trader()

print(trader.get())
print(trader.get(tab="GEO"))
print(trader.get(tab="GEO", columns="country"))
print(trader.get(tab="GEO", columns="country;  iso"))
print(trader.get(tab="GEO", columns="country;  iso2"))
print(trader.get(tab="stock",update_symbols=True,region='?'))
print(trader.get(tab="stock",update_symbols=True,region='africa'))
print(trader.get(tab="stock",update_symbols=True,region='sub-saharan '))
#print(trader.get(tab="stock",update_symbols=True,region='EUROPE & CENTRAL ASIA '))
print(trader.get(tab="stock",start='01-01-2023'))
print(trader.get(tab="STOCK", columns="symbol"))
print(trader.get(tab="STOCK", columns="country"))
print(trader.get(tab="STOCK", component="wig20 "))
print(trader.get(tab="STOCK", component="s&p 500"))
print(trader.get(tab="INDEXES"))
print(trader.get(tab="INDEXES", region="east asia"))
print(trader.get(tab="STOCK", name="ALLEGRO"))
print(trader.get(tab="STOCK", name="ALLEGRO", start="01-01-2023"))
print(trader.get(tab="STOCK", name="pkn"))
print(trader.get(tab='STOCK', region='east asia', currency='pln'))

# missing currency fo ~20countries

# remove cache option

# /home/mi/Dropbox/prog/python/tradeDB/workers/sql.py:264: 
# FutureWarning: The behavior of DataFrame concatenation with empty or 
# all-NA entries is deprecated. In a future version, this will no longer
#  exclude empty or all-NA columns when determining the result dtypes. 
# To retain the old behavior, exclude the relevant entries before the concat operation.
#   resp_col = pd.concat([resp_col, resp_part[cmd]], ignore_index=True)