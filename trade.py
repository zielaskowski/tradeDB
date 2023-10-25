#!/home/mi/.backup/venv/ML/bin/python
# just starting file, for testing basically
from tradeDB import Trader


trader = Trader(update_symbols=False)

trader.get()
trader.get(tab='?')
#trader.get(tab="stock",update_symbols=True,region='EUROPE & CENTRAL ASIA ')
trader.get(tab="stock",region='EUROPE & CENTRAL ASIA ')
trader.get(tab='stock',symbol='ale;pkn', currency='USD')
trader.get(tab='stock',symbol='ale;pkn', start_date='01-01-2023',end_date='01-05-2023', currency='USD')
trader.get(tab='stock',symbol='ale;pkn')
trader.get(tab='stock',symbol='ale;pkn', start_date='01-01-2023',end_date='01-05-2023')
trader.get(tab='stock',symbol='ale', columns='indexes')
trader.get(tab='stock',symbol='ale',columns='name,val,date')
trader.get(tab='stock',symbol='ale',columns='name;val;date')
trader.get(tab='stock',symbol='ale',columns='name;val;date;-indexes')
trader.get(tab='stock',symbol='ale',columns='-indexes')
trader.get(tab='stock',symbol='ale',columns='%;-indexes')
trader.get(tab='stock',symbol='ale',columns='%')
trader.get(tab='stock',symbol='ale',columns='%;-name')
trader.get(tab='STOCK',country='PL', region='europe', componentsss='')
trader.get(tab='stocks')
trader.get(tab="stock",update_symbols=True,region='africa')
trader.get(tab="stock",update_symbols=True,region='sub-saharan ')
trader.get(tab="stock",update_symbols=True,region='')
trader.get(region='?')

trader.get(tab="stock",region='EUROPE & CENTRAL ASIA ')
trader.get(tab="stock",country='pol')
trader.get(tab="stock",country='DE')
trader.get(tab="GEO", columns="country")
trader.get(tab="GEO", columns="country;  iso")
trader.get(tab="GEO", columns="country;  iso2")
trader.get(tab="GEO", name="ALLEGRO")
trader.get(tab="GEO", region='EUROPE & CENTRAL ASIA ')
trader.get(tab="GEO", country="POLAND")
trader.get(tab="GEO", components='WIG20 ')
trader.get(tab="GEO", symbol='PL ')

trader.get(tab="stock",region='EUROPE & CENTRAL ASIA ', start_date='01-01-2023')
trader.get(tab="stock",country='GERMANY')
trader.get(tab="GEO")
trader.get(tab="stock",region='?')
trader.get(tab="stock",start_date='01-01-2023')
trader.get(tab="STOCK", columns="symbol")
trader.get(tab="STOCK", columns="country")
trader.get(tab="STOCK", component="wig20 ")
trader.get(tab="STOCK", components="wig20 ")
trader.get(tab="STOCK", components="s&p 500")
trader.get(tab="INDEXES", components="wig20")
trader.get(tab="INDEXES")
#trader.get(tab="stock",update_symbols=True,region='east asia')
trader.get(tab="INDEXES", region="east asia")
trader.get(tab="STOCK", name="ALLEGRO")
trader.get(tab="STOCK", symbol="ALE")
trader.get(tab="STOCK", name="ALLEGRO", start_date="21-10-2023")
trader.get(tab="STOCK", name="ALLEGRO", start_date="01-01-2023")
trader.get(tab="STOCK", name="pkn")
trader.get(tab='STOCK', region='east asia', currency='pln')


#currency can be selected by symbol or name, make sure self.currency is symbol
# make sure currency is not eur

# date holes are possible when updating symbols!!!
# check requested dates also between max and min date!
# just do not allow holes, always set min date to to_date and max date to from_date



# add args:
#- country indicators: GDP, jobs, inflation,...
#- 

