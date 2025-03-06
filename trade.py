#!/home/mi/docs/prog/python/tradeDB/.venv/bin/python
# example file, for testing basically
from tradeDB import Trader


tr = Trader()
tr.get(tab="stock", name="MTR CORPORATION", start_date="1-1-2020")
print(tr)
tr.plot()
tr.plot(xticks=10)
tr.candle_pattern("monthly")
tr.plot()


pkn = Trader()
pkn.get(tab="stock", symbol="ale", columns="-indexes;-industry;-info", start_date="1-1-2020",update_dates=False)
pkn.plot()

# will take last available from db (not today)
ale = Trader()
ale.get(tab="stock", name="CITIC", columns="-indexes;-industry;-info", update_dates=False)
print(ale)

# nothing available locally
ale = Trader()
ale.get(tab="stock", name="CITIC", columns="-indexes;-industry;-info", start_date = "1-1-1910",end_date = "1-1-1920",update_dates=False)
print(ale)



pkn=Trader()
ale=Trader()
pkn.get(tab='stock',symbol='pkn', start_date='1-1-2020')
ale.get(tab='stock',symbol='ale')
pkn+=ale
pkn.candle_pattern('monthly')
pkn.pivot()
pkn.plot()

spx = Trader()
spx.get(tab='indexes', symbol='^spx', start_date='1-01-2015')
spx.get(tab='indexes', symbol='^spx', start_date='1-01-2015')
ale=Trader()
ale.get(tab='STOCK', name='ALLEGRO')
spx+=ale
print(spx)
ale+=spx
print(ale)


trade=Trader()
trade.get(tab='STOCK',components='wig20')
ind = trade.to_str('symbol')
trade.get(tab='STOCK',symbol=ind, start_date='1-01-2010')
trade.get(tab='STOCK',symbol=ind, start_date='1-01-2010',currency='USD')
trade+=spx
print(trade)

ale = Trader()
pkn=Trader()
ale.get(tab="STOCK", name="ALLEGRO", start_date="01-05-2023", currency="USD")
pkn.get(tab="STOCK", symbol="PKN")
print(pkn)
ale+=pkn
print(ale)

ale = Trader()
pkn=Trader()
ale.get(tab="STOCK", name="ALLEGRO", start_date="01-05-2023", currency="USD")
pkn.get(tab="STOCK", country='pl')
ale+=pkn
print(ale)


trader = Trader(update_symbols=False)
trader2 = Trader(update_symbols=False)
#print(trader.get(tab="stock",update_symbols=True,region='EUROPE & CENTRAL ASIA '))
#print(trader.get(tab="stock",update_symbols=True,region='NORTH'))
trader.get()
trader.get(tab='?')
print(trader.get(tab='stock',symbol='ale',columns='name;val;vol;date', start_date='20-10-2023')+trader.get(symbol='pkn'))
ale=trader.get(tab='stock',symbol='ale',columns='name;val;vol;date', start_date='20-10-2023')
pkn=trader2.get(tab='stock',symbol='pkn')
print(ale+pkn)
print(ale.pivot())

print(trader.get(tab='indexes', symbol='^spx'))

print(trader.get(tab='stock',symbol='pkn', start_date='07-01-2023'))
print(trader.get(tab='stock',symbol='pkn', start_date='07-01-2013', currency='USD'))
print(trader.get(tab="stock",region='EUROPE & CENTRAL ASIA '))
print(trader.get(tab='stock',symbol='ale;pkn', currency='USD'))
print(trader.get(tab='stock',symbol='ale;pkn', start_date='01-01-2023',end_date='01-05-2023', currency='USD'))
print(trader.get(tab='stock',symbol='ale;pkn', currency='eur'))
print(trader.get(tab='stock',symbol='ale;pkn', start_date='01-01-2023',end_date='01-05-2023', currency='%'))
print(trader.get(tab='stock',symbol='ale', columns='indexes'))
print(trader.get(tab='stock',symbol='ale',columns='name,val,date'))
print(trader.get(tab='stock',symbol='ale',columns='name;val;date'))
print(trader.get(tab='stock',symbol='ale',columns='name;val;date;-indexes'))
print(trader.get(tab='stock',symbol='ale',columns='-indexes'))
print(trader.get(tab='stock',symbol='ale',columns='%;-indexes'))
print(trader.get(tab='stock',symbol='ale',columns='%'))
print(trader.get(tab='stock',symbol='ale',columns='%;-name'))
print(trader.get(tab='STOCK',country='PL', region='europe', componentsss=''))
print(trader.get(tab='stocks'))
print(trader.get(tab="stock",update_symbols=True,region='africa'))
print(trader.get(tab="stock",update_symbols=True,region='sub-saharan '))
print(trader.get(tab="stock",update_symbols=True,region=''))
print(trader.get(region='?'))

print(trader.get(tab="stock",region='EUROPE & CENTRAL ASIA '))
print(trader.get(tab="stock",country='pol'))
print(trader.get(tab="stock",country='DE'))
print(trader.get(tab="GEO", columns="country"))
print(trader.get(tab="GEO", columns="country;  iso"))
print(trader.get(tab="GEO", columns="country;  iso2"))
print(trader.get(tab="GEO", name="ALLEGRO"))
print(trader.get(tab="GEO", region='EUROPE & CENTRAL ASIA '))
print(trader.get(tab="GEO", country="POLAND"))
print(trader.get(tab="GEO", components='WIG20 '))
print(trader.get(tab="GEO", symbol='PL '))

print(trader.get(tab="stock",region='EUROPE & CENTRAL ASIA ', start_date='01-01-2023'))
print(trader.get(tab="stock",country='GERMANY'))
print(trader.get(tab="GEO"))
print(trader.get(tab="stock",region='?'))
print(trader.get(tab="stock",start_date='01-01-2023'))
print(trader.get(tab="STOCK", columns="symbol"))
print(trader.get(tab="STOCK", columns="country"))
print(trader.get(tab="STOCK", component="wig20 "))
print(trader.get(tab="STOCK", components="wig20 "))
print(trader.get(tab="STOCK", components="s&p 500"))
print(trader.get(tab="STOCK", components="BET"))
print(trader.get(tab="INDEXES", components="wig20"))
print(trader.get(tab="INDEXES"))
#print(trader.get(tab="stock",update_symbols=True,region='east asia'))
print(trader.get(tab="INDEXES", region="east asia"))
print(trader.get(tab="STOCK", name="ALLEGRO"))
print(trader.get(tab="STOCK", symbol="ALE"))
print(trader.get(tab="STOCK", name="ALLEGRO", start_date="21-10-2023"))
print(trader.get(tab="STOCK", name="ALLEGRO", start_date="01-01-2023"))
print(trader.get(tab="STOCK", name="pkn"))
print(trader.get(tab='STOCK', region='east asia', currency='pln'))



# prophet liblary
# stocksight library for twitter sentiment

# add TA-lib
# ta: indicator name as str + other kwargs
# group by symbol self.data, for each group take OHLCV and call talib
# add column to self.data with symbol and TA as suffix
# store TA in class, so can be restored after change dates i.e.
####
# pivot table including TA....
# if no other way, pivot_longer moving indicator to symbol and name and values to val


# date holes are possible when updating symbols!!!
# check requested dates also between max and min date!
# just do not allow holes, always set min date to to_date and max date to from_date



# add args:
#- country indicators: GDP, jobs, inflation,...
#- 

