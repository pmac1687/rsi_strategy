import psycopg2
import keys
import pandas as pd
import stockstats
from tapy import Indicators
import matplotlib.pyplot as plt


conn = psycopg2.connect(database="postgres", user=keys.user, password=keys.password, host=keys.host, port="5432")
cur = conn.cursor()
#cur.execute(f"select * from master_ticker_list limit 5;")
cur.execute(f"select * from historical_stock_data where ticker_id in (select id from master_ticker_list) and date >= '2021-01-20';")
data = cur.fetchall()
conn.commit()
conn.close()

#put in group arrays by ticker
grouped_data = []
arr = []
for item in data:
    if len(arr) == 0:
        arr.append(item)
    if arr[0][0] == item[0]:
        arr.append(item)
    if arr[0][0] != item[0]:
        grouped_data.append(arr)
        arr = []
        arr.append(item)
    if item == data[-1]:
        arr.append(item)
        grouped_data.append(arr)    

#make dfs from each group, run the rsi to see if optimal
#ticker_id, open, close, high, low, volume, date
tickers = []
aos = []
for tick in grouped_data:
    #one stock array filled with many arrays
    name = []
    _open = []
    close = []
    high = []
    low = []
    volume = []
    index = []
    for b in tick:
        name.append(b[0])
        _open.append(b[1])
        close.append(b[2])
        high.append(b[3])
        low.append(b[4])
        volume.append(b[5])
        index.append(b[6])
    data = {
        'ticker_id': name,
        'open': _open,
        'close': close,
        'high':high,
        'low': low,
        'volume': volume,
        'date': index,
    }
    cols = ['ticker_id', 'open', 'close', 'high', 'low', 'volume', 'date']
    
    df = pd.DataFrame (data, columns = cols)
    df.index = index
    stock = stockstats.StockDataFrame.retype(df)
    stock['rsi_12']
    stock.rename(columns={
        "high": "High",
        'low':'Low',
        'close': 'Close'
    }, inplace=True)

    stock = Indicators(stock)

    stock.awesome_oscillator(column_name='ao')
    
    stock = stock.df
    for c in stock['rsi_12'][-11:-1]:
        if c < 40 and c > 0:
            if stock['ticker_id'][0] not in tickers:
                tickers.append(stock['ticker_id'][0])
    #print(stock['ao'][-10:-1])      
    ao = stock['ao'][-11:-1]
    for n in range(len(ao)):
        if ao[n] < 0 and ao[n] != ao[-1] and n != 0:
            if ao[n] < ao[n-1] and ao[n] < ao[n+1]:
                if stock['ticker_id'][0] not in aos:
                    aos.append(stock['ticker_id'][0])

res = []
for m in aos:
    if m in tickers:
        res.append(m)




# check for trending ma
conn = psycopg2.connect(database="postgres", user=keys.user, password=keys.password, host=keys.host, port="5432")
cur = conn.cursor()
#cur.execute(f"select * from master_ticker_list limit 5;")
arg = ""
for d in range(len(res)):
    if res[d] == res[-1]:
        arg += str(res[d])
    else:
        arg += str(res[d]) + ','
cur.execute(f"select * from historical_stock_data where ticker_id in ({arg}) and date >= '2021-01-01';")
data1 = cur.fetchall()
conn.commit()
conn.close()





#ow have data, group it back
res_arr = []
for w in res:
    arr = []
    for l in data1:
        if w in l:
            arr.append(l)
        if l == data1[-1]:
            res_arr.append(arr)
            
success = []
for tick in res_arr:
    name = []
    _open = []
    close = []
    high = []
    low = []
    volume = []
    index = []
    for b in tick:
        name.append(b[0])
        _open.append(b[1])
        close.append(b[2])
        high.append(b[3])
        low.append(b[4])
        volume.append(b[5])
        index.append(b[6])
    data = {
        'ticker_id': name,
        'open': _open,
        'close': close,
        'high':high,
        'low': low,
        'volume': volume,
        'date': index,
    }
    cols = ['ticker_id', 'open', 'close', 'high', 'low', 'volume', 'date']
    
    df = pd.DataFrame (data, columns = cols)
    df.index = index
    ma = []
    for g in range(len(df['close'])):
        if g > 40:
            avg = sum(df['close'][g-40:g]) / 40
            ma.append(avg)
        else:
            ma.append(0)
    df['ma'] = ma
    """
    c= []
    m=[]
    for g in range(len(df['close'])):
        c.append(int(df['close'][g]))
        m.append(int(df['ma'][g]))
    df['close'] = c
    df['ma'] = m
    
    ax = plt.gca()

    df.plot(kind='line',x='date',y='close',ax=ax)
    df.plot(kind='line',x='date',y='ma', color='red', ax=ax)

    plt.show()
    """
    counter = 0
    print(df['ma'][-20:-1])
    for f in range(-20,-1):
        if df['close'][f] > df['ma'][f]:
            counter += 1
            print('pos', df['close'][f], df['ma'][f], f)
        else:
            counter -= 1
            print('neg')
    
    if counter > 0:
        print(counter, df['ticker_id'][0])
        success.append(df['ticker_id'][0])
    
print(success)



conn = psycopg2.connect(database="postgres", user=keys.user, password=keys.password, host=keys.host, port="5432")
cur = conn.cursor()
arg = ""
for d in range(len(success)):
    if success[d] == success[-1]:
        arg += str(success[d])
    else:
        arg += str(success[d])  + ','
print(arg)
cur.execute(f"select * from master_ticker_list where id in ({arg});")
data1 = cur.fetchall()
print(data1)
conn.commit()
conn.close()