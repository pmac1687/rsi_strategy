import psycopg2
import keys
import pandas as pd
import stockstats
from tapy import Indicators
import matplotlib.pyplot as plt
import csv
import gc
import sqlalchemy

def query_db(que):
    conn = psycopg2.connect(database="postgres", user=keys.user, password=keys.password, host=keys.host, port="5432")
    cur = conn.cursor()
    credentials = f"postgresql://{keys.user}:{keys.password}@{keys.host}:5432/postgres"# read in your SQL query results using pandas
    df = pd.read_sql(que, con = credentials)
    print(df)
    conn.commit()
    conn.close()
    return df

def build_data_query(ticks):
    ticker_list_len = 100
    ticker_ids = ''
    tickers = []
    for i in range(ticker_list_len):
        if i == 99:
            ticker_ids += f"{ticks['id'][i]}"
        else:
            ticker_ids += f"{ticks['id'][i]},"
    print(ticker_ids)
    que = f"select * from historical_stock_data where ticker_id in ({ticker_ids}) and date >= '2021-01-01';"
    return que



def check_optimal_stock(df, ticks):
    tick_arr = df['ticker_id']
    print(tick_arr)
    for i in range(len(ticks)):
        tick = ticks['id'][i]
        ind_arr = []
        for b in range(len(df)):
            if df['ticker_id'][b] == tick:
                ind_arr.append(b)
        arr2 = ind_arr[-10:]
        check_ma(arr2,df)
        hit_rsi = False
        optimal = False
        for c in arr2:

        break


        #print(tick)
        #arr = df.loc[df['ticker_id'] == tick]
        ##arr2 = df.loc[df['ticker_id'] == '548222485']
        #for b in arr:
        #    if b.index == "2021-05-07 00:00:00":
        #        print(b,'duh')
        #break
    

def main():
    que_ticks = 'select * from master_ticker_list;'
    all_tickers = query_db(que_ticks)
    que = build_data_query(all_tickers)
    df = query_db(que)
    df.index = df['date']
    df = stockstats.StockDataFrame.retype(df)
    df['rsi_12']
    df.rename(columns={
        "high": "High",
        'low':'Low',
        'close': 'Close'
    }, inplace=True)
    df = Indicators(df)
    
    df.sma(period=40, column_name='ma', apply_to='Close')

    df.awesome_oscillator(column_name='ao')
    
    df = df.df
    check_optimal_stock(df, all_tickers)



if __name__ == '__main__':
    main()