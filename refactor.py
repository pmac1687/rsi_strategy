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
    #cur.execute(f"select * from master_ticker_list limit 5;")
    #cur.execute(que)
    #SQL_Query = pd.read_sql_query(que, conn)
    #cols = ['ticker_id', 'open', 'close', 'high', 'low', 'volume', 'date']
    #df = pd.DataFrame(SQL_Query, columns=cols)
    #print(df)
    #data = conn.read_frame(que, cur)
    #print(data)
    #data = cur.fetchall()
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
            ticker_ids += f"{ticks['id'][0]}"
        else:
            ticker_ids += f"{ticks['id'][0]},"
    print(len(tickers), len(ticker_ids))
    que = f"select * from historical_stock_data where ticker_id in ({ticker_ids}) and date >= '2021-01-01';"
    return que



def check_optimal_stock(df, ticks):
    for i in range(len(ticks)):
        tick = ticks['id'][i]
        #print(tick)
        arr = df.loc[df['ticker_id'] > 4]
        print(df.loc[df['ticker_id']])
    

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