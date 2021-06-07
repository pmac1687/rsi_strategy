import psycopg2
import keys
import pandas as pd
import stockstats
from tapy import Indicators
import matplotlib.pyplot as plt
from csv import writer
import sqlalchemy



def query_db(que, cur, conn):
    credentials = f"postgresql://{keys.user}:{keys.password}@{keys.host}:5432/postgres"# read in your SQL query results using pandas
    df = pd.read_sql(que, con = credentials)
    conn.commit()
    return df

def add_indicators(df):
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
    return df

def check_trending(df):
    counter = 0
    for i in range(57,87):
        if df['ma'][i] > df['Close'][i]:
            counter -= 1
        else:
            counter += 1
    if counter > 0:
        return True
    else:
        return False

def check_indicators(df):
    hit_rsi = False
    hit_ao = False
    for n in range(77,87):
        if df['rsi_12'][n] < 40:
            hit_rsi = True
        if hit_rsi == True:
            if n != 86:
                if df['ao'][n-1] > df['ao'][n] < df['ao'][n+1]:
                    hit_ao = True
    if hit_ao == True:
        return True
    else:
        return False

def write_to_file(arr):
    with open('optimal.csv', 'a+', newline='') as write_obj:
        # Create a writer object from csv module
        csv_writer = writer(write_obj)
        # Add contents of list as last row in the csv file
        csv_writer.writerow(arr)

def main(l,que_ticks):
    conn = psycopg2.connect(database="postgres", user=keys.user, password=keys.password, host=keys.host, port="5432")
    cur = conn.cursor()
    #que_ticks = 'select * from master_ticker_list;'
    #print(cur)
    df_tickers = query_db(que_ticks, cur, conn)
    counter = 0
    for i in range(len(df_tickers)):
        tick = df_tickers['ticker'][i]
        #print(tick)
        _id = df_tickers['id'][i]
        que = f"select * from historical_stock_data as a where a.ticker_id in ({_id}) and a.date>='2021-01-01 00:00:00'"
        df = query_db(que, cur, conn)
        if len(df) < 87:
            continue
        df = add_indicators(df)
        trending = check_trending(df)
        if trending == True:
            ind_check = check_indicators(df)
            if ind_check == True:
                write_to_file(df_tickers.iloc[i])
                counter += 1
                print(counter)
    conn.close()
    print(counter)
    #que = build_data_query(all_tickers)
    #df = query_db(que)


if __name__=='__main__':
    main()