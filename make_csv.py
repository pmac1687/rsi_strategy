import psycopg2
import keys
import numpy as np
import csv

conn = psycopg2.connect(database="postgres", user=keys.user, password=keys.password, host=keys.host, port="5432")
cur = conn.cursor()
#cur.execute(f"select * from master_ticker_list limit 5;")
cur.execute(f"select * from historical_stock_data where ticker_id in (select id from master_ticker_list limit 2 as a) and date >= '2021-04-20' return a,*;")
data = cur.fetchall()
print(data)
#with open('stocks.csv', 'w', newline='') as file:
#  myreader = csv.writer(file, delimiter=',')
#  myreader.writerows(data)

conn.commit()
conn.close()

"""SELECT * 
FROM dbo.March2010 A
WHERE A.Date >= 2010-04-01;"""