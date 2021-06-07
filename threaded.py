import multiprocessing 
import numpy as np
import tqdm
from itertools import repeat
from multiprocessing import Process, Manager
from multiprocessing import Pool
import pandas as pd
import keys
import psycopg2
import optimized


num_processes = multiprocessing.cpu_count()

conn = psycopg2.connect(database="postgres", user=keys.user, password=keys.password, host=keys.host, port="5432")
cur = conn.cursor()
cur.execute(f"select * from master_ticker_list;")
data = cur.fetchall()
conn.commit()
conn.close()

print(len(data))
master = []
arr = []
for i in range(len(data)):
    arr.append(data[i])
    if len(arr) == 845:
        master.append(arr)
        arr = []
    if data[i] == data[-1]:
        master.append(arr)

processes = []
for i in range(1,10):
    p = multiprocessing.Process(target=optimized.main, args=(i,master))
    processes.append(p)
    p.start()
    
for process in processes:
    process.join()

if __name__ == '__main__':
    pass