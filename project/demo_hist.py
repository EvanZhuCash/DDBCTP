import dolphindb as ddb

import time
import pandas as pd
from threading import Event, Thread
import queue
import threading
import os

# define factor queue to convert factor value
factor_queue = queue.Queue()
# define stop event to finish running
stop_event = Event()

# start dolphindb server
s = ddb.session()
s.connect("localhost", 8848, "admin", "123456")

# create streamTables
s.run("""
    share streamTable(1000:0, `timestamp`symbol`price`volume, [TIMESTAMP, SYMBOL, DOUBLE, INT]) as level1Stream
    share streamTable(10000:0, `timestamp`symbol`price, [TIMESTAMP,SYMBOL,DOUBLE]) as agg10s
    share streamTable(10000:0, `timestamp`symbol`price, [TIMESTAMP,SYMBOL,DOUBLE]) as agg1min
    share streamTable(10000:0, `timestamp`symbol`price, [TIMESTAMP,SYMBOL,DOUBLE]) as factorresult 
    """)
s.enableStreaming()

# create engine
s.run("""
    def factor_calc(x, y, z){
      ts = x
      sym = y[0]
      last10s = z
      last1min = select price from agg1min where symbol==sym order by timestamp desc
      if (isNull(last1min.price[0])){
        result = double(null)
      } else {
        result = last10s / last1min.price[0] - 1
      }
      return result
    }
    factor1 = <factor_calc(timestamp,symbol,price)>
    
    go
    
    engine10s = createTimeSeriesEngine(name="engine10s", 
    windowSize=10000, step=10000, metrics=<[last(price)]>,
    dummyTable=level1Stream, outputTable=agg10s, timeColumn="timestamp",
    useSystemTime=false, keyColumn="symbol", useWindowStartTime=false)
    
    engine1min = createTimeSeriesEngine(name="engine1min", windowSize=60000, 
    step=60000, metrics=<[last(price)]>,dummyTable=level1Stream, outputTable=agg1min,
    timeColumn="timestamp",useSystemTime=false,keyColumn="symbol",useWindowStartTime=false)
    
    factor_calc = createReactiveStateEngine(name="factor", metrics=[<symbol>,factor1],
    dummyTable=agg10s,outputTable=factorresult,keyColumn="timestamp")
    """)

# subscribe Table
s.run("""
    subscribeTable(tableName=`level1Stream, actionName="engine10s", handler=tableInsert{engine10s}, msgAsTable=true, offset=-1)
    subscribeTable(tableName=`level1Stream, actionName="engine1min", handler=tableInsert{engine1min}, msgAsTable=true, offset=-1)
    """)

s.run("""
    subscribeTable(tableName=`agg10s, actionName="factor", handler=tableInsert{factor_calc}, msgAsTable=true, offset=-1)
    """)

time.sleep(5)

num = 0

def handler(lsts):
  global num
  num += 1
  factor_queue.put(lsts)
    

s.subscribe("localhost", 8848, handler, "factorresult", offset=-1, throttle=0.1)

  
def run():
  history_file_path = os.getenv("HISTORY_FILE_PATH", "history.csv")
  df = pd.read_csv(history_file_path)
  for i in range(df.shape[0]):
    symbol_ = df.loc[i, "symbol"]
    time_ = df.loc[i, "tradetime"]
    price_ = df.loc[i, "tradeprice"]
    volume_ = df.loc[i, "tradevolume"]
    if not stop_event.is_set():
      s.run(f"insert into level1Stream values({time_}, `{symbol_}, {price_}, {volume_})")
      time.sleep(0.01)
    else:
      break
  print("thread_thick is out")
          
def strategy():
  while not stop_event.is_set():
    try:
      factor = factor_queue.get(timeout=1)
      sym_ = factor[1]
      factor_ = factor[2]
      print("factor value: ", factor_)
      # send order according to factor value
      if factor_:
        if factor_ > float(0.00):
          print(f"send long order")
        elif factor_ < float(0.00):
          print(f"send short order")
    except queue.Empty:
      continue
  print("thread_strategy is out")
    
if __name__ == "__main__":
  thread_tick = Thread(name="tick", target=run)
  thread_strategy = Thread(name="strategy", target=strategy)
  
  thread_tick.start()
  thread_strategy.start()
  
  try:
    while True:
      time.sleep(1)
  except KeyboardInterrupt:
    print("stop running!!!")
    stop_event.set()

  for thread in threading.enumerate():
    print(thread.name)
  thread_tick.join()
  thread_strategy.join()
  
  print("all threads stopped")
  time.sleep(5)
  s.close()
  print("ddb closed")
  
  