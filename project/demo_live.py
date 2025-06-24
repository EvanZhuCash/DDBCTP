import dolphindb as ddb

from datetime import datetime
import time
from threading import Event, Thread
import queue
import json
import os

from vnpy_scripttrader import ScriptEngine
from vnpy_scripttrader import init_cli_trading
from vnpy_ctp import CtpGateway

# define factor queue to convert factor value
factor_queue = queue.Queue()
# define stop event to finish running
stop_event = Event()

symbols_file_path = os.getenv("SYMBOL_FILE_PATH", "symbol.json")
account_file_path = os.getenv("ACCOUNT_FILE_PATH", "account.json")

# symbol list as global buffer
with open(symbols_file_path, 'r') as file1:
  data_symbol = json.load(file1)
vt_symbols = list(data_symbol.values())
# convert symbol name into vnpy format
symbol_convert: dict = {}
for i in vt_symbols:
  symbol_convert[i.split(".")[0]] = i

# start vnpy engine
engine = init_cli_trading([CtpGateway])
with open(account_file_path, 'r') as file2:
  ctp_setting = json.load(file2)
engine.connect_gateway(ctp_setting, "CTP")

while True:
  engine.subscribe(vt_symbols)
  for vt_symbol in vt_symbols:
    contract = engine.get_contract(vt_symbol)
  if contract:
    engine.write_log(f"contract info, {contract}")
    break

time.sleep(20)

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

# define global variable num to count factor result received from ddb 
num = 0

def handler(lsts):
  global num
  num += 1
  factor_queue.put(lsts)
  
s.subscribe("localhost", 8848, handler, "factorresult", offset=-1, throttle=0.1)

def run(engine: ScriptEngine):
  while not stop_event.is_set():
    for vt_symbol in vt_symbols:
      tick = engine.get_tick(vt_symbol)
    if tick:
      # convert the tick data into specified format and invert it into stream table level1stream
      str_time = datetime.strftime(tick.datetime, "%Y.%m.%d %H:%M:%S.%f")
      str_time_1, str_time_2 = str_time.split(" ")
      time_ = str_time_1 + "T" + str_time_2[:-3]
      symbol_ = tick.name
      price_ = tick.last_price
      volume_ = int(tick.volume)
      s.run(f"insert into level1Stream values({time_}, `{symbol_}, {price_}, {volume_})")
      time.sleep(0.5)
    else:
      print("尚未连接")
      time.sleep(10)
  print("thread_thick is out")
          
def strategy():
  while not stop_event.is_set():
    try:
      factor = factor_queue.get(timeout=1)
      sym_ = factor[1]
      factor_ = factor[2]
      # send order according to factor value
      if factor_:
        if factor_ > float(0.00):
          engine.write_log(f"send long order")
          symbol = symbol_convert.get(sym_)
          askprice = engine.get_tick(symbol).ask_price_1
          engine.buy(symbol, askprice, 1)
        elif factor_ < float(0.00):
          engine.write_log(f"send short order")
          symbol = symbol_convert.get(sym_)
          bidprice = engine.get_tick(symbol).bid_price_1
          engine.sell(symbol, bidprice, 1)
    except queue.Empty:
      continue
  print("thread_strategy is out")
    
    
if __name__ == "__main__":
  thread_tick = Thread(target=run, args=(engine,))
  thread_strategy = Thread(target=strategy)
  
  thread_tick.start()
  thread_strategy.start()
  
  try:
    while True:
      time.sleep(1)
  except KeyboardInterrupt:
    print("stop running!!!")
    stop_event.set()
    engine.event_engine.stop()
  
  thread_tick.join()
  thread_strategy.join()
  
  print("all threads stopped")
  time.sleep(5)
  s.close()
  print("ddb closed")
    
  