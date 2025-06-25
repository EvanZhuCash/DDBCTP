import dolphindb as ddb
from time import sleep

class ddb_server():
  def __init__(self):
    self.session = ddb.session()
    self.session.connect("localhost", 8848, "admin", "123456")
    
  def create_streamTable(self):
    self.session.run("""
      share streamTable(1000:0, `symbol`timestamp`price`qty`dir`offset, [SYMBOL, TIMESTAMP, DOUBLE, INT, SYMBOL, SYMBOL]) as trades
      share streamTable(1000:0, `symbol`timestamp`price`vol, [SYMBOL, TIMESTAMP, DOUBLE, INT]) as tickStream
      share streamTable(1:0, `symbol`timestamp`price`pnl`dir, [SYMBOL, TIMESTAMP, DOUBLE, DOUBLE, SYMBOL]) as unrealized_positions
      share streamTable(1:0, `symbol`timestamp_close`price_open`price_close`qty`pnl, [SYMBOL, TIMESTAMP, DOUBLE, DOUBLE, INT, DOUBLE]) as realized_positions
      share streamTable(1000:0, `orderID`symbol`timestamp`orderPrice`orderVolume`direction, [INT, SYMBOL, TIMESTAMP, DOUBLE, INT, SYMBOL]) as orderStream
      share streamTable(1000:0, `symbol`timestamp`close`upper`mean`lower, [SYMBOL, TIMESTAMP, DOUBLE, DOUBLE, DOUBLE, DOUBLE]) as bollST
      share streamTable(1000:0, `symbol`timestamp`close`signal, [SYMBOL, TIMESTAMP, DOUBLE, STRING]) as signalST
      share streamTable(1000:0, `timestamp`symbol`price, [TIMESTAMP, SYMBOL, DOUBLE]) as agg1min
      go
      """)
  
  def create_table(self):
    self.session.run("""
      share keyedTable(`symbol, 1:0, `symbol`qty`price`dir, [SYMBOL, INT, DOUBLE, SYMBOL]) as positionTable
      share table(1000:0, `sym1`timestamp1`price1`qty1`dir1, [SYMBOL, TIMESTAMP, DOUBLE, INT, SYMBOL]) as pos_table
    """)

  def bband_calc(self):
    self.session.run("""
      engine1min = createTimeSeriesEngine(name="engine1min", windowSize=60000, step=60000, metrics=<[last(price)]>,\
      dummyTable=tickStream, outputTable=agg1min, timeColumn="timestamp",useSystemTime=false,keyColumn="symbol",\
      useWindowStartTime=false)
      use ta
      reactiveStateMetrics = <[
        timestamp,
        price,
        ta::bBands(price, timePeriod=5, nbdevUp=1, nbdevDn=1, maType=0)[0] as `upper,
        ta::bBands(price, timePeriod=5, nbdevUp=1, nbdevDn=1, maType=0)[1] as `mean,
        ta::bBands(price, timePeriod=5, nbdevUp=1, nbdevDn=1, maType=0)[2] as `lower
      ]>
      createReactiveStateEngine(name="taReactiveStateEngine",\
      metrics=reactiveStateMetrics, dummyTable=agg1min, outputTable=bollST, keyColumn="symbol", keepOrder=true)
      go
      subscribeTable(tableName=`tickStream, actionName="engine1min", handler=tableInsert{engine1min}, msgAsTable=true, offset=-1)
      subscribeTable(tableName=`agg1min, actionName="taReactiveStateEngine", handler=getStreamEngine("taReactiveStateEngine"), msgAsTable=true, reconnect=true)
    """)

  def strategy_run(self):
    self.session.run("""
      posDict = dict(SYMBOL, STRING)
      sigDict = dict(SYMBOL, ANY)

      def signal_calc(mutable posDict, mutable sigDict, symbol, timestamp, close, mid, upper, lower, pre_close, pre_upper, pre_lower){
        if (!(symbol[0] in posDict.keys())){
          posDict[symbol[0]] = "FLAT"
        }
        pos = posDict[symbol[0]]
        sig = "NO_TRADE"
        if (pos == "FLAT"){
          if ((close[0] < lower[0]) && (pre_close[0] > pre_lower[0])){
            sig = "OPEN_LONG"
            posDict[symbol[0]] = "LONG"
          } else if ((close[0] > upper[0]) && (pre_close[0] < pre_upper[0])){
            sig = "OPEN_SHORT"
            posDict[symbol[0]] = "SHORT"
          }
        } else if (pos == "LONG"){
          if (close[0] > mid[0]){
            sig = "CLOSE_LONG"
            posDict[symbol[0]] = "FLAT"
          }
        } else if (pos == "SHORT"){
          if (close[0] < mid[0]){
            sig = "CLOSE_SHORT"
            posDict[symbol[0]] = "FLAT"
          }
        }
        if (sig != "NO_TRADE"){
          signal = dict(STRING, ANY)
          signal["symbol"] = symbol[0]
          signal["totalVolume"] = 100
          signal["startTime"] = timestamp[0].second() + 1
          signal["numSlice"] = 10
          signal["intervalSec"] = 2
          signal["volumePerSlice"] = signal["totalVolume"] / signal["numSlice"]
          signal["direction"] = sig
          signal["orderSlice"] = dict(INT,BOOL)
          sigDict[symbol[0]] = signal
        }
        return sig
      }
      go
      createReactiveStateEngine(name="reactiveDemo", metrics=<[timestamp, close, signal_calc(posDict, sigDict, symbol, timestamp, close, mean, upper, lower, prev(close), prev(upper), prev(lower))]>, dummyTable=bollST, outputTable=signalST, keyColumn="symbol")
      subscribeTable(tableName="bollST", actionName="reactiveDemo", handler=getStreamEngine(`reactiveDemo), msgAsTable=true, offset=-1)
    """)

  def twap_order(self):
    self.session.run("""
      def twapHandler(mutable orderStream, mutable sigDict, msg){
        sym = msg.symbol[0]
        ts = msg.timestamp[0]
        px = msg.price[0]
        if (sym in sigDict.keys()){
          processedSlice = sigDict[sym]["orderSlice"]
          start_ = sigDict[sym]["startTime"].second()
          interval_ = sigDict[sym]["intervalSec"]
          max_ = sigDict[sym]["numSlice"]
          idx = floor((ts.second() - start_)/interval_)
          if (!(idx in processedSlice.keys()) && (idx >= 0) && (idx < max_)){
            sigDict[sym]["orderSlice"][idx] = true
            dir_ = sigDict[sym]["direction"]
            orderPerSlice_ = sigDict[sym]["volumePerSlice"]
            orderId_ = idx + 1
            insert into orderStream values(orderId_, sym, ts, px, orderPerSlice_, dir_)
          }
          if (idx == max_ - 1){
            sigDict.pop(sym)
          } 
        } 
      }
      go
      subscribeTable(tableName="tickStream", actionName="twapHandler", offset=-1, handler=twapHandler{orderStream, sigDict}, msgAsTable=true)
    """)

  def positions(self):
    self.session.run("""
      def updatePosition(mutable pos_table, mutable positionTable, mutable realized_positions, msg){
        sym = msg.symbol[0]
        vol = msg.qty[0]
        timestamp = msg.timestamp[0]
        price = msg.price[0]
        dir = msg.dir[0]
        offset = msg.offset[0]
        if (offset == "OPEN"){
          // Enqueue
          pos_table.append!(table(sym as sym1, timestamp as timestamp1, price as price1, vol as qty1, dir as dir1)) 
        } else if (offset == "CLOSE"){
          // Dequeue
          pre_dir = iif(dir == "LONG", "SELL", "LONG")
          queue = select * from pos_table where (sym1 = sym and dir1 = pre_dir) order by timestamp1 asc; 
          remaining = vol
          cost = 0.0
          closedVol = 0
          pnl = 0.0
          irow = 0
          do {
            qVol = queue.qty1[irow]
            qPrice = queue.price1[irow]
            if (qVol <= remaining){
              cost += qVol * qPrice
              remaining -= qVol
              closedVol += qVol
              irow += 1
              pnl += iif(dir == "LONG", (cost - closedVol * price), (closedVol * price - cost))
            } else {
              cost += remaining * qPrice
              update pos_table set qty1 = qty1 - remaining where timestamp1 = queue.timestamp1[irow] and sym1 = sym and dir1 = pre_dir;
              closedVol += remaining
              remaining = 0
              pnl += iif(dir == "LONG", (cost - closedVol * price), (closedVol * price - cost))
            }
          } while (remaining > 0 && irow < size(queue))
          if (irow > 0){
            delete from pos_table where sym1=sym and timestamp1 in queue.slice(:irow).timestamp1;
          }
          avg_open = cost / closedVol
          // append realized interests
          insert into realized_positions values(sym, timestamp, avg_open, price, vol, pnl)
          dir = pre_dir
        }
        // summarize open positions
        queue =  select * from pos_table where sym1 = sym and dir1 = dir order by timestamp1 asc; 
        if (size(queue) > 0) {
          totalVol = sum(queue.qty1)
          totalCost = sum(queue.qty1 * queue.price1)
          avgPrice = totalCost / totalVol
          insert into positionTable values(sym, totalVol, avgPrice, dir)
        }
      }
      subscribeTable(tableName="trades", actionName="trade_positions", offset=-1, handler=updatePosition{pos_table, positionTable, realized_positions}, msgAsTable=true)
      def pnl_calc(cur_price, cost_price, val, dir){
        return iif(dir=="LONG", val*(cur_price-cost_price), val*(cost_price-cur_price))
      }
      LjEngine = createLookupJoinEngine(name="position_lookforjoin", leftTable=tickStream, rightTable=positionTable, outputTable=unrealized_positions, metrics=<[tickStream.timestamp, tickStream.price, pnl_calc(tickStream.price, positionTable.price, qty, dir), dir]>, matchingColumn=`symbol, checkTimes=1s)
      subscribeTable(tableName="tickStream", actionName="unrealized_position", offset=-1, handler=appendForJoin{LjEngine, true}, msgAsTable=true)
    """) 

def handler(msg):
  print(msg)

if __name__ == "__main__":
  db = ddb_server()
  s = db.session
  db.create_streamTable()
  s.enableStreaming()
  db.create_table()
  # db.bband_calc()
  # db.strategy_run()
  # db.twap_order()
  db.positions()


  # s.subscribe("localhost", 8848, handler, "orderStream", offset=-1, throttle=0.1)

  s.run("tt = loadText('./IC2311_tick_demo.csv')")
  s.run("td = loadText('./IC2311_trade.csv')")
  
  # # test strategy and orders
  # s.run("""
  #   for (i in tt){
  #     tickStream.append!(tt.slice(i))
  #     sleep(20)
  #   }
  # """)
  
  # test positions 
  s.run("""
    for (i in 0..50){
      if (tt.slice(i).tradetime in td.tradetime){
        temp_ = select * from td where td.tradetime=tt.slice(i).tradetime
        trades.append!(temp_)
        sleep(1000)
      }
      insert into tickStream values(tt.slice(i).values())
      sleep(10) 
    }
  """)


  try:
    while True:
      sleep(1)
  except KeyboardInterrupt:
    s.close()