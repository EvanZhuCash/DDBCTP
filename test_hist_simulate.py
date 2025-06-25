import dolphindb as ddb
from time import sleep

class ddb_server():
  def __init__(self):
    self.session = ddb.session()
    self.session.connect("localhost", 8848, "admin", "123456")
    
  def create_streamTable(self):
    tables = [
      ("trades", "share streamTable(1000:0, `symbol`timestamp`price`qty`dir`offset, [SYMBOL, TIMESTAMP, DOUBLE, INT, SYMBOL, SYMBOL]) as trades"),
      ("tickStream", "share streamTable(1000:0, `symbol`timestamp`price`vol, [SYMBOL, TIMESTAMP, DOUBLE, INT]) as tickStream"),
      ("unrealized_positions", "share streamTable(1:0, `symbol`timestamp`price`pnl`dir, [SYMBOL, TIMESTAMP, DOUBLE, DOUBLE, SYMBOL]) as unrealized_positions"),
      ("realized_positions", "share streamTable(1:0, `symbol`timestamp_close`price_open`price_close`qty`pnl, [SYMBOL, TIMESTAMP, DOUBLE, DOUBLE, INT, DOUBLE]) as realized_positions"),
      ("orderStream", "share streamTable(1000:0, `orderID`symbol`timestamp`orderPrice`orderVolume`direction, [INT, SYMBOL, TIMESTAMP, DOUBLE, INT, SYMBOL]) as orderStream"),
      ("bollST", "share streamTable(1000:0, `symbol`timestamp`close`upper`mean`lower, [SYMBOL, TIMESTAMP, DOUBLE, DOUBLE, DOUBLE, DOUBLE]) as bollST"),
      ("signalST", "share streamTable(1000:0, `symbol`timestamp`close`signal, [SYMBOL, TIMESTAMP, DOUBLE, STRING]) as signalST"),
      ("agg1min", "share streamTable(1000:0, `timestamp`symbol`price, [TIMESTAMP, SYMBOL, DOUBLE]) as agg1min")
    ]

    for table_name, table_sql in tables:
      try:
        self.session.run(table_sql)
        print(f"Created table: {table_name}")
      except Exception as e:
        print(f"Table {table_name} already exists or error: {e}")

    self.session.run("go")
  
  def create_table(self):
    try:
      self.session.run("""
        share keyedTable(`symbol, 1:0, `symbol`qty`price`dir, [SYMBOL, INT, DOUBLE, SYMBOL]) as positionTable
        share table(1000:0, `sym1`timestamp1`price1`qty1`dir1, [SYMBOL, TIMESTAMP, DOUBLE, INT, SYMBOL]) as pos_table
      """)
      print("Created position tables")
    except Exception as e:
      print(f"Position tables already exist or error: {e}")

  def simulate_order(self):
    """Order simulation function - generates orders based on tick data"""
    # First define the function with a unique name
    self.session.run("""
      def order_simulator(mutable orderStream, tickStream, positionTable, msg){
        time_ = msg.timestamp[0]
        sym_ = msg.symbol[0]
        price_ = msg.price[0]
        orderid = count(orderStream)

        // Generate order every 5 ticks
        if (mod(orderid, 5) == 0) {
          insert into orderStream values(orderid, sym_, time_, price_, 1, "OPEN_LONG")
          print("Generated OPEN_LONG order #" + string(orderid) + " for " + string(sym_) + " at price " + string(price_))
        }
      }
    """)

    # Then create the subscription with a new action name
    try:
      self.session.run("""
        subscribeTable(tableName="tickStream", actionName="orderSimulator", offset=-1, handler=order_simulator{orderStream, tickStream, positionTable}, msgAsTable=true)
      """)
      print("orderSimulator subscription created")
    except Exception as e:
      print(f"orderSimulator subscription already exists: {e}")

  def order_to_trade_converter(self):
    """Convert orders to trades for simulation"""
    self.session.run("""
      def order_to_trade(mutable trades, msg){
        orderid = msg.orderID[0]
        sym = msg.symbol[0]
        timestamp = msg.timestamp[0]
        price = msg.orderPrice[0]
        volume = msg.orderVolume[0]
        direction = msg.direction[0]
        
        // Convert order direction to trade direction and offset
        if (direction == "OPEN_LONG") {
          dir = "LONG"
          offset = "OPEN"
        } else if (direction == "CLOSE_LONG") {
          dir = "SELL"  // Selling to close long position
          offset = "CLOSE"
        } else if (direction == "OPEN_SHORT") {
          dir = "SHORT"
          offset = "OPEN"
        } else if (direction == "CLOSE_SHORT") {
          dir = "LONG"  // Buying to close short position
          offset = "CLOSE"
        }
        
        // Insert trade with slight delay to simulate execution
        insert into trades values(sym, timestamp + 100, price, volume, dir, offset)
        print("Executed trade: " + string(direction) + " " + string(volume) + " " + string(sym) + " at " + string(price))
      }
      go
      try {
        subscribeTable(tableName="orderStream", actionName="orderToTrade", offset=-1, handler=order_to_trade{trades}, msgAsTable=true)
      } catch(ex) {
        print("orderToTrade subscription already exists")
      }
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
      // Clean up existing engines
      try { dropStreamEngine("position_lookforjoin") } catch(ex) {}

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
      try {
        subscribeTable(tableName="trades", actionName="trade_positions", offset=-1, handler=updatePosition{pos_table, positionTable, realized_positions}, msgAsTable=true)
      } catch(ex) {
        print("trade_positions subscription already exists")
      }

      def pnl_calc(cur_price, cost_price, val, dir){
        return iif(dir=="LONG", val*(cur_price-cost_price), val*(cost_price-cur_price))
      }

      try {
        LjEngine = createLookupJoinEngine(name="position_lookforjoin", leftTable=tickStream, rightTable=positionTable, outputTable=unrealized_positions, metrics=<[tickStream.timestamp, tickStream.price, pnl_calc(tickStream.price, positionTable.price, qty, dir), dir]>, matchingColumn=`symbol, checkTimes=1s)
        subscribeTable(tableName="tickStream", actionName="unrealized_position", offset=-1, handler=appendForJoin{LjEngine, true}, msgAsTable=true)
      } catch(ex) {
        print("unrealized_position subscription already exists")
      }
    """) 

def handler(msg):
  print(msg)

if __name__ == "__main__":
  # First run cleanup
  print("Running cleanup...")
  import subprocess
  subprocess.run(["python", "cleanup.py"], cwd=".")

  print("Starting historical simulation test...")
  db = ddb_server()
  s = db.session
  db.create_streamTable()
  s.enableStreaming()
  db.create_table()
  
  # Add order simulation functionality
  db.simulate_order()
  db.order_to_trade_converter()
  
  # Add position tracking
  db.positions()

  # Subscribe to order stream to monitor orders
  s.subscribe("localhost", 8848, handler, "orderStream", offset=-1, throttle=0.1)

  import os
  current_dir = os.getcwd()
  tick_file = os.path.join(current_dir, "IC2311_tick_demo.csv").replace("\\", "/")

  s.run(f"tt = loadText('{tick_file}')")
  
  # Process historical data with order simulation
  print("Starting data processing with order simulation...")
  s.run("""
    for (i in 0..100){
      insert into tickStream values(tt.slice(i).values())
      if (i % 20 == 0) print("Processed " + string(i) + " tick records")
      sleep(50)  // Faster processing for simulation
    }
  """)

  print("Data processing completed!")

  # Check results
  print("Checking results...")
  trades_count = s.run("size(trades)")
  ticks_count = s.run("size(tickStream)")
  orders_count = s.run("size(orderStream)")
  positions_count = s.run("size(positionTable)")
  realized_count = s.run("size(realized_positions)")

  print(f"Ticks processed: {ticks_count}")
  print(f"Orders generated: {orders_count}")
  print(f"Trades executed: {trades_count}")
  print(f"Position table size: {positions_count}")
  print(f"Realized positions: {realized_count}")

  if orders_count > 0:
    print("Generated orders:")
    orders = s.run("select * from orderStream")
    print(orders)

  if trades_count > 0:
    print("Executed trades:")
    trades = s.run("select * from trades")
    print(trades)

  if positions_count > 0:
    print("Current positions:")
    positions = s.run("select * from positionTable")
    print(positions)

  if realized_count > 0:
    print("Realized positions:")
    realized = s.run("select * from realized_positions")
    print(realized)

  s.close()
  print("Historical simulation test completed successfully!")
