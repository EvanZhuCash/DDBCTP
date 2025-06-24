import time
from datetime import datetime
import sys
from threading import Event, Thread
import queue
import dolphindb as ddb

from vnpy_ctp import CtpGateway
from vnpy_ctp.api import (
  MdApi,
  TdApi,
  THOST_FTDC_OPT_LimitPrice,
  THOST_FTDC_TC_GFD,
  THOST_FTDC_VC_AV,
  THOST_FTDC_OPT_AnyPrice,
  THOST_FTDC_TC_IOC,
  THOST_FTDC_VC_CV,
  THOST_FTDC_D_Buy,
  THOST_FTDC_D_Sell,
  THOST_FTDC_OF_Open,
  THOST_FTDC_OFEN_Close,
  THOST_FTDC_HF_Speculation,
  THOST_FTDC_CC_Immediately,
  THOST_FTDC_FCC_NotForceClose
)

from vnpy.trader.utility import get_folder_path
from vnpy.trader.object import OrderRequest_


ORDERTYPE_VT2CTP: dict[str, tuple] = {
  "LIMIT": (THOST_FTDC_OPT_LimitPrice, THOST_FTDC_TC_GFD, THOST_FTDC_VC_AV),
  "MARKET": (THOST_FTDC_OPT_AnyPrice, THOST_FTDC_TC_GFD, THOST_FTDC_VC_AV),
  "FAK": (THOST_FTDC_OPT_LimitPrice, THOST_FTDC_TC_IOC, THOST_FTDC_VC_AV),
  "FOK": (THOST_FTDC_OPT_LimitPrice, THOST_FTDC_TC_IOC, THOST_FTDC_VC_CV),
}

DIRECTION_CTP: dict[str, str] = {
  "LONG": THOST_FTDC_D_Buy,
  "SHORT": THOST_FTDC_D_Sell
}
DIRECTION_CTP_: dict[str, str] = {v:k for k, v in DIRECTION_CTP.items()}

OFFSET_CTP: dict[str, str] = {
  "OPEN": THOST_FTDC_OF_Open,
  "CLOSE": THOST_FTDC_OFEN_Close
}
OFFSET_CTP_: dict[str, str] = {v:k for k, v in OFFSET_CTP.items()}

ctp_setting = {
    "用户名": "239344",
    "密码": "xyzhao@3026528",
    "经纪商代码": "9999",
    "交易服务器": "180.168.146.187:10201",
    "行情服务器": "180.168.146.187:10211",
    "产品名称": "simnow_client_test",
    "授权编码": "0000000000000000",
    "产品信息": ""
}

# ctp_setting = {
#     "用户名": "224829",
#     "密码": "Evan@cash1q2",
#     "经纪商代码": "9999",
#     "交易服务器": "180.168.146.187:10202",
#     "行情服务器": "180.168.146.187:10212",
#     "产品名称": "simnow_client_test",
#     "授权编码": "0000000000000000",
#     "产品信息": ""
# }


MAX_FLOAT = sys.float_info.max

order_queue = queue.Queue()
stop_event = Event()

class ddb_server():
  def __init__(self):
    self.session = ddb.session()
    self.session.connect("localhost", 8848, "admin", "123456")
    
  def create_streamTable(self):
    self.session.run("""
      share streamTable(1000:0, `symbol`timestamp`price`qty`dir`offset, [SYMBOL, TIMESTAMP, DOUBLE, INT, SYMBOL, SYMBOL]) as trades
      share streamTable(1000:0, `symbol`timestamp`price`vol, [SYMBOL, TIMESTAMP, DOUBLE, INT]) as tickStream
      share keyedTable(`symbol, 1:0, `symbol`qty`price`dir, [SYMBOL, INT, DOUBLE, SYMBOL]) as positionTable
      share streamTable(1:0, `symbol`timestamp`price`pnl`dir, [SYMBOL, TIMESTAMP, DOUBLE, DOUBLE, SYMBOL]) as unrealized_positions
      share streamTable(1:0, `symbol`timestamp_close`price_open`price_close`qty`pnl, [SYMBOL, TIMESTAMP, DOUBLE, DOUBLE, INT, DOUBLE]) as realized_positions
      share streamTable(1000:0, `orderID`symbol`timestamp`orderPrice`orderVolume`direction, [INT, SYMBOL, TIMESTAMP, DOUBLE, INT, SYMBOL]) as orderStream
      share streamTable(1000:0, `symbol`timestamp`close`upper`mean`lower, [SYMBOL, TIMESTAMP, DOUBLE, DOUBLE, DOUBLE, DOUBLE]) as bollST
      share streamTable(1000:0, `symbol`timestamp`close`signal, [SYMBOL, TIMESTAMP, DOUBLE, STRING]) as signalST
      share streamTable(1000:0, `timestamp`symbol`price, [TIMESTAMP, SYMBOL, DOUBLE]) as agg1min
      go
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

  ### TODO PHASE 3 请使用vnpy converter的逻辑把这些开平，今昨，多空的逻辑用DDB实现。trader在使用时希望只需要管理持仓即可（即-1->1->2 这样，不需要管开平等）
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

  ### TODO ORDER 管理
  def positions(self):
    self.session.run("""
      positions = dict(SYMBOL, ANY)

      def updatePosition(mutable positions, mutable positionTable, mutable realized_positions, msg){
        sym = msg.sym[0]
        vol = msg.qty[0]
        timestamp = msg.timestamp[0]
        price = msg.price[0]
        dir = msg.dir[0]
        offset = msg.offset[0]
        key = sym + "_" + dir
        if (!(key in positions.keys())){
          positions[key] = table(1:0, `time1`vol1`price`, [TIMESTAMP, INT, DOUBLE])
        }
        if (offset == "OPEN"){
          positions[key] = positions[key].append!(table(timestamp as time1, vol as vol1, price as price1))
        } else if (offset == "CLOSE"){
          pre_dir = iif(dir == "LONG", "SELL", "LONG")
          pre_key = sym + "_" + pre_dir
          queue = positions[pre_key]
          remaining = vol
          cost = 0.0
          closedVol = 0
          pnl = 0.0
          irow = 0
          do {
            qVol = queue.vol1[irow]
            qPrice = queue.price1[irow]
            if (qVol <= remaining){
              cost += qVol * qPrice
              remaining -= qVol
              closedVol += qVol
              irow += 1
              pnl += iif(dir == "LONG", (cost - closedVol*price), (closedVol*price-cost))
            } else {
              update positions[pre_key] set vol1 = vol1 - remaining where time1 = time1[irow]
              closedVol += remaining
              remaining = 0
              pnl += iif(dir == "LONG", (cost - closedVol * price), (closedVol * price - cost))
            }
          } while (remaining > 0 && irow < size(queue))
          if (irow > 0){
            positions[pre_key] = positions[pre_key].slice(irow:)
          }
          key = pre_key
          avg_open = cost / closedVol
          insert into realized_positions values(sym, timestamp, avg_open, price, vol, pnl)
        }
        queue = positions[key]
        if (size(queue) > 0){
          totalVol = sum(queue.vol1)
          totalCost = sum(queue.vol1 * queue.price1)
          avgPrice = totalCost / totalVol
          sym, dir = split(key, "_")
          insert into positionTable values(sym, totalVol, avgPrice, dir)
        }
      }
      subscribeTable(tableName="trades", actionName="trade_positions", offset=-1, handler=updatePosition{positions, positionTable, realized_positions}, msgAsTable=true)
      def pnl_calc(cur_price, cost_price, val, dir){
        return iif(dir=="LONG", val*(cur_price-cost_price), val*(cost_price-cur_price))
      }
      LjEngine = createLookupJoinEngine(name="position_lookforjoin", leftTable=tickStream, rightTable=positionTable, outputTable=unrealized_positions, metrics=<[tickStream.timestamp, tickStream.price, pnl_calc(tickStream.price, positionTable.price, qty, dir), dir]>, matchingColumn=`symbol, checkTimes=1s)
      subscribeTable(tableName="tickStream", actionName="unrealized_position", offset=-1, handler=appendForJoin{LjEngine, true}, msgAsTable=true)
    """)

class ctp_gateway(CtpGateway):
  
  def __init__(self, s):
    self.md_api = CtpMdApi(self, s)
    self.td_api = CtpTdApi(self, s)
  
  def connect(self, setting):
    userid = setting["用户名"]
    password = setting["密码"]
    brokerid = setting["经纪商代码"]
    td_address = setting["交易服务器"]
    md_address = setting["行情服务器"]
    appid = setting["产品名称"]
    auth_code = setting["授权编码"]

    if (
      (not td_address.startswith("tcp://"))
      and (not td_address.startswith("ssl://"))
      and (not td_address.startswith("socks"))
    ):
      td_address = "tcp://" + td_address

    if (
      (not md_address.startswith("tcp://"))
      and (not md_address.startswith("ssl://"))
      and (not md_address.startswith("socks"))
    ):
      md_address = "tcp://" + md_address

    self.td_api.connect(td_address, userid, password, brokerid, auth_code, appid)
    self.md_api.connect(md_address, userid, password, brokerid)
    
  def subscribe(self, req):
    self.md_api.subscribe(req)
    
  def send_order(self, req):
    self.td_api.send_order(req)
    
  def close(self):
    self.td_api.close()
    self.md_api.close()

class CtpMdApi(MdApi):
  
  def __init__(self, gateway, s):
    super().__init__()
    self.gateway = "CTP"
    self.session = s
    
    self.reqid: int = 0
    
    self.connect_status: bool = False
    self.login_status: bool = False
    self.subscribed: set = set()
    
    self.userid: str = ""
    self.password: str = ""
    self.brokerid: str = ""
    
    self.current_date: str = datetime.now().strftime("%Y%m%d")
    
  def onFrontConnected(self):
    print("行情服务器连接成功")
    self.login()
    
  def onFrontDisconnected(self, reason):
    self.login_status = False
    print(f"行情服务器连接断开，原因{reason}")
  
  def onRspUserLogin(self, data, error, reqid, last):
    if not error["ErrorID"]:
      self.login_status = True
      print("行情服务器登录成功")
      
      for symbol in self.subscribed:
        self.subscribeMarketData(symbol)
    else:
      print("行情服务器登录失败", error)
  
  def onRspError(self, error, reqid, last):
    print("行情接口报错", error)
  
  def onRspSubMarketData(self, data, error, reqid, last):
    if not error or not error["ErrorID"]:
      return
    
    print("行情订阅失败", error)
  
  def onRtnDepthMarketData(self, data):
    if not data["UpdateTime"]:
      return
    
    symbol = data["InstrumentID"]
    if not data["ActionDay"]:
      date_str = self.current_date
    else:
      date_str = data["ActionDay"]
    
    date_str_dot = date_str[:4]+"."+date_str[4:6]+"."+date_str[6:]
    milisec_str = str(data["UpdateMillisec"])+"0"*(3-len(str(data["UpdateMillisec"])))
    timestamp = f"{date_str_dot}T{data['UpdateTime']}.{milisec_str}"
    price = 0 if data["LastPrice"] == MAX_FLOAT else data["LastPrice"]
    volume = int(data["Volume"])
    self.session.run(f"insert into tickStream values(`{symbol}, {timestamp}, {price}, {volume})")
  
  def connect(self, address, userid, password, brokerid):
    self.userid = userid
    self.password = password
    self.brokerid = brokerid
    self.connect_status = False

    if not self.connect_status:
      path: Path = get_folder_path(self.gateway.lower())
      self.createFtdcMdApi((str(path) + "\\Md").encode("GBK"))

      self.registerFront(address)
      self.init()
      
      self.connect_status = True
  
  def login(self):
    ctp_req = {
      "UserID": self.userid,
      "Password": self.password,
      "BrokerID": self.brokerid
    }
    
    self.reqid += 1
    self.reqUserLogin(ctp_req, self.reqid)
      
  def subscribe(self, req):
    if self.login_status:
      self.subscribeMarketData(req)
    self.subscribed.add(req)
  
  def close(self):
    if self.connect_status:
      self.exit()
    
class CtpTdApi(TdApi):
  
  def __init__(self, gateway, s):
    super().__init__()
    self.gateway = "CTP"
    self.session = s
    
    self.reqid = 0
    self.order_ref = 0
    
    self.connect_status = False
    self.login_status = False
    self.auth_status = False
    self.login_failed = False
    self.auth_failed = False
    self.contract_inited = False
    
    self.userid = ""
    self.password = ""
    self.brokerid = ""
    self.auth_code = ""
    self.appid = ""
    
    self.frontid = 0
    self.sessionid = 0
    
  def onFrontConnected(self):
    print("交易服务器连接成功")
    if self.auth_code:
      self.authenticate()
    else:
      self.login()
      
  def onFrontDisconnected(self, reason):
    self.login_status = False
    print(f"交易服务器断开，原因{reason}")
    
  def onRspAuthenticate(self, data, error, reqid, last):
    if not error["ErrorID"]:
      self.auth_status = True
      print("交易服务器权限验证成功")
      self.login()
    else:
      if error["ErrorID"] == 63:
        self.auth_failed = True
      print("交易服务器权限验证失败", error)
      
  def onRspUserLogin(self, data, error, reqid, last):
    if not error["ErrorID"]:
      self.frontid = data["FrontID"]
      self.sessionid = data["SessionID"]
      self.login_status = True
      print("交易服务器登录成功")

      ctp_req = {
        "BrokerID": self.brokerid,
        "InvestorID": self.userid
      }
      self.reqid += 1
      self.reqSettlementInfoConfirm(ctp_req, self.reqid)
    else:
      self.login_failed = True
      print("交易服务器登录失败", error)
  
  def onRspSettlementInfoConfirm(self, data, error, reqid, last):
    print("结算信息确认成功")
  
  def onRtnTrade(self, data):
    symbol = data["InstrumentID"]
    date_str = data["TradeDate"][:4] + "." + data["TradeDate"][4:6] + "." + data["TradeDate"][6:]
    timestamp = f"{date_str}T{data['TradeTime']}.000"
    direction = DIRECTION_CTP_[data["Direction"]]
    price = data["Price"]
    qty = int(data["Volume"]) 
    offset = OFFSET_CTP_[data["OffsetFlag"]]
    self.session.run(f"insert into trades values(`{symbol},{timestamp},{price},{qty},{direction},{offset})")
  
  def connect(self, address, userid, password, brokerid, auth_code, appid):
    self.userid = userid
    self.password = password
    self.brokerid = brokerid
    self.auth_code = auth_code
    self.appid = appid
    
    if not self.connect_status:
      path = get_folder_path(self.gateway.lower())
      self.createFtdcTraderApi((str(path) + "\\Td").encode("GBK"))

      self.subscribePrivateTopic(0)
      self.subscribePublicTopic(0)

      self.registerFront(address)
      self.init()

      self.connect_status = True
    else:
      self.authenticate()

  def authenticate(self):
    if self.auth_failed:
      return
    ctp_req = {
      "UserID": self.userid,
      "BrokerID": self.brokerid,
      "AuthCode": self.auth_code,
      "AppID": self.appid 
    }
    
    self.reqid += 1
    self.reqAuthenticate(ctp_req, self.reqid)
    
  def login(self):
    if self.login_failed:
      return
    
    ctp_req = {
      "UserID": self.userid,
      "Password": self.password,
      "BrokerID": self.brokerid
    }
    
    self.reqid += 1
    self.reqUserLogin(ctp_req, self.reqid)
    
  def send_order(self, req):
    self.order_ref += 1
    tp = ORDERTYPE_VT2CTP[req.type]
    price_type, time_condition, volume_condition = tp
    
    ctp_req = {
      "InstrumentID": req.symbol,
      "ExchangeID": req.exchange,
      "LimitPrice": req.price,
      "VolumeTotalOriginal": req.volume,
      "OrderPriceType": price_type,
      "Direction": DIRECTION_CTP.get(req.direction, ""),
      "CombOffsetFlag": OFFSET_CTP.get(req.offset, ""),
      "OrderRef": str(self.order_ref),
      "InvestorID": self.userid,
      "UserID": self.userid,
      "BrokerID": self.brokerid,
      "CombHedgeFlag": THOST_FTDC_HF_Speculation,
      "ContingentCondition": THOST_FTDC_CC_Immediately,
      "ForceCloseReason": THOST_FTDC_FCC_NotForceClose,
      "IsAutoSuspend": 0,
      "TimeCondition": time_condition,
      "VolumeCondition": volume_condition,
      "MinVolume": 1
    }
    
    self.reqid += 1
    n = self.reqOrderInsert(ctp_req, self.reqid)
    if n:
      print(f"委托请求发送失败，错误代码：{n}")
    
  def close(self):
    if self.connect_status:
      self.exit()

def handler(msg):
  order_queue.put(msg)


def send_order(gateway):
  while not stop_event.is_set():
    try:
      order = order_queue.get(timeout=1)
      symbol_ = order[1]
      price_ = order[3]
      volume_ = order[4]
      offset_, dir_ = order[5].split("_")
      if offset_ == "CLOSE":
        direction_ = "LONG" if dir_ == "SHORT" else "SHORT"
      else:
        direction_ = dir_
      req_order = OrderRequest_(
        symbol = symbol_,
        exchange = "CFFEX",-
        direction = direction_,
        type = "LIMIT",
        volume = volume_,
        price = price_,
        offset = offset_
      )
      gateway.send_order(req_order)
    except queue.Empty:
      continue
  print("thread_send_order is out")
  
if __name__ == "__main__":
  
  # initialize dolphindb server
  db = ddb_server()
  s = db.session
  db.create_streamTable()
  s.enableStreaming()
  db.bband_calc()
  db.strategy_run()
  db.twap_order()
  db.positions()
  
  s.subscribe("localhost", 8848, handler, "orderStream", offset=-1, throttle=0.1)
  
  gateway = ctp_gateway(s)
  gateway.connect(ctp_setting)
  gateway.subscribe("IC2509")
  
  thread_order = Thread(target=send_order, args=(gateway,))
  thread_order.start()
  
  try:
    while True:
      time.sleep(1)
  except KeyboardInterrupt:
    print("stop running")
    stop_event.set()
    gateway.close()
    
  thread_order.join()
  time.sleep(5)
  s.close()
  
  ### TODO PHASE 3 请用连接图的方式总结一下python 和 DDB 分别用了哪些线程和进程？
  ### 3.10的python的线程理论上还是会被全局锁影响，是否用进程会好一些？

  ### TODO 后续可能需要另外一个更简单的strategy，比如每隔多少秒买入，然后多少秒后卖出。这样子一来触发信号直观一些，二来也可以模拟多策略同时进行的场景
  
  ### TODO 需要一个文档，关于每个table的介绍
  
  ### TODO 另外我ctrl + C 停止python test_live.py后，是需要重新启动ddb才能再python test_live.py吗？如果是的话看看能不能改进一下
