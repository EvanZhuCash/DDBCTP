import time
from datetime import datetime
import sys
from threading import Event, Thread
import queue
import dolphindb as ddb
import logging
logger = logging.getLogger("heartbeatwarning")
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler("heartbeatwarning.log")
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

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
  THOST_FTDC_OFEN_CloseToday,
  THOST_FTDC_OFEN_CloseYesterday,
  THOST_FTDC_HF_Speculation,
  THOST_FTDC_CC_Immediately,
  THOST_FTDC_FCC_NotForceClose,
  THOST_FTDC_OST_NoTradeQueueing,
  THOST_FTDC_OST_PartTradedQueueing,
  THOST_FTDC_OST_AllTraded,
  THOST_FTDC_OST_Canceled,
  THOST_FTDC_OST_Unknown,
  THOST_FTDC_PD_Long,
  THOST_FTDC_PD_Short,
  THOST_FTDC_AF_Delete
)

from vnpy.trader.utility import get_folder_path
from vnpy.trader.object import OrderRequest_, EXCHANGE_SYM, CONTRACT_SIZE

STATUS_CTP: dict[str, str] = {
    THOST_FTDC_OST_NoTradeQueueing: "NOTTRADED",
    THOST_FTDC_OST_PartTradedQueueing: "PARTTRADED",
    THOST_FTDC_OST_AllTraded: "ALLTRADED",
    THOST_FTDC_OST_Canceled: "CANCELLED",
    THOST_FTDC_OST_Unknown: "SUBMITTING"
}

ORDERTYPE_VT2CTP: dict[str, tuple] = {
  "LIMIT": (THOST_FTDC_OPT_LimitPrice, THOST_FTDC_TC_GFD, THOST_FTDC_VC_AV),
  "MARKET": (THOST_FTDC_OPT_AnyPrice, THOST_FTDC_TC_GFD, THOST_FTDC_VC_AV),
  "FAK": (THOST_FTDC_OPT_LimitPrice, THOST_FTDC_TC_IOC, THOST_FTDC_VC_AV),
  "FOK": (THOST_FTDC_OPT_LimitPrice, THOST_FTDC_TC_IOC, THOST_FTDC_VC_CV),
}
ORDERTYPE_CTP_: dict[tuple, str] = {v:k for k, v in ORDERTYPE_VT2CTP.items()}

DIRECTION_CTP: dict[str, str] = {
  "LONG": THOST_FTDC_D_Buy,
  "SHORT": THOST_FTDC_D_Sell
}
DIRECTION_CTP_: dict[str, str] = {v:k for k, v in DIRECTION_CTP.items()}
DIRECTION_CTP_[THOST_FTDC_PD_Long] = "LONG"
DIRECTION_CTP_[THOST_FTDC_PD_Short] = "SHORT"

OFFSET_CTP: dict[str, str] = {
  "OPEN": THOST_FTDC_OF_Open,
  "CLOSE": THOST_FTDC_OFEN_Close,
  "CLOSETODAY": THOST_FTDC_OFEN_CloseToday,
  "CLOSEYESTERDAY": THOST_FTDC_OFEN_CloseYesterday
}
OFFSET_CTP_: dict[str, str] = {v:k for k, v in OFFSET_CTP.items()}


ctp_setting = {
    "用户名": "239344",
    "密码": "xyzhao@3026528",
    "经纪商代码": "9999",
    "交易服务器": "182.254.243.31:30001",
    "行情服务器": "182.254.243.31:30011",
    "产品名称": "simnow_client_test",
    "授权编码": "0000000000000000",
    "产品信息": ""
}

MAX_FLOAT = sys.float_info.max

order_queue = queue.Queue()
order_fail_queue = queue.Queue()
order_cancel_queue = queue.Queue()
stop_event = Event()

class ddb_server():
  def __init__(self):
    self.session = ddb.session()
    self.session.connect("localhost", 8900, "admin", "123456")
    
  def create_streamTable(self):
    self.session.run("""
      share streamTable(1000:0, `symbol`timestamp`price`qty`dir`offset`exchange, [SYMBOL, TIMESTAMP, DOUBLE, INT, SYMBOL, SYMBOL, SYMBOL]) as trades
      share streamTable(1000:0, `symbol`timestamp`price`vol, [SYMBOL, TIMESTAMP, DOUBLE, INT]) as tickStream
      share streamTable(1:0, `symbol`timestamp`price`pnl`dir, [SYMBOL, TIMESTAMP, DOUBLE, DOUBLE, SYMBOL]) as unrealized_positions
      share streamTable(1:0, `symbol`timestamp_close`price_open`price_close`qty`pnl, [SYMBOL, TIMESTAMP, DOUBLE, DOUBLE, INT, DOUBLE]) as realized_positions
      share streamTable(1000:0, `orderID`symbol`timestamp`orderPrice`orderVolume`direction, [INT, SYMBOL, TIMESTAMP, DOUBLE, INT, SYMBOL]) as orderStream
      share streamTable(1000:0, `symbol`timestamp`close`upper`mean`lower, [SYMBOL, TIMESTAMP, DOUBLE, DOUBLE, DOUBLE, DOUBLE]) as bollST
      share streamTable(1000:0, `symbol`timestamp`close`signal, [SYMBOL, TIMESTAMP, DOUBLE, STRING]) as signalST
      share streamTable(1000:0, `timestamp`symbol`price, [TIMESTAMP, SYMBOL, DOUBLE]) as agg1min
      share streamTable(1000:0, `orderid`timestamp`symbol`exchange`direction`offset`status`volume`traded, [INT, TIMESTAMP, SYMBOL, SYMBOL, SYMBOL, SYMBOL, SYMBOL, INT, INT]) as positions_order
      share streamTable(1:0, `orderid`symbol, [INT, SYMBOL]) as cancelStream
      go
    """)
  
  def create_table(self):
    self.session.run("""
      share keyedTable(`symbol`dir, 1:0, `symbol`qty`price`dir, [SYMBOL, INT, DOUBLE, SYMBOL]) as positionTable
      share keyedTable(`orderid, 1:0, `orderid`symbol`timestamp`price`vol`dir`offset`status`type, [INT,SYMBOL,TIMESTAMP,DOUBLE,INT,SYMBOL,SYMBOL,SYMBOL,SYMBOL]) as orders
      share keyedTable(`symbol, 1000:0, `timestamp`symbol`long_pos`long_yd`long_td`short_pos`short_yd`short_td`long_pos_frozen`long_yd_frozen`long_td_frozen`short_pos_frozen`short_yd_frozen`short_td_frozen, [TIMESTAMP,SYMBOL,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT]) as positionholding
      share table(1000:0, `sym1`timestamp1`price1`qty1`dir1, [SYMBOL, TIMESTAMP, DOUBLE, INT, SYMBOL]) as pos_table
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
          pos_table.append!(table(sym as sym1, timestamp as timestamp1, price as price1, vol as qty1, dir as dir1)) 
        } else if (offset == "CLOSE"){
          pre_dir = iif(dir == "LONG", "SHORT", "LONG")
          queue = select * from pos_table where (sym1 = sym and dir1 = pre_dir) order by timestamp1 asc; 
          if (size(queue)==0){return}
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
          insert into realized_positions values(sym, timestamp, avg_open, price, vol, pnl)
          dir = pre_dir
        }
        queue =  select * from pos_table where sym1 = sym and dir1 = dir order by timestamp1 asc; 
        if (size(queue) > 0) {
          totalVol = sum(queue.qty1)
          totalCost = sum(queue.qty1 * queue.price1)
          avgPrice = totalCost / totalVol
          insert into positionTable values(sym, totalVol, avgPrice, dir)
        }
        else {
          delete from positionTable where symbol=sym and dir=dir
        }
      }
      subscribeTable(tableName="trades", actionName="trade_positions", offset=-1, handler=updatePosition{pos_table, positionTable, realized_positions}, msgAsTable=true)
      def pnl_calc(cur_price, cost_price, val, dir){
        return iif(dir=="LONG", val*(cur_price-cost_price), val*(cost_price-cur_price))
      }
      def unrealizedpnl(mutable unrealized_positions, pnl_calc, positionTable, msg){
        sym_ = msg.symbol[0]
        price_ = msg.price[0]
        time_ = msg.timestamp[0]
        posi_ = select * from positionTable where symbol=sym_
        if (size(posi_)==1){
          dir_ = posi_.dir[0]
          cost_price_ = posi_.price[0]
          vol_ = posi_.qty[0]
          pnl = pnl_calc(price_, cost_price_, vol_, dir_)
          insert into unrealized_positions values(sym_, time_, price_, pnl, dir_)
        }
        else if (size(posi_)==2){
          for (i in 0..1){
            dir_ = posi_.dir[i]
            cost_price_ = posi_.price[i]
            vol_ = posi_.qty[i]
            pnl = pnl_calc(price_, cost_price_, vol_, dir_)
            insert into unrealized_positions values(sym_, time_, price_, pnl, dir_)
          }
        }
      } 
      subscribeTable(tableName="tickStream", actionName="unrealized_pnl_handler", handler=unrealizedpnl{unrealized_positions, pnl_calc, positionTable}, msgAsTable=true)
    """)
  
  def simulate_order(self):
    self.session.run("""
      def simulate_order(mutable orderStream, tickStream, positionTable, msg){
        time_ = msg.timestamp[0]
        sym_ = msg.symbol[0]
        num_sec = secondOfMinute(time_)
        num_min = minuteOfHour(time_)
        temp_ = select timestamp, price from tickStream where symbol=sym_ order by timestamp desc limit 2
        pre_num_sec = secondOfMinute(temp_.timestamp[1])
        orderprice = temp_.price[0]
        orderid = count(orderStream)
        if (mod(num_min,2)==0){
          if ((num_sec<30)&&(div(num_sec,20)!=div(pre_num_sec,20))){
            open_ = select qty from positionTable where symbol=sym_ and dir="LONG"
            if ((size(open_)==0)||(open_.qty[0]<5)){
              insert into orderStream values(orderid, sym_, time_, orderprice, 1, "OPEN_LONG")
            }
          }
          else if ((num_sec>30)&&(div(num_sec,20)!=div(pre_num_sec,20))){
            close_ = select qty from positionTable where symbol=sym_ and dir="LONG"
            if ((size(close_)>0)&&(close_.qty[0]>=1)){
              insert into orderStream values(orderid, sym_, time_, orderprice, 1, "CLOSE_LONG")
            } 
          }
        }
        else if (mod(num_min,2)==1){
          if ((num_sec<30)&&(div(num_sec,20)!=div(pre_num_sec,20))){
            open_ = select qty from positionTable where symbol=sym_ and dir="SHORT"
            if ((size(open_)==0)||(open_.qty[0]<5)){
              insert into orderStream values(orderid, sym_, time_, orderprice, 1, "OPEN_SHORT")
            }
          }
          else if ((num_sec>30)&&(div(num_sec,20)!=div(pre_num_sec,20))){
            close_ = select qty from positionTable where symbol=sym_ and dir="SHORT"
            if ((size(close_)>0)&&(close_.qty[0]>=1)){
              insert into orderStream values(orderid, sym_, time_, orderprice, 1, "CLOSE_SHORT")
            }
          }
        }
      }
      go
      subscribeTable(tableName="tickStream", actionName="simulateHandler", offset=-1, handler=simulate_order{orderStream, tickStream, positionTable}, msgAsTable=true)
    """)
    
  def position_update(self):
    self.session.run("""
      def update_order_handler(mutable positionholding, msg){
        timestamp = msg.timestamp[0]
        sym = msg.symbol[0]
        status = msg.status[0]
        if (!(status in ["NOTTRADED", "PARTTRADED"])) {return}
        offset = msg.offset[0]
        direction = msg.direction[0]
        volume = msg.volume[0]
        traded = msg.traded[0]
        symbols = select symbol from positionholding
        if (sym in symbols.symbol) {
            temp_ = select long_pos, long_td, long_yd, short_pos, short_td, short_yd from positionholding where symbol=sym
            long_pos = temp_.long_pos[0]
            long_td = temp_.long_td[0]
            long_yd = temp_.long_yd[0]
            short_pos = temp_.short_pos[0]
            short_td = temp_.short_td[0]
            short_yd = temp_.short_yd[0]
            if (offset == "OPEN") {return}
            long_pos_frozen = 0
            long_yd_frozen = 0
            long_td_frozen = 0
            short_pos_frozen = 0
            short_yd_frozen = 0
            short_td_frozen = 0
            frozen = volume - traded
            if (direction == "LONG"){
                if (offset == "CLOSETODAY"){
                    short_td_frozen = short_td_frozen + frozen
                }
                else if (offset == "CLOSEYESTERDAY"){
                    short_yd_frozen = short_yd_frozen + frozen
                }
                else if (offset == "CLOSE"){
                    short_td_frozen = short_td_frozen + frozen
                    if (short_td_frozen > short_td){
                        short_yd_frozen = short_yd_frozen + short_td_frozen - short_td
                        short_td_frozen = short_td
                    }
                }
            }
            else if (direction == "SHORT"){
                if (offset == "CLOSETODAY"){
                    long_td_frozen = long_td_frozen + frozen
                }
                else if (offset == "CLOSEYESTERDAY"){
                    long_yd_frozen = long_yd_frozen + frozen
                }
                else if (offset == "CLOSE"){
                    long_td_frozen = long_td_frozen + frozen
                    if (long_td_frozen > long_td){
                        long_yd_frozen = long_yd_frozen + long_td_frozen - long_td
                        long_td_frozen = long_td
                    }
                }
            }
            long_td_frozen = min(long_td_frozen, long_td)
            long_yd_frozen = min(long_yd_frozen, long_yd)
            short_td_frozen = min(short_td_frozen, short_td)
            short_yd_frozen = min(short_yd_frozen, short_yd)
            long_pos_frozen = long_td_frozen + long_yd_frozen
            short_pos_frozen = short_td_frozen + short_yd_frozen
            insert into positionholding values(timestamp, sym, long_pos, long_yd, long_td, short_pos, short_yd, short_td, long_pos_frozen, long_yd_frozen, long_td_frozen, short_pos_frozen, short_yd_frozen, short_td_frozen)
        }
      }
      go
      subscribeTable(tableName="positions_order", actionName="update_order_handler", offset=-1, handler=update_order_handler{positionholding}, msgAsTable=true)
      
      def update_trade_handler(mutable positionholding, msg){
          sym = msg.symbol[0]
          direction_trade = msg.dir[0]
          volume = msg.qty[0]
          timestamp = msg.timestamp[0]
          offset = msg.offset[0]
          exchange = msg.exchange[0]
          temp_ = select long_td, long_yd, long_td_frozen, long_yd_frozen, short_td, short_yd, short_td_frozen, short_yd_frozen from positionholding where symbol=sym
          if (size(temp_)>0){
            long_td = temp_.long_td[0]
            long_yd = temp_.long_yd[0]
            short_td = temp_.short_td[0]
            short_yd = temp_.short_yd[0]
            long_td_frozen = temp_.long_td_frozen[0]
            long_yd_frozen = temp_.long_yd_frozen[0]
            short_td_frozen = temp_.short_td_frozen[0]
            short_yd_frozen = temp_.short_yd_frozen[0]
          }
          else{
            if (offset=="CLOSE"){return}
            long_td = 0
            long_yd = 0
            short_td = 0
            short_yd = 0
            long_td_frozen = 0
            long_yd_frozen = 0
            short_td_frozen = 0
            short_yd_frozen = 0
          }
          if (direction_trade == "LONG"){
              if (offset == "OPEN") {long_td = long_td + volume}
              else if (offset == "CLOSETODAY") {short_td = short_td - volume}
              else if (offset == "CLOSEYESTERDAY") {short_yd = short_yd - volume}
              else if (offset == "CLOSE") {
                  if (exchange in ["SHFE", "INE"]){
                      short_yd = short_yd - volume
                  }
                  else {
                      short_td = short_td - volume
                      if (short_td < 0){
                          short_yd = short_yd + short_td
                          short_td = 0
                      }
                  }
              }
          }
          else if (direction_trade == "SHORT"){
              if (offset == "OPEN") {short_td = short_td + volume}
              else if (offset == "CLOSETODAY") {long_td = long_td - volume}
              else if (offset == "CLOSEYESTERDAY") {long_yd = long_yd - volume}
              else if (offset == "CLOSE") {
                  if (exchange in ["SHFE", "INE"]){
                      long_yd = long_yd - volume
                  }
                  else {
                      long_td = long_td - volume
                      if (long_td < 0){
                          long_yd = long_yd + long_td
                          long_td = 0
                      }
                  }
              }
          }
          long_pos = long_td + long_yd
          short_pos = short_td + short_yd
          if ((long_pos == 0)&&(short_pos == 0)){
              delete from positionholding where symbol=sym
          }
          else {
              long_td_frozen = min(long_td_frozen, long_td)
              long_yd_frozen = min(long_yd_frozen, long_yd)
              short_td_frozen = min(short_td_frozen, short_td)
              short_yd_frozon = min(short_yd_frozen, short_yd)
              long_pos_frozen = long_td_frozen + long_yd_frozen
              short_pos_frozen = short_td_frozen + short_yd_frozen
              insert into positionholding values(timestamp, sym, long_pos, long_yd, long_td, short_pos, short_yd, short_td, long_pos_frozen, long_yd_frozen, long_td_frozen, short_pos_frozen, short_yd_frozen, short_td_frozen)
          }
      }
      go 
      subscribeTable(tableName="trades", actionName="update_trade_handler", offset=-1, handler=update_trade_handler{positionholding}, msgAsTable=true)
    """)

  def cancel_order(self):
    self.session.run("""
      orderdict = dict(INT, SYMBOL)
      def cancel_handler(mutable cancelStream, orders, mutable orderdict, msg){
        sym_ = msg.symbol[0]
        time_ = msg.timestamp[0]
        order_temp_ = select orderid, timestamp from orders where symbol=sym_ and status in ["PARTTRADED", "NOTTRADED"]
        if (size(order_temp_) > 0){
          for (i in 0..size(order_temp_)){
            if (time_ - order_temp_.timestamp[i] > 3000){
              orderid_ = order_temp_.orderid[i]
              if (orderdict[orderid_] is null){
                insert into cancelStream values(orderid_, sym_)
                orderdict[orderid_] = "CANCELLED" 
              }
            }
          }
        }
      }
      go
      subscribeTable(tableName="tickStream", actionName="cancelHandler", offset=-1, handler=cancel_handler{cancelStream, orders, orderdict}, msgAsTable=true)
    """)
    
class ctp_gateway(CtpGateway):
  
  def __init__(self, s, fail_queue, logger):
    self.md_api = CtpMdApi(self, s, logger)
    self.td_api = CtpTdApi(self, s, fail_queue)
  
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
    
  def cancel_order(self, req):
    self.td_api.cancel_order(req)
    
  def close(self):
    self.td_api.close()
    self.md_api.close()

class CtpMdApi(MdApi):
  
  def __init__(self, gateway, s, logger):
    super().__init__()
    self.gateway = "CTP"
    self.session = s
    self.logger = logger
    
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
  
  def onHeartBeatWarning(self, reqid):
    self.logger.info(f"距离上次接收报文的事件, {reqid}")
  
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
      path = get_folder_path(self.gateway.lower())
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
  
  def __init__(self, gateway, s, fail_queue):
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
    self.fail_queue = fail_queue
    
    self.interval = 10
    self.position_success = False
    self.position_thread_ = Thread(target=self.query_position)
    
    self.failed_order: dict[int, int] = {}
    
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
  
  def onRspQryInvestorPosition(self, data, error, reqid, last):
    if not data:
      return
    if last:
      self.position_success = True
    
    symbol = data["InstrumentID"]
    direction = DIRECTION_CTP_[data["PosiDirection"]]
    offset = "OPEN"
    exchange = EXCHANGE_SYM[symbol]
    
    size = CONTRACT_SIZE[symbol]
    
    if exchange in {"SHFE", "INE"}:
      if data["YdPosition"] and not data["TodayPosition"]:
        yd_volume = int(data["Position"])
    else:
      yd_volume = int(data["Position"]) - int(data["TodayPosition"])
    
    volume = int(data["Position"])
    price = data["PositionCost"] / (volume*size)
    time_ = datetime.strftime(datetime.now(), "%Y.%m.%d %H:%M:%S.%f")
    timestamp = time_.split(" ")[0] + "T" + time_.split(" ")[1][:-3]
    last_value = 0
    
    if direction == "LONG":
      long_pos = volume
      long_yd = yd_volume
      long_td = long_pos - long_yd
      short_pos = 0
      short_yd = 0
      short_td = 0
    else:
      short_pos = volume
      short_yd = yd_volume
      short_td = short_pos - short_yd
      long_pos = 0
      long_yd = 0
      long_td = 0
    self.session.run(f"insert into positionholding values({timestamp}, `{symbol}, \
                     {long_pos}, {long_yd}, {long_td}, {short_pos},\
                     {short_yd}, {short_td}, {last_value}, {last_value}, {last_value}, {last_value}, {last_value}, {last_value})") 
    self.session.run(f"insert into trades values(`{symbol}, {timestamp}, {price}, {volume}, `{direction}, `{offset}, `{exchange})")
  
  def onRspOrderInsert(self, data, error, reqid, last):
    print("交易委托失败", error)
    orderid = int(data["OrderRef"])
    if (not (orderid in self.failed_order.keys())):
      self.failed_order[orderid] = 0
    if self.failed_order[orderid] <= 3:
      self.failed_order[orderid] += 1 
      symbol = data["InstrumentID"]
      price = data["LimitPrice"]
      volume = data["VolumeTotalOriginal"]
      direction = DIRECTION_CTP_[data["Direction"]]
      offset = OFFSET_CTP_[data["CombOffsetFlag"]]
      self.fail_queue.put([orderid, symbol, price, volume, direction, offset])
    else:
      print("停止重新提交订单")
  
  def onRtnOrder(self, data):
    symbol = data["InstrumentID"]
    exchange = EXCHANGE_SYM[symbol]
    orderid = int(data["OrderRef"])
    status = STATUS_CTP.get(data["OrderStatus"], None)
    if not status:
      print(f"收到不支持的委托状态，{orderid}")
      return
    date_str = data["InsertDate"][:4] + "." + data["InsertDate"][4:6] + "." + data["InsertDate"][6:]
    timestamp = f"{date_str}T{data['InsertTime']}.000"
    tp = (data["OrderPriceType"], data["TimeCondition"], data["VolumeCondition"])
    order_type = ORDERTYPE_CTP_.get(tp, None)
    if not order_type:
      return
    
    if order_type == "MARKET":
      price = "NULL"
    else:
      price = data["LimitPrice"]
    direction = DIRECTION_CTP_[data["Direction"]]
    offset = OFFSET_CTP_[data["CombOffsetFlag"]]
    volume = int(data["VolumeTotalOriginal"])
    traded = int(data["VolumeTraded"])
    self.session.run(f"insert into orders values({orderid}, `{symbol}, {timestamp}, {price}, {volume}, `{direction}, `{offset}, `{status}, `{order_type})")
    self.session.run(f"insert into positions_order values({orderid}, {timestamp}, `{symbol}, `{exchange}, `{direction}, `{offset}, `{status}, {volume}, {traded})") 
 
  def onRtnTrade(self, data):
    symbol = data["InstrumentID"]
    exchange = EXCHANGE_SYM[symbol]
    date_str = data["TradeDate"][:4] + "." + data["TradeDate"][4:6] + "." + data["TradeDate"][6:]
    timestamp = f"{date_str}T{data['TradeTime']}.000"
    direction = DIRECTION_CTP_[data["Direction"]]
    price = data["Price"]
    qty = int(data["Volume"]) 
    offset = OFFSET_CTP_[data["OffsetFlag"]]
    self.session.run(f"insert into trades values(`{symbol},{timestamp},{price},{qty},`{direction},`{offset}, `{exchange})")
  
  def connect(self, address, userid, password, brokerid, auth_code, appid):
    self.userid = userid
    self.password = password
    self.brokerid = brokerid
    self.auth_code = auth_code
    self.appid = appid
    
    if not self.connect_status:
      path = get_folder_path(self.gateway.lower())
      self.createFtdcTraderApi((str(path) + "\\Td").encode("GBK"))

      self.subscribePrivateTopic(2)
      self.subscribePublicTopic(2)

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
  
  def query_position(self):
    ctp_req = {
      "BrokerID": self.brokerid,
      "InvestorID": self.userid
    }
    while not self.position_success:
      self.reqid += 1
      self.reqQryInvestorPosition(ctp_req, self.reqid)
      time.sleep(self.interval)
    print("完成持仓数据请求")
    
  def send_order(self, req):
    if req.reference == "origin":
      self.order_ref += 1
      orderid = self.order_ref
    else:
      orderid = req.orderid
    tp = ORDERTYPE_VT2CTP[req.type_]
    price_type, time_condition, volume_condition = tp
    
    ctp_req = {
      "InstrumentID": req.symbol,
      "ExchangeID": req.exchange,
      "LimitPrice": req.price,
      "VolumeTotalOriginal": req.volume,
      "OrderPriceType": price_type,
      "Direction": DIRECTION_CTP.get(req.direction, ""),
      "CombOffsetFlag": OFFSET_CTP.get(req.offset, ""),
      "OrderRef": str(orderid),
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
    
  def cancel_order(self, req):
    ctp_req = {
      "InstrumentID": req.symbol,
      "ExchangeID": req.exchange,
      "OrderRef": str(req.orderid),
      "FrontID": self.frontid,
      "SessionID": self.sessionid,
      "ActionFlag": THOST_FTDC_AF_Delete,
      "BrokerID": self.brokerid,
      "InvestorID": self.userid
    }
    
    self.reqid += 1
    self.reqOrderAction(ctp_req, self.reqid)
  
  def close(self):
    if self.connect_status:
      self.exit()

def handler(msg):
  order_queue.put(msg)

def handler_cancel(msg):
  order_cancel_queue.put(msg)

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
        exchange = EXCHANGE_SYM[symbol_],
        direction = direction_,
        type_ = "LIMIT",
        volume = volume_,
        price = price_,
        offset = offset_
      )
      gateway.send_order(req_order)
    except queue.Empty:
      continue
    time.sleep(0.01)
  print("thread_send_order is out")
  
  
def failed_order(gateway):
  while not stop_event.is_set():
    try:
      failed_order = order_fail_queue.get(timeout=1)
      orderid_ = failed_order[0]
      symbol_ = failed_order[1]
      price_ = failed_order[2]
      volume_ = failed_order[3]
      dir_ = failed_order[4]
      offset_ = failed_order[5]
      if offset_ == "CLOSE":
        direction_ = "LONG" if dir_ == "SHORT" else "SHORT"
      else:
        direction_ = dir_
      req_order = OrderRequest_(
        symbol = symbol_,
        exchange = EXCHANGE_SYM[symbol_],
        direction = direction_,
        type_ = "LIMIT",
        volume = volume_,
        price = price_,
        offset = offset_,
        reference = "failed_order",
        orderid = orderid_
      )
      gateway.send_order(req_order)
    except queue.Empty:
      continue
    time.sleep(0.01)
  print("thread_failed_order is out")
  
def cancel_order(gateway):
  while not stop_event.is_set():
    try:
      order_cancels = order_cancel_queue.get(timeout=1)
      orderid_ = order_cancels[0]
      symbol_ = order_cancels[1]
      exchange_ = EXCHANGE_SYM[symbol_]
      cancel_req_ = OrderRequest_(
        symbol = symbol_,
        exchange = exchange_,
        orderid = orderid_
      )
      gateway.cancel_order(cancel_req_)
    except queue.Empty:
      continue
    time.sleep(0.01)
  print("thread_cancel_order is out")
  
if __name__ == "__main__":
  
  # initialize dolphindb server
  db = ddb_server()
  s = db.session
  db.create_streamTable()
  s.enableStreaming()
  db.create_table()
  db.simulate_order()
  db.positions()
  db.position_update()
  db.cancel_order()
  
  
  s.subscribe("localhost", 8900, handler, "orderStream", offset=-1, throttle=0.1)
  s.subscribe("localhost", 8900, handler_cancel, "cancelStream", offset=-1, throttle=0.1)
  
  gateway = ctp_gateway(s, order_fail_queue, logger)
  gateway.connect(ctp_setting)
  # gateway.td_api.position_thread_.start()
  # gateway.td_api.position_thread_.join()
  for i in ["IC2509"]:
    gateway.subscribe(i)
  
  thread_order = Thread(target=send_order, args=(gateway,), daemon=True)
  thread_failed_order = Thread(target=failed_order, args=(gateway,), daemon=True)
  thread_order_cancel = Thread(target=cancel_order, args=(gateway,), daemon=True)
  thread_order.start()
  thread_failed_order.start()
  thread_order_cancel.start()
  
  try:
    while True:
      time.sleep(3)
  except KeyboardInterrupt:
    print("stop running")
    stop_event.set()
    gateway.close()

  time.sleep(5)
  s.close()