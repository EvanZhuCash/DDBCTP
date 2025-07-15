import time
from datetime import datetime
import sys
import dolphindb as ddb
import logging

logger = logging.getLogger("heartbeatwarning")
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler("heartbeatwarning.log")
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

from vnpy_ctp.api import MdApi
from vnpy.trader.utility import get_folder_path

ctp_setting = {
    "用户名": "224829",
    "密码": "Evan@cash1q2",
    "经纪商代码": "9999",
    "交易服务器": "182.254.243.31:30001",
    "行情服务器": "182.254.243.31:30011",
    "产品名称": "simnow_client_test",
    "授权编码": "0000000000000000",
    "产品信息": ""
}
MAX_FLOAT = sys.float_info.max
hostname = "192.168.91.124"

class ddb_server():
  def __init__(self):
    self.session = ddb.session()
    self.session.connect(hostname, 8848, "admin", "123456")

  def create_streamTable(self):
    self.session.run("""
      share streamTable(1000:0, `symbol`timestamp`last_price`volume`bid1`ask1`bid_vol1`ask_vol1`turnover`open_interest`exchange_id`trading_day`open_price`high_price`low_price`pre_close_price`pre_settlement_price`upper_limit`lower_limit`pre_open_interest`settlement_price`close_price`bid2`bid_vol2`ask2`ask_vol2`bid3`bid_vol3`ask3`ask_vol3`bid4`bid_vol4`ask4`ask_vol4`bid5`bid_vol5`ask5`ask_vol5`average_price`action_day`exchange_inst_id,
                     [SYMBOL, TIMESTAMP, DOUBLE, LONG, DOUBLE, DOUBLE, LONG, LONG, DOUBLE, DOUBLE, SYMBOL, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, LONG, DOUBLE, LONG, DOUBLE, LONG, DOUBLE, LONG, DOUBLE, LONG, DOUBLE, LONG, DOUBLE, LONG, DOUBLE, LONG, DOUBLE, SYMBOL, SYMBOL]) as tickStream
      share streamTable(1000:0, `timestamp`symbol`last_price`volume`bid1`ask1`bid_vol1`ask_vol1`turnover`open_interest`open_price`high_price`low_price,
                     [TIMESTAMP, SYMBOL, DOUBLE, LONG, DOUBLE, DOUBLE, LONG, LONG, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE]) as agg1min
      go
    """)

  def create_agg1min_engine(self):
    self.session.run("""
      engine1min = createTimeSeriesEngine(name="engine1min", windowSize=60000, step=60000,
      metrics=<[last(last_price), sum(volume), last(bid1), last(ask1), last(bid_vol1), last(ask_vol1), sum(turnover), last(open_interest), last(open_price), max(high_price), min(low_price)]>,
      dummyTable=tickStream, outputTable=agg1min, timeColumn="timestamp",useSystemTime=false,keyColumn="symbol",
      useWindowStartTime=false)
      go
      subscribeTable(tableName=`tickStream, actionName="engine1min", handler=tableInsert{engine1min}, msgAsTable=true, offset=-1)
    """)



class ctp_gateway():

  def __init__(self, s, logger):
    self.md_api = CtpMdApi(self, s, logger)

  def connect(self, setting):
    userid = setting["用户名"]
    password = setting["密码"]
    brokerid = setting["经纪商代码"]
    md_address = setting["行情服务器"]

    if (
      (not md_address.startswith("tcp://"))
      and (not md_address.startswith("ssl://"))
      and (not md_address.startswith("socks"))
    ):
      md_address = "tcp://" + md_address

    self.md_api.connect(md_address, userid, password, brokerid)

  def subscribe(self, req):
    self.md_api.subscribe(req)

  def close(self):
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

    # Extract all available fields with proper null handling
    last_price = 0 if data["LastPrice"] == MAX_FLOAT else data["LastPrice"]
    volume = int(data["Volume"])
    bid1 = 0 if data["BidPrice1"] == MAX_FLOAT else data["BidPrice1"]
    ask1 = 0 if data["AskPrice1"] == MAX_FLOAT else data["AskPrice1"]
    bid_vol1 = int(data["BidVolume1"])
    ask_vol1 = int(data["AskVolume1"])
    turnover = 0 if data["Turnover"] == MAX_FLOAT else data["Turnover"]
    open_interest = 0 if data["OpenInterest"] == MAX_FLOAT else data["OpenInterest"]
    exchange_id = data.get("ExchangeID", "")
    trading_day = data.get("TradingDay", "")
    open_price = 0 if data["OpenPrice"] == MAX_FLOAT else data["OpenPrice"]
    high_price = 0 if data["HighestPrice"] == MAX_FLOAT else data["HighestPrice"]
    low_price = 0 if data["LowestPrice"] == MAX_FLOAT else data["LowestPrice"]
    pre_close_price = 0 if data["PreClosePrice"] == MAX_FLOAT else data["PreClosePrice"]
    pre_settlement_price = 0 if data["PreSettlementPrice"] == MAX_FLOAT else data["PreSettlementPrice"]
    upper_limit = 0 if data["UpperLimitPrice"] == MAX_FLOAT else data["UpperLimitPrice"]
    lower_limit = 0 if data["LowerLimitPrice"] == MAX_FLOAT else data["LowerLimitPrice"]
    pre_open_interest = 0 if data["PreOpenInterest"] == MAX_FLOAT else data["PreOpenInterest"]
    settlement_price = 0 if data["SettlementPrice"] == MAX_FLOAT else data["SettlementPrice"]
    close_price = 0 if data["ClosePrice"] == MAX_FLOAT else data["ClosePrice"]

    # Bid/Ask levels 2-5
    bid2 = 0 if data["BidPrice2"] == MAX_FLOAT else data["BidPrice2"]
    bid_vol2 = int(data["BidVolume2"])
    ask2 = 0 if data["AskPrice2"] == MAX_FLOAT else data["AskPrice2"]
    ask_vol2 = int(data["AskVolume2"])
    bid3 = 0 if data["BidPrice3"] == MAX_FLOAT else data["BidPrice3"]
    bid_vol3 = int(data["BidVolume3"])
    ask3 = 0 if data["AskPrice3"] == MAX_FLOAT else data["AskPrice3"]
    ask_vol3 = int(data["AskVolume3"])
    bid4 = 0 if data["BidPrice4"] == MAX_FLOAT else data["BidPrice4"]
    bid_vol4 = int(data["BidVolume4"])
    ask4 = 0 if data["AskPrice4"] == MAX_FLOAT else data["AskPrice4"]
    ask_vol4 = int(data["AskVolume4"])
    bid5 = 0 if data["BidPrice5"] == MAX_FLOAT else data["BidPrice5"]
    bid_vol5 = int(data["BidVolume5"])
    ask5 = 0 if data["AskPrice5"] == MAX_FLOAT else data["AskPrice5"]
    ask_vol5 = int(data["AskVolume5"])

    average_price = 0 if data.get("AveragePrice", MAX_FLOAT) == MAX_FLOAT else data.get("AveragePrice", 0)
    action_day = data.get("ActionDay", "")
    exchange_inst_id = data.get("ExchangeInstID", "")

    # Insert comprehensive tick data
    # Handle empty string values properly for DolphinDB
    exchange_id_val = f'"{exchange_id}"' if exchange_id else '""'
    trading_day_val = f'"{trading_day}"' if trading_day else '""'
    action_day_val = f'"{action_day}"' if action_day else '""'
    exchange_inst_id_val = f'"{exchange_inst_id}"' if exchange_inst_id else '""'

    # Create the insert statement on a single line to avoid parsing issues
    insert_sql = f"insert into tickStream values(`{symbol}, {timestamp}, {last_price}, {volume}l, {bid1}, {ask1}, {bid_vol1}l, {ask_vol1}l, {turnover}, {open_interest}, {exchange_id_val}, {trading_day_val}, {open_price}, {high_price}, {low_price}, {pre_close_price}, {pre_settlement_price}, {upper_limit}, {lower_limit}, {pre_open_interest}, {settlement_price}, {close_price}, {bid2}, {bid_vol2}l, {ask2}, {ask_vol2}l, {bid3}, {bid_vol3}l, {ask3}, {ask_vol3}l, {bid4}, {bid_vol4}l, {ask4}, {ask_vol4}l, {bid5}, {bid_vol5}l, {ask5}, {ask_vol5}l, {average_price}, {action_day_val}, {exchange_inst_id_val})"

    self.session.run(insert_sql)

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

def get_all_instruments():
  """
  Get all available instruments for trading using DolphinDB CTP plugin
  Returns a list of instrument IDs
  """
  session = ddb.session()
  session.connect(hostname, 8848, "admin", "123456")

  # Load CTP plugin (ignore if already loaded)
  try:
    session.run('loadPlugin("ctp")')
  except:
    pass  # Plugin already loaded

  # Query all instruments
  script = '''
  result = ctp::queryInstrument("182.254.243.31", 30001,"224829", "Evan@cash1q2", "9999", "simnow_client_test", "0000000000000000");
  ids = exec instrumentID from result;
  ids
  '''

  result = session.run(script)
  session.close()

  print(f"Found {len(result)} instruments")
  return result

if __name__ == "__main__":
  import sys
  import argparse

  # Parse command line arguments
  parser = argparse.ArgumentParser(description='CTP Market Data Recorder')
  parser.add_argument('--get-instruments', action='store_true',
                     help='Get list of all available instruments and exit')
  parser.add_argument('--all-instruments', action='store_true',
                     help='Subscribe to ALL available instruments')
  parser.add_argument('--futures-only', action='store_true',
                     help='Subscribe to futures contracts only (excludes options)')
  parser.add_argument('--contracts', nargs='+',
                     help='Specific contracts to subscribe to (e.g., --contracts sc2509 IC2509)')

  args = parser.parse_args()

  # Check if user wants to get all instruments
  if args.get_instruments:
    print("Getting all available instruments...")
    instruments = get_all_instruments()
    print(f"Found {len(instruments)} instruments:")
    for i, inst in enumerate(instruments):
      print(f"  {i+1}. {inst}")
    sys.exit(0)

  # First run cleanup
  print("Running cleanup...")
  import subprocess
  subprocess.run(["python", "cleanup.py"], cwd=".")

  # initialize dolphindb server
  db = ddb_server()
  s = db.session
  db.create_streamTable()
  s.enableStreaming()
  db.create_agg1min_engine()

  gateway = ctp_gateway(s, logger)
  gateway.connect(ctp_setting)

  # Determine which contracts to subscribe to
  contracts_to_subscribe = []

  if args.all_instruments or args.futures_only:
    print("Getting all available instruments...")
    all_instruments = get_all_instruments()

    if args.futures_only:
      # Filter for futures only (exclude options which typically have longer names or specific patterns)
      futures_contracts = []
      for inst in all_instruments:
        # Basic filtering: futures typically have shorter names and follow patterns like AB2509
        # Options often have longer names with strike prices and C/P indicators
        if len(inst) <= 6 and not any(char in inst.upper() for char in ['C', 'P']):
          futures_contracts.append(inst)
      contracts_to_subscribe = futures_contracts
      print(f"Filtered to {len(contracts_to_subscribe)} futures contracts")
    else:
      contracts_to_subscribe = all_instruments
      print(f"Using all {len(contracts_to_subscribe)} instruments")

  elif args.contracts:
    contracts_to_subscribe = args.contracts
    print(f"Using specified contracts: {contracts_to_subscribe}")
  else:
    # Default contracts
    contracts_to_subscribe = ["sc2509", "IC2509"]
    print(f"Using default contracts: {contracts_to_subscribe}")

  # Subscribe to contracts
  print(f"Subscribing to {len(contracts_to_subscribe)} contracts...")
  for i, contract in enumerate(contracts_to_subscribe):
    print(f"  {i+1}/{len(contracts_to_subscribe)}: {contract}")
    gateway.subscribe(contract)

  try:
    while True:
      time.sleep(1)
  except KeyboardInterrupt:
    print("stop running")
    gateway.close()

  time.sleep(5)
  s.close()