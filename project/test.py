from vnpy_ctp import CtpGateway
from vnpy_ctp.api import MdApi
import time
from vnpy.trader.utility import get_folder_path

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

class ctp_gateway(CtpGateway):
  
  def __init__(self):
    self.md_api: CtpMdApi = CtpMdApi(self)
  
  def connect(self, setting):
    userid: str = setting["用户名"]
    password: str = setting["密码"]
    brokerid: str = setting["经纪商代码"]
    td_address: str = setting["交易服务器"]
    md_address: str = setting["行情服务器"]
    appid: str = setting["产品名称"]
    auth_code: str = setting["授权编码"]

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

    # self.td_api.connect(td_address, userid, password, brokerid, auth_code, appid)
    self.md_api.connect(md_address, userid, password, brokerid)

class CtpMdApi(MdApi):
  
  def __init__(self, gateway):
    super().__init__()
    self.gateway = "CTP"
    
  def onFrontConnected(self) -> None:
    print("连接成功")
  
  def connect(self, address: str, userid: str, password: str, brokerid: str):
    self.userid = userid
    self.password = password
    self.brokerid = brokerid
    self.connect_status = False

    if not self.connect_status:
      path: Path = get_folder_path(self.gateway.lower())
      self.createFtdcMdApi((str(path) + "\\Md").encode("GBK"))

      self.registerFront(address)
      self.init()

if __name__ == "__main__":
  gate = ctp_gateway()
  gate.connect(ctp_setting)
  
  try:
    while True:
      time.sleep(1)
  except KeyboardInterrupt:
    print("stop running")