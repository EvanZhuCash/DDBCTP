"""
Basic data structure used for general trading function in the trading platform.
"""

from dataclasses import dataclass, field
from logging import INFO


@dataclass
class BaseData:
    """
    Any data object needs a gateway_name as source
    and should inherit base data.
    """

    gateway_name: str

    extra: dict = field(default=None, init=False)

@dataclass
class OrderRequest_():
    symbol: str
    exchange: str
    direction: str
    type_: str
    volume: int
    price: float
    offset: str
    reference: str = "origin"
    orderid: int = 0


EXCHANGE_SYM: dict[str, str] = {
  "rb2509": "SHFE",
  "au2508": "SHFE",
  "ag2508": "SHFE",
  "fu2509": "SHFE",
  "ru2509": "SHFE",
  "cu2507": "SHFE",
  "sc2507": "INE",
  "p2509": "DCE",
  "i2509": "DCE",
  "jm2509": "DCE",
  "m2509": "DCE",
  "y2509": "DCE",
  "eb2507": "DCE",
  "v2509": "DCE",
  "MA509": "CZCE",
  "OI509": "CZCE",
  "TA509": "CZCE",
  "SH509": "CZCE",
  "FG509": "CZCE",
  "SA509": "CZCE",
  "UR509": "CZCE",
  "SR509": "CZCE",
  "IM2509": "CFFEX",
  "IC2509": "CFFEX",
  "TS2509": "CFFEX",
  "TL2509": "CFFEX",
  "TF2509": "CFFEX",
  "T2509": "CFFEX",
  "IF2509": "CFFEX",
  "IH2509": "CFFEX",
}
CONTRACT_SIZE: dict[str, int] = {
  "i2509": 100,
  "IC2509": 200
}