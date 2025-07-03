# IB_Async多网关接入DolphinDB交易系统方案

## 1. 项目概述

### 1.1 目标
基于现有vnpy_ctp架构，开发支持多个交易网关的DolphinDB交易系统：
- Interactive Brokers (IB) - 主要目标
- Binance Futures - 加密货币期货
- OKX - 加密货币交易所
- Bybit - 加密货币衍生品
- Gate.io - 加密货币交易所

### 1.2 架构设计
```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   IB Gateway    │    │ Binance Gateway │    │   OKX Gateway   │
│   (ib_async)    │    │  (vnpy_binance) │    │  (vnpy_okx)     │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
                    ┌─────────────────┐
                    │  DolphinDB      │
                    │  Data Engine    │
                    │                 │
                    │ - Tick Stream   │
                    │ - Factor Calc   │
                    │ - Order Mgmt    │
                    │ - Risk Control  │
                    └─────────────────┘
```

## 2. IB Gateway实现

### 2.1 IB_Async连接器
```python
# ib_gateway/ib_connector.py
from ib_insync import IB, Stock, Future, Contract, MarketOrder, LimitOrder
import dolphindb as ddb
import asyncio
from datetime import datetime
import threading

class IBGateway:
    """Interactive Brokers网关"""
    
    def __init__(self, config):
        self.ib = IB()
        self.config = config
        self.session = ddb.session()
        self.session.connect("localhost", 8848, "admin", "123456")
        self.contracts = {}
        self.subscriptions = {}
        
    async def connect(self):
        """连接到IB TWS/Gateway"""
        try:
            await self.ib.connectAsync(
                host=self.config.get('host', '127.0.0.1'),
                port=self.config.get('port', 7497),  # TWS: 7496, Gateway: 4001
                clientId=self.config.get('client_id', 1)
            )
            print("Connected to Interactive Brokers")
            self.setup_dolphindb_tables()
            return True
        except Exception as e:
            print(f"Failed to connect to IB: {e}")
            return False
    
    def setup_dolphindb_tables(self):
        """设置DolphinDB表结构"""
        self.session.run("""
          // IB市场数据流表
          share streamTable(10000:0, `timestamp`symbol`price`volume`bid`ask`bid_size`ask_size`gateway, 
            [TIMESTAMP, SYMBOL, DOUBLE, LONG, DOUBLE, DOUBLE, LONG, LONG, SYMBOL]) as ib_tick_stream
          
          // IB订单流表
          share streamTable(1000:0, `timestamp`symbol`direction`price`volume`order_type`status`gateway, 
            [TIMESTAMP, SYMBOL, SYMBOL, DOUBLE, INT, SYMBOL, SYMBOL, SYMBOL]) as ib_order_stream
          
          // IB持仓表
          share streamTable(1000:0, `timestamp`symbol`position`avg_cost`unrealized_pnl`gateway, 
            [TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, SYMBOL]) as ib_position_stream
        """)
    
    async def subscribe_market_data(self, contracts):
        """订阅市场数据"""
        for contract_info in contracts:
            contract = self.create_contract(contract_info)
            self.contracts[contract_info['symbol']] = contract
            
            # 订阅实时数据
            ticker = self.ib.reqMktData(contract, '', False, False)
            ticker.updateEvent += self.on_tick_data
            
            self.subscriptions[contract_info['symbol']] = ticker
            print(f"Subscribed to {contract_info['symbol']}")
    
    def create_contract(self, contract_info):
        """创建IB合约对象"""
        if contract_info['type'] == 'STK':
            return Stock(contract_info['symbol'], contract_info['exchange'], contract_info['currency'])
        elif contract_info['type'] == 'FUT':
            return Future(contract_info['symbol'], contract_info['lastTradeDateOrContractMonth'], 
                         contract_info['exchange'], contract_info['currency'])
        else:
            # 通用合约
            contract = Contract()
            contract.symbol = contract_info['symbol']
            contract.secType = contract_info['type']
            contract.exchange = contract_info['exchange']
            contract.currency = contract_info['currency']
            return contract
    
    def on_tick_data(self, ticker):
        """处理tick数据"""
        if ticker.last and ticker.lastSize:
            tick_data = {
                'timestamp': datetime.now(),
                'symbol': ticker.contract.symbol,
                'price': float(ticker.last),
                'volume': int(ticker.lastSize),
                'bid': float(ticker.bid) if ticker.bid else 0.0,
                'ask': float(ticker.ask) if ticker.ask else 0.0,
                'bid_size': int(ticker.bidSize) if ticker.bidSize else 0,
                'ask_size': int(ticker.askSize) if ticker.askSize else 0,
                'gateway': 'IB'
            }
            
            # 发送到DolphinDB
            self.send_tick_to_dolphindb(tick_data)
    
    def send_tick_to_dolphindb(self, tick_data):
        """发送tick数据到DolphinDB"""
        try:
            self.session.run(f"""
              insert into ib_tick_stream values(
                {tick_data['timestamp'].strftime('%Y.%m.%dT%H:%M:%S.%f')[:-3]},
                '{tick_data['symbol']}',
                {tick_data['price']},
                {tick_data['volume']},
                {tick_data['bid']},
                {tick_data['ask']},
                {tick_data['bid_size']},
                {tick_data['ask_size']},
                '{tick_data['gateway']}'
              )
            """)
        except Exception as e:
            print(f"Failed to send tick data to DolphinDB: {e}")
```

### 2.2 IB订单执行
```python
class IBOrderManager:
    """IB订单管理器"""
    
    def __init__(self, ib_gateway):
        self.ib = ib_gateway.ib
        self.session = ib_gateway.session
        self.contracts = ib_gateway.contracts
        
    async def place_order(self, order_data):
        """下单"""
        try:
            contract = self.contracts.get(order_data['symbol'])
            if not contract:
                raise ValueError(f"Contract not found for {order_data['symbol']}")
            
            # 创建订单
            if order_data['order_type'] == 'MARKET':
                order = MarketOrder(
                    action=order_data['direction'],
                    totalQuantity=order_data['volume']
                )
            elif order_data['order_type'] == 'LIMIT':
                order = LimitOrder(
                    action=order_data['direction'],
                    totalQuantity=order_data['volume'],
                    lmtPrice=order_data['price']
                )
            
            # 提交订单
            trade = self.ib.placeOrder(contract, order)
            
            # 记录订单到DolphinDB
            self.record_order(order_data, trade)
            
            return trade
            
        except Exception as e:
            print(f"Failed to place order: {e}")
            return None
    
    def record_order(self, order_data, trade):
        """记录订单到DolphinDB"""
        self.session.run(f"""
          insert into ib_order_stream values(
            now(),
            '{order_data['symbol']}',
            '{order_data['direction']}',
            {order_data.get('price', 0.0)},
            {order_data['volume']},
            '{order_data['order_type']}',
            'SUBMITTED',
            'IB'
          )
        """)
```

## 3. 加密货币网关实现

### 3.1 Binance Gateway
```python
# crypto_gateways/binance_gateway.py
from vnpy_binance import BinanceUsdtGateway
import dolphindb as ddb

class BinanceDolphinDBGateway(BinanceUsdtGateway):
    """Binance网关DolphinDB适配器"""
    
    def __init__(self, event_engine):
        super().__init__(event_engine)
        self.session = ddb.session()
        self.session.connect("localhost", 8848, "admin", "123456")
        self.setup_tables()
    
    def setup_tables(self):
        """设置Binance专用表"""
        self.session.run("""
          share streamTable(10000:0, `timestamp`symbol`price`volume`bid`ask`gateway, 
            [TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, SYMBOL]) as binance_tick_stream
          
          share streamTable(1000:0, `timestamp`symbol`direction`price`volume`status`gateway, 
            [TIMESTAMP, SYMBOL, SYMBOL, DOUBLE, DOUBLE, SYMBOL, SYMBOL]) as binance_order_stream
        """)
    
    def on_tick(self, tick):
        """处理tick数据"""
        super().on_tick(tick)
        
        # 发送到DolphinDB
        self.session.run(f"""
          insert into binance_tick_stream values(
            now(),
            '{tick.symbol}',
            {tick.last_price},
            {tick.volume},
            {tick.bid_price_1},
            {tick.ask_price_1},
            'BINANCE'
          )
        """)
```

### 3.2 多网关统一接口
```python
# gateway_manager.py
class MultiGatewayManager:
    """多网关管理器"""
    
    def __init__(self):
        self.gateways = {}
        self.session = ddb.session()
        self.session.connect("localhost", 8848, "admin", "123456")
        self.setup_unified_tables()
    
    def setup_unified_tables(self):
        """设置统一数据表"""
        self.session.run("""
          // 统一tick数据表
          share streamTable(10000:0, `timestamp`symbol`price`volume`bid`ask`gateway, 
            [TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, SYMBOL]) as unified_tick_stream
          
          // 统一订单表
          share streamTable(1000:0, `timestamp`symbol`direction`price`volume`status`gateway, 
            [TIMESTAMP, SYMBOL, SYMBOL, DOUBLE, DOUBLE, SYMBOL, SYMBOL]) as unified_order_stream
          
          // 网关状态表
          share streamTable(100:0, `timestamp`gateway`status`latency`error_count, 
            [TIMESTAMP, SYMBOL, SYMBOL, DOUBLE, INT]) as gateway_status
        """)
    
    def register_gateway(self, name, gateway):
        """注册网关"""
        self.gateways[name] = gateway
        print(f"Registered gateway: {name}")
    
    def route_order(self, order_data):
        """智能订单路由"""
        gateway_name = self.select_best_gateway(order_data['symbol'])
        gateway = self.gateways.get(gateway_name)
        
        if gateway:
            return gateway.place_order(order_data)
        else:
            raise ValueError(f"No available gateway for {order_data['symbol']}")
    
    def select_best_gateway(self, symbol):
        """选择最佳网关"""
        # 根据延迟、费用、流动性等因素选择
        gateway_scores = self.session.run("""
          select gateway, avg(latency) as avg_latency, sum(error_count) as total_errors
          from gateway_status 
          where timestamp > now() - 300000  // 最近5分钟
          group by gateway
          order by avg_latency asc, total_errors asc
        """)
        
        if len(gateway_scores) > 0:
            return gateway_scores['gateway'][0]
        else:
            return list(self.gateways.keys())[0]  # 默认第一个
```

## 4. 测试环境配置

### 4.1 IB测试配置
```python
# config/ib_config.py
IB_CONFIG = {
    'host': '127.0.0.1',
    'port': 7497,  # TWS Paper Trading
    'client_id': 1,
    'contracts': [
        {
            'symbol': 'AAPL',
            'type': 'STK',
            'exchange': 'SMART',
            'currency': 'USD'
        },
        {
            'symbol': 'ES',
            'type': 'FUT',
            'exchange': 'CME',
            'currency': 'USD',
            'lastTradeDateOrContractMonth': '20241220'
        }
    ]
}
```

### 4.2 加密货币测试配置
```python
# config/crypto_config.py
CRYPTO_CONFIGS = {
    'binance': {
        'testnet': True,
        'api_key': 'your_testnet_api_key',
        'secret': 'your_testnet_secret',
        'base_url': 'https://testnet.binancefuture.com',
        'symbols': ['BTCUSDT', 'ETHUSDT', 'ADAUSDT']
    },
    'okx': {
        'testnet': True,
        'api_key': 'your_testnet_api_key',
        'secret': 'your_testnet_secret',
        'passphrase': 'your_passphrase',
        'symbols': ['BTC-USDT-SWAP', 'ETH-USDT-SWAP']
    },
    'bybit': {
        'testnet': True,
        'api_key': 'your_testnet_api_key',
        'secret': 'your_testnet_secret',
        'symbols': ['BTCUSDT', 'ETHUSDT']
    }
}
```

## 5. 实施计划

### 第1周：IB Gateway开发
- 实现ib_async连接器
- 开发市场数据订阅
- 实现基本订单执行

### 第2周：加密货币网关集成
- 集成vnpy_binance
- 集成vnpy_okx和vnpy_bybit
- 实现统一数据接口

### 第3周：多网关管理
- 开发网关管理器
- 实现智能订单路由
- 添加网关监控

### 第4周：测试和优化
- 连接测试环境
- 性能测试和优化
- 文档编写

## 6. 预期收益

### 6.1 市场覆盖
- 传统金融市场 (IB)
- 加密货币市场 (Binance, OKX, Bybit)
- 全球24小时交易

### 6.2 风险分散
- 多网关备份
- 降低单点故障风险
- 提高系统可靠性

### 6.3 套利机会
- 跨市场套利
- 跨网关价差交易
- 流动性聚合
