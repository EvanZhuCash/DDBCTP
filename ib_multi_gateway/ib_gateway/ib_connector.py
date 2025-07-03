"""
Interactive Brokers Gateway for DolphinDB Trading System
Based on ib_async library for async market data and order execution
"""

from ib_insync import IB, Stock, Future, Contract, MarketOrder, LimitOrder
import dolphindb as ddb
import asyncio
from datetime import datetime
import threading
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class IBGateway:
    """Interactive Brokers网关"""
    
    def __init__(self, config):
        self.ib = IB()
        self.config = config
        self.session = ddb.session()
        self.contracts = {}
        self.subscriptions = {}
        self.connected = False
        
    async def connect(self):
        """连接到IB TWS/Gateway"""
        try:
            # Connect to DolphinDB first
            self.session.connect(
                host=self.config.get('ddb_host', 'localhost'), 
                port=self.config.get('ddb_port', 8848), 
                userid=self.config.get('ddb_user', 'admin'), 
                password=self.config.get('ddb_password', '123456')
            )
            logger.info("Connected to DolphinDB")
            
            # Connect to IB
            await self.ib.connectAsync(
                host=self.config.get('host', '127.0.0.1'),
                port=self.config.get('port', 7497),  # TWS: 7496, Gateway: 4001
                clientId=self.config.get('client_id', 1)
            )
            logger.info("Connected to Interactive Brokers")
            
            self.setup_dolphindb_tables()
            self.connected = True
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to IB: {e}")
            return False
    
    def setup_dolphindb_tables(self):
        """设置DolphinDB表结构"""
        try:
            self.session.run("""
              // IB市场数据流表
              if(existsTable("ib_tick_stream")){
                dropStreamTable(`ib_tick_stream)
              }
              share streamTable(10000:0, `timestamp`symbol`price`volume`bid`ask`bid_size`ask_size`gateway, 
                [TIMESTAMP, SYMBOL, DOUBLE, LONG, DOUBLE, DOUBLE, LONG, LONG, SYMBOL]) as ib_tick_stream
              
              // IB订单流表
              if(existsTable("ib_order_stream")){
                dropStreamTable(`ib_order_stream)
              }
              share streamTable(1000:0, `timestamp`symbol`direction`price`volume`order_type`status`gateway, 
                [TIMESTAMP, SYMBOL, SYMBOL, DOUBLE, INT, SYMBOL, SYMBOL, SYMBOL]) as ib_order_stream
              
              // IB持仓表
              if(existsTable("ib_position_stream")){
                dropStreamTable(`ib_position_stream)
              }
              share streamTable(1000:0, `timestamp`symbol`position`avg_cost`unrealized_pnl`gateway, 
                [TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, SYMBOL]) as ib_position_stream
            """)
            logger.info("DolphinDB tables setup completed")
            
        except Exception as e:
            logger.error(f"Failed to setup DolphinDB tables: {e}")
    
    async def subscribe_market_data(self, contracts):
        """订阅市场数据"""
        for contract_info in contracts:
            try:
                contract = self.create_contract(contract_info)
                self.contracts[contract_info['symbol']] = contract
                
                # 订阅实时数据
                ticker = self.ib.reqMktData(contract, '', False, False)
                ticker.updateEvent += self.on_tick_data
                
                self.subscriptions[contract_info['symbol']] = ticker
                logger.info(f"Subscribed to {contract_info['symbol']}")
                
            except Exception as e:
                logger.error(f"Failed to subscribe to {contract_info['symbol']}: {e}")
    
    def create_contract(self, contract_info):
        """创建IB合约对象"""
        try:
            if contract_info['type'] == 'STK':
                return Stock(
                    contract_info['symbol'], 
                    contract_info['exchange'], 
                    contract_info['currency']
                )
            elif contract_info['type'] == 'FUT':
                return Future(
                    contract_info['symbol'], 
                    contract_info['lastTradeDateOrContractMonth'], 
                    contract_info['exchange'], 
                    contract_info['currency']
                )
            else:
                # 通用合约
                contract = Contract()
                contract.symbol = contract_info['symbol']
                contract.secType = contract_info['type']
                contract.exchange = contract_info['exchange']
                contract.currency = contract_info['currency']
                return contract
                
        except Exception as e:
            logger.error(f"Failed to create contract for {contract_info}: {e}")
            raise
    
    def on_tick_data(self, ticker):
        """处理tick数据"""
        try:
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
                
        except Exception as e:
            logger.error(f"Error processing tick data: {e}")
    
    def send_tick_to_dolphindb(self, tick_data):
        """发送tick数据到DolphinDB"""
        try:
            timestamp_str = tick_data['timestamp'].strftime('%Y.%m.%dT%H:%M:%S.%f')[:-3]
            
            self.session.run(f"""
              insert into ib_tick_stream values(
                {timestamp_str},
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
            logger.error(f"Failed to send tick data to DolphinDB: {e}")
    
    async def disconnect(self):
        """断开连接"""
        try:
            if self.connected:
                self.ib.disconnect()
                self.session.close()
                self.connected = False
                logger.info("Disconnected from IB and DolphinDB")
                
        except Exception as e:
            logger.error(f"Error during disconnect: {e}")


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
            else:
                raise ValueError(f"Unsupported order type: {order_data['order_type']}")
            
            # 提交订单
            trade = self.ib.placeOrder(contract, order)
            
            # 记录订单到DolphinDB
            self.record_order(order_data, trade)
            
            logger.info(f"Order placed: {order_data['symbol']} {order_data['direction']} {order_data['volume']}")
            return trade
            
        except Exception as e:
            logger.error(f"Failed to place order: {e}")
            return None
    
    def record_order(self, order_data, trade):
        """记录订单到DolphinDB"""
        try:
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
            
        except Exception as e:
            logger.error(f"Failed to record order to DolphinDB: {e}")
