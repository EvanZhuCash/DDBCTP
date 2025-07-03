"""
Multi-Gateway Manager for DolphinDB Trading System
Manages multiple trading gateways (IB, Binance, OKX, Bybit, etc.)
"""

import dolphindb as ddb
import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MultiGatewayManager:
    """多网关管理器"""
    
    def __init__(self, ddb_config: Dict[str, Any] = None):
        self.gateways = {}
        self.gateway_status = {}
        self.session = ddb.session()
        
        # Default DolphinDB configuration
        self.ddb_config = ddb_config or {
            'host': 'localhost',
            'port': 8848,
            'user': 'admin',
            'password': '123456'
        }
        
        self.connected = False
        
    async def initialize(self):
        """初始化管理器"""
        try:
            # Connect to DolphinDB
            self.session.connect(
                host=self.ddb_config['host'],
                port=self.ddb_config['port'],
                userid=self.ddb_config['user'],
                password=self.ddb_config['password']
            )
            logger.info("Connected to DolphinDB")
            
            # Setup unified tables
            self.setup_unified_tables()
            self.connected = True
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize gateway manager: {e}")
            return False
    
    def setup_unified_tables(self):
        """设置统一数据表"""
        try:
            self.session.run("""
              // 统一tick数据表
              if(existsTable("unified_tick_stream")){
                dropStreamTable(`unified_tick_stream)
              }
              share streamTable(10000:0, `timestamp`symbol`price`volume`bid`ask`gateway, 
                [TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, SYMBOL]) as unified_tick_stream
              
              // 统一订单表
              if(existsTable("unified_order_stream")){
                dropStreamTable(`unified_order_stream)
              }
              share streamTable(1000:0, `timestamp`symbol`direction`price`volume`status`gateway, 
                [TIMESTAMP, SYMBOL, SYMBOL, DOUBLE, DOUBLE, SYMBOL, SYMBOL]) as unified_order_stream
              
              // 网关状态表
              if(existsTable("gateway_status")){
                dropStreamTable(`gateway_status)
              }
              share streamTable(100:0, `timestamp`gateway`status`latency`error_count, 
                [TIMESTAMP, SYMBOL, SYMBOL, DOUBLE, INT]) as gateway_status
              
              // 持仓汇总表
              if(existsTable("unified_position_stream")){
                dropStreamTable(`unified_position_stream)
              }
              share streamTable(1000:0, `timestamp`symbol`position`avg_cost`unrealized_pnl`gateway, 
                [TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, SYMBOL]) as unified_position_stream
            """)
            logger.info("Unified DolphinDB tables setup completed")
            
        except Exception as e:
            logger.error(f"Failed to setup unified tables: {e}")
            raise
    
    def register_gateway(self, name: str, gateway: Any):
        """注册网关"""
        try:
            self.gateways[name] = gateway
            self.gateway_status[name] = {
                'status': 'REGISTERED',
                'last_update': datetime.now(),
                'error_count': 0,
                'latency': 0.0
            }
            
            # Record gateway registration
            self.update_gateway_status(name, 'REGISTERED', 0.0, 0)
            logger.info(f"Registered gateway: {name}")
            
        except Exception as e:
            logger.error(f"Failed to register gateway {name}: {e}")
    
    def update_gateway_status(self, gateway_name: str, status: str, latency: float, error_count: int):
        """更新网关状态"""
        try:
            self.session.run(f"""
              insert into gateway_status values(
                now(),
                '{gateway_name}',
                '{status}',
                {latency},
                {error_count}
              )
            """)
            
            # Update local status
            if gateway_name in self.gateway_status:
                self.gateway_status[gateway_name].update({
                    'status': status,
                    'last_update': datetime.now(),
                    'latency': latency,
                    'error_count': error_count
                })
                
        except Exception as e:
            logger.error(f"Failed to update gateway status for {gateway_name}: {e}")
    
    async def route_order(self, order_data: Dict[str, Any]) -> Optional[Any]:
        """智能订单路由"""
        try:
            gateway_name = self.select_best_gateway(order_data['symbol'])
            gateway = self.gateways.get(gateway_name)
            
            if gateway:
                logger.info(f"Routing order for {order_data['symbol']} to {gateway_name}")
                
                # Place order through selected gateway
                if hasattr(gateway, 'place_order'):
                    result = await gateway.place_order(order_data)
                elif hasattr(gateway, 'order_manager') and hasattr(gateway.order_manager, 'place_order'):
                    result = await gateway.order_manager.place_order(order_data)
                else:
                    raise ValueError(f"Gateway {gateway_name} does not support order placement")
                
                # Record to unified order stream
                self.record_unified_order(order_data, gateway_name)
                
                return result
            else:
                raise ValueError(f"No available gateway for {order_data['symbol']}")
                
        except Exception as e:
            logger.error(f"Failed to route order: {e}")
            return None
    
    def select_best_gateway(self, symbol: str) -> str:
        """选择最佳网关"""
        try:
            # Query gateway performance from last 5 minutes
            gateway_scores = self.session.run("""
              select gateway, avg(latency) as avg_latency, sum(error_count) as total_errors
              from gateway_status 
              where timestamp > now() - 300000  // 最近5分钟
              group by gateway
              order by avg_latency asc, total_errors asc
            """)
            
            if len(gateway_scores) > 0:
                best_gateway = gateway_scores['gateway'][0]
                logger.info(f"Selected best gateway: {best_gateway} for symbol: {symbol}")
                return best_gateway
            else:
                # Fallback to first available gateway
                available_gateways = [name for name, status in self.gateway_status.items() 
                                    if status['status'] in ['CONNECTED', 'REGISTERED']]
                if available_gateways:
                    return available_gateways[0]
                else:
                    raise ValueError("No available gateways")
                    
        except Exception as e:
            logger.error(f"Error selecting best gateway: {e}")
            # Fallback to first available gateway
            if self.gateways:
                return list(self.gateways.keys())[0]
            else:
                raise ValueError("No gateways available")
    
    def record_unified_order(self, order_data: Dict[str, Any], gateway_name: str):
        """记录统一订单"""
        try:
            self.session.run(f"""
              insert into unified_order_stream values(
                now(),
                '{order_data['symbol']}',
                '{order_data['direction']}',
                {order_data.get('price', 0.0)},
                {order_data['volume']},
                'SUBMITTED',
                '{gateway_name}'
              )
            """)
            
        except Exception as e:
            logger.error(f"Failed to record unified order: {e}")
    
    def get_gateway_status(self) -> Dict[str, Any]:
        """获取所有网关状态"""
        return self.gateway_status.copy()
    
    def get_available_gateways(self) -> List[str]:
        """获取可用网关列表"""
        return [name for name, status in self.gateway_status.items() 
                if status['status'] in ['CONNECTED', 'REGISTERED']]
    
    async def start_monitoring(self):
        """启动网关监控"""
        logger.info("Starting gateway monitoring...")
        
        while self.connected:
            try:
                for gateway_name, gateway in self.gateways.items():
                    # Check gateway health
                    if hasattr(gateway, 'connected'):
                        status = 'CONNECTED' if gateway.connected else 'DISCONNECTED'
                    else:
                        status = 'UNKNOWN'
                    
                    # Update status (simplified latency calculation)
                    self.update_gateway_status(gateway_name, status, 0.0, 0)
                
                # Wait before next check
                await asyncio.sleep(30)  # Check every 30 seconds
                
            except Exception as e:
                logger.error(f"Error in gateway monitoring: {e}")
                await asyncio.sleep(5)
    
    async def shutdown(self):
        """关闭管理器"""
        try:
            logger.info("Shutting down gateway manager...")
            
            # Disconnect all gateways
            for gateway_name, gateway in self.gateways.items():
                if hasattr(gateway, 'disconnect'):
                    await gateway.disconnect()
                    logger.info(f"Disconnected gateway: {gateway_name}")
            
            # Close DolphinDB connection
            if self.connected:
                self.session.close()
                self.connected = False
                
            logger.info("Gateway manager shutdown completed")
            
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")
