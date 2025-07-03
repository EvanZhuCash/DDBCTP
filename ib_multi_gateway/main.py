"""
Main entry point for IB Multi-Gateway Trading System
"""

import asyncio
import logging
import signal
import sys
from typing import Dict, Any

from gateway_manager import MultiGatewayManager
from ib_gateway.ib_connector import IBGateway, IBOrderManager
from config.ib_config import IB_TEST_CONFIG
from config.crypto_config import CRYPTO_CONFIGS

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TradingSystem:
    """主交易系统类"""
    
    def __init__(self):
        self.manager = MultiGatewayManager()
        self.gateways = {}
        self.running = False
        
    async def initialize(self):
        """初始化系统"""
        try:
            logger.info("Initializing trading system...")
            
            # Initialize gateway manager
            success = await self.manager.initialize()
            if not success:
                raise Exception("Failed to initialize gateway manager")
            
            logger.info("Trading system initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize trading system: {e}")
            return False
    
    async def setup_ib_gateway(self):
        """设置IB网关"""
        try:
            logger.info("Setting up IB Gateway...")
            
            # Create IB gateway
            ib_gateway = IBGateway(IB_TEST_CONFIG)
            
            # Connect to IB
            connected = await ib_gateway.connect()
            if not connected:
                raise Exception("Failed to connect to IB")
            
            # Register with manager
            self.manager.register_gateway('IB', ib_gateway)
            self.gateways['IB'] = ib_gateway
            
            # Subscribe to market data
            await ib_gateway.subscribe_market_data(IB_TEST_CONFIG['contracts'])
            
            logger.info("IB Gateway setup completed")
            return True
            
        except Exception as e:
            logger.error(f"Failed to setup IB Gateway: {e}")
            return False
    
    async def setup_crypto_gateways(self):
        """设置加密货币网关 (占位符)"""
        logger.info("Crypto gateways setup - placeholder for future implementation")
        # TODO: Implement crypto gateway setup
        return True
    
    async def start_monitoring(self):
        """启动监控"""
        logger.info("Starting system monitoring...")
        
        # Start gateway monitoring
        monitoring_task = asyncio.create_task(self.manager.start_monitoring())
        
        return monitoring_task
    
    async def run_demo_trading(self):
        """运行演示交易"""
        logger.info("Starting demo trading...")
        
        try:
            # Wait a bit for market data
            await asyncio.sleep(10)
            
            # Place a demo order
            demo_order = {
                'symbol': 'AAPL',
                'direction': 'BUY',
                'volume': 1,
                'order_type': 'MARKET'
            }
            
            logger.info(f"Placing demo order: {demo_order}")
            result = await self.manager.route_order(demo_order)
            
            if result:
                logger.info("Demo order placed successfully")
            else:
                logger.warning("Demo order failed")
            
            # Wait and place another order
            await asyncio.sleep(30)
            
            demo_order_2 = {
                'symbol': 'SPY',
                'direction': 'SELL',
                'volume': 1,
                'order_type': 'MARKET'
            }
            
            logger.info(f"Placing second demo order: {demo_order_2}")
            result_2 = await self.manager.route_order(demo_order_2)
            
            if result_2:
                logger.info("Second demo order placed successfully")
            else:
                logger.warning("Second demo order failed")
                
        except Exception as e:
            logger.error(f"Demo trading failed: {e}")
    
    async def run(self):
        """运行主系统"""
        try:
            self.running = True
            logger.info("Starting IB Multi-Gateway Trading System...")
            
            # Initialize system
            if not await self.initialize():
                return False
            
            # Setup gateways
            if not await self.setup_ib_gateway():
                return False
            
            # Setup crypto gateways (placeholder)
            await self.setup_crypto_gateways()
            
            # Start monitoring
            monitoring_task = await self.start_monitoring()
            
            # Run demo trading
            demo_task = asyncio.create_task(self.run_demo_trading())
            
            logger.info("System is running. Press Ctrl+C to stop.")
            
            # Wait for tasks or interruption
            try:
                await asyncio.gather(monitoring_task, demo_task)
            except KeyboardInterrupt:
                logger.info("Received interrupt signal")
            
            return True
            
        except Exception as e:
            logger.error(f"System run failed: {e}")
            return False
        
        finally:
            await self.shutdown()
    
    async def shutdown(self):
        """关闭系统"""
        try:
            logger.info("Shutting down trading system...")
            self.running = False
            
            # Shutdown gateway manager
            await self.manager.shutdown()
            
            logger.info("Trading system shutdown completed")
            
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")


def signal_handler(signum, frame):
    """信号处理器"""
    logger.info(f"Received signal {signum}")
    sys.exit(0)


async def main():
    """主函数"""
    # Setup signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Create and run trading system
    system = TradingSystem()
    success = await system.run()
    
    if success:
        logger.info("Trading system completed successfully")
    else:
        logger.error("Trading system failed")
        sys.exit(1)


if __name__ == "__main__":
    print("IB Multi-Gateway Trading System")
    print("================================")
    print()
    print("Prerequisites:")
    print("1. DolphinDB server running on localhost:8848")
    print("2. TWS or IB Gateway running in paper trading mode")
    print("3. Required Python packages installed")
    print()
    
    confirm = input("Are prerequisites met? (y/n): ").strip().lower()
    if confirm == 'y':
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            print("\nSystem interrupted by user")
        except Exception as e:
            print(f"\nSystem error: {e}")
    else:
        print("Please ensure prerequisites are met before running the system.")
