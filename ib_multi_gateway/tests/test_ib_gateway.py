"""
Test script for IB Gateway functionality
Requires TWS or IB Gateway running in paper trading mode
"""

import asyncio
import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ib_gateway.ib_connector import IBGateway, IBOrderManager
from config.ib_config import IB_TEST_CONFIG
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def test_ib_connection():
    """测试IB连接"""
    logger.info("Testing IB Gateway connection...")
    
    # Create gateway instance
    gateway = IBGateway(IB_TEST_CONFIG)
    
    try:
        # Test connection
        connected = await gateway.connect()
        if not connected:
            logger.error("Failed to connect to IB")
            return False
        
        logger.info("Successfully connected to IB")
        
        # Test market data subscription
        await gateway.subscribe_market_data(IB_TEST_CONFIG['contracts'])
        
        # Wait for some market data
        logger.info("Waiting for market data... (30 seconds)")
        await asyncio.sleep(30)
        
        # Test order placement (paper trading only)
        order_manager = IBOrderManager(gateway)
        
        test_order = {
            'symbol': 'AAPL',
            'direction': 'BUY',
            'volume': 1,
            'order_type': 'MARKET'
        }
        
        logger.info("Placing test order...")
        trade = await order_manager.place_order(test_order)
        
        if trade:
            logger.info(f"Test order placed successfully: {trade}")
        else:
            logger.warning("Test order failed")
        
        # Wait a bit more to see order status
        await asyncio.sleep(10)
        
        return True
        
    except Exception as e:
        logger.error(f"Test failed: {e}")
        return False
        
    finally:
        # Cleanup
        await gateway.disconnect()


async def test_market_data_only():
    """仅测试市场数据接收"""
    logger.info("Testing market data reception only...")
    
    gateway = IBGateway(IB_TEST_CONFIG)
    
    try:
        # Connect
        connected = await gateway.connect()
        if not connected:
            logger.error("Failed to connect to IB")
            return False
        
        # Subscribe to market data
        await gateway.subscribe_market_data(IB_TEST_CONFIG['contracts'])
        
        # Wait for market data
        logger.info("Receiving market data for 60 seconds...")
        await asyncio.sleep(60)
        
        logger.info("Market data test completed")
        return True
        
    except Exception as e:
        logger.error(f"Market data test failed: {e}")
        return False
        
    finally:
        await gateway.disconnect()


def check_dolphindb_data():
    """检查DolphinDB中的数据"""
    import dolphindb as ddb
    
    try:
        session = ddb.session()
        session.connect("localhost", 8848, "admin", "123456")
        
        # Check tick data
        tick_count = session.run("select count(*) from ib_tick_stream")
        logger.info(f"Total tick records in DolphinDB: {tick_count}")
        
        if tick_count > 0:
            # Show latest ticks
            latest_ticks = session.run("select top 10 * from ib_tick_stream order by timestamp desc")
            logger.info(f"Latest ticks:\n{latest_ticks}")
        
        # Check order data
        order_count = session.run("select count(*) from ib_order_stream")
        logger.info(f"Total order records in DolphinDB: {order_count}")
        
        if order_count > 0:
            latest_orders = session.run("select * from ib_order_stream order by timestamp desc")
            logger.info(f"Latest orders:\n{latest_orders}")
        
        session.close()
        
    except Exception as e:
        logger.error(f"Failed to check DolphinDB data: {e}")


async def main():
    """主测试函数"""
    logger.info("Starting IB Gateway tests...")
    
    print("\nAvailable tests:")
    print("1. Full test (connection + market data + orders)")
    print("2. Market data only")
    print("3. Check DolphinDB data")
    
    choice = input("\nSelect test (1-3): ").strip()
    
    if choice == "1":
        success = await test_ib_connection()
        if success:
            logger.info("Full test completed successfully")
        else:
            logger.error("Full test failed")
            
    elif choice == "2":
        success = await test_market_data_only()
        if success:
            logger.info("Market data test completed successfully")
        else:
            logger.error("Market data test failed")
            
    elif choice == "3":
        check_dolphindb_data()
        
    else:
        logger.error("Invalid choice")


if __name__ == "__main__":
    # Check prerequisites
    print("Prerequisites:")
    print("1. DolphinDB server running on localhost:8848")
    print("2. TWS or IB Gateway running in paper trading mode")
    print("3. ib_insync library installed: pip install ib_insync")
    print("4. dolphindb library installed: pip install dolphindb")
    
    confirm = input("\nAre prerequisites met? (y/n): ").strip().lower()
    if confirm == 'y':
        asyncio.run(main())
    else:
        print("Please ensure prerequisites are met before running tests.")
