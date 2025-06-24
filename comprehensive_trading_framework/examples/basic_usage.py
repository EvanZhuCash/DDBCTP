#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Basic Usage Example for Comprehensive Trading Framework
综合交易框架基础使用示例
"""

import sys
import os
import time
import logging

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from comprehensive_trading_system import ComprehensiveTradingSystem, CTGateway
from strategy_factory import create_strategy_factory
from order_management import AdvancedOrderManager
from config import get_config

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def basic_trading_example():
    """基础交易示例"""
    logger.info("Starting basic trading example")
    
    try:
        # 1. 初始化交易系统
        logger.info("Initializing trading system...")
        trading_system = ComprehensiveTradingSystem()
        trading_system.initialize_system()
        
        # 2. 创建策略工厂并加载策略
        logger.info("Loading strategies...")
        strategy_factory = create_strategy_factory()
        enabled_strategies = strategy_factory.get_enabled_strategies()
        
        logger.info(f"Loaded {len(enabled_strategies)} strategies:")
        for strategy in enabled_strategies:
            logger.info(f"  - {strategy.name} ({strategy.__class__.__name__})")
        
        # 3. 初始化订单管理器
        logger.info("Initializing order manager...")
        order_manager = AdvancedOrderManager(trading_system.session)
        order_manager.initialize_ddb_functions()
        
        # 4. 启动订单管理线程
        logger.info("Starting order management threads...")
        order_threads = order_manager.start_order_management_threads()
        
        # 5. 初始化CTP网关 (模拟)
        logger.info("Initializing CTP gateway...")
        gateway = CTGateway(trading_system.session)
        ctp_config = get_config("ctp")
        gateway.connect(ctp_config)
        
        # 6. 订阅行情数据
        symbols = ["IC2509", "IF2509", "IH2509"]
        logger.info(f"Subscribing to symbols: {symbols}")
        for symbol in symbols:
            gateway.subscribe(symbol)
        
        # 7. 订阅订单流
        logger.info("Subscribing to order streams...")
        trading_system.subscribe_order_streams()
        
        # 8. 模拟运行
        logger.info("System is running. Simulating for 30 seconds...")
        
        # 模拟发送一些tick数据
        simulate_market_data(trading_system.session, symbols)
        
        # 运行30秒
        time.sleep(30)
        
        logger.info("Example completed successfully")
        
    except Exception as e:
        logger.error(f"Error in basic trading example: {e}")
        raise
    
    finally:
        # 清理资源
        logger.info("Cleaning up resources...")
        if 'order_manager' in locals():
            order_manager.stop()
        if 'gateway' in locals():
            gateway.close()
        if 'trading_system' in locals():
            trading_system.session.close()

def simulate_market_data(session, symbols):
    """模拟市场数据"""
    logger.info("Starting market data simulation...")
    
    import threading
    import random
    
    def send_tick_data():
        base_prices = {"IC2509": 5000, "IF2509": 4000, "IH2509": 3000}
        
        for i in range(100):  # 发送100个tick
            for symbol in symbols:
                base_price = base_prices[symbol]
                price = base_price + random.uniform(-50, 50)
                volume = random.randint(1, 100)
                
                # 发送tick数据到DolphinDB
                session.run(f"""
                    insert into tickStream values(`{symbol}, now(), {price}, {volume})
                """)
            
            time.sleep(0.1)  # 100ms间隔
    
    # 在后台线程中发送数据
    data_thread = threading.Thread(target=send_tick_data, daemon=True)
    data_thread.start()

def strategy_performance_example():
    """策略性能示例"""
    logger.info("Starting strategy performance example")
    
    try:
        # 初始化系统
        trading_system = ComprehensiveTradingSystem()
        trading_system.initialize_system()
        
        # 查询系统状态
        logger.info("Checking system status...")
        
        # 检查流表状态
        tables_info = trading_system.session.run("""
            tables = ["tickStream", "agg1min", "bollinger_stream", "orderStream", "positionTable"]
            result = table(tables as table_name, 0 as row_count)
            for(i in 0..(size(tables)-1)){
                table_name = tables[i]
                try{
                    count = exec count(*) from objByName(table_name)
                    update result set row_count = count[0] where table_name = table_name
                } catch(ex){
                    update result set row_count = -1 where table_name = table_name
                }
            }
            select * from result
        """)
        
        logger.info("Stream tables status:")
        for row in tables_info:
            logger.info(f"  {row['table_name']}: {row['row_count']} rows")
        
        # 模拟一些交易活动
        logger.info("Simulating trading activity...")
        simulate_trading_activity(trading_system.session)
        
        # 等待处理
        time.sleep(5)
        
        # 再次检查状态
        tables_info = trading_system.session.run("""
            tables = ["tickStream", "agg1min", "bollinger_stream", "orderStream", "positionTable"]
            result = table(tables as table_name, 0 as row_count)
            for(i in 0..(size(tables)-1)){
                table_name = tables[i]
                try{
                    count = exec count(*) from objByName(table_name)
                    update result set row_count = count[0] where table_name = table_name
                } catch(ex){
                    update result set row_count = -1 where table_name = table_name
                }
            }
            select * from result
        """)
        
        logger.info("Stream tables status after simulation:")
        for row in tables_info:
            logger.info(f"  {row['table_name']}: {row['row_count']} rows")
        
    except Exception as e:
        logger.error(f"Error in strategy performance example: {e}")
        raise
    
    finally:
        if 'trading_system' in locals():
            trading_system.session.close()

def simulate_trading_activity(session):
    """模拟交易活动"""
    # 发送一些tick数据
    session.run("""
        // 插入模拟tick数据
        symbols = ["IC2509", "IF2509", "IH2509"]
        for(i in 0..99){
            for(j in 0..(size(symbols)-1)){
                symbol = symbols[j]
                price = 5000 + rand(-100..100, 1)[0]
                volume = rand(1..100, 1)[0]
                insert into tickStream values(symbol, now(), price, volume)
            }
            sleep(10)  // 10ms间隔
        }
    """)

def order_management_example():
    """订单管理示例"""
    logger.info("Starting order management example")
    
    try:
        # 初始化系统
        trading_system = ComprehensiveTradingSystem()
        trading_system.initialize_system()
        
        # 初始化订单管理器
        order_manager = AdvancedOrderManager(trading_system.session)
        order_manager.initialize_ddb_functions()
        
        # 模拟一些订单
        logger.info("Simulating orders...")
        
        from order_management import OrderInfo, OrderStatus
        
        # 创建测试订单
        test_orders = [
            OrderInfo(orderid=1001, symbol="IC2509", direction="LONG", price=5000.0, volume=10),
            OrderInfo(orderid=1002, symbol="IF2509", direction="SHORT", price=4000.0, volume=5),
            OrderInfo(orderid=1003, symbol="IH2509", direction="LONG", price=3000.0, volume=8)
        ]
        
        # 添加到跟踪器
        for order in test_orders:
            order_manager.order_tracker.add_order(order)
            logger.info(f"Added order: {order.orderid} {order.symbol} {order.direction} {order.volume}@{order.price}")
        
        # 模拟订单状态更新
        logger.info("Simulating order updates...")
        
        # 模拟部分成交
        order_manager.order_tracker.update_order(1001, status=OrderStatus.PARTIAL_FILLED, filled_volume=3)
        order_manager.order_tracker.update_order(1002, status=OrderStatus.FULLY_FILLED, filled_volume=5)
        
        # 检查订单状态
        pending_orders = order_manager.order_tracker.get_pending_orders()
        logger.info(f"Pending orders: {len(pending_orders)}")
        
        for order in pending_orders:
            logger.info(f"  Order {order.orderid}: {order.status.value}, filled: {order.filled_volume}/{order.volume}")
        
        # 测试风险检查
        logger.info("Testing risk checks...")
        
        for order in test_orders:
            risk_ok = order_manager.risk_manager.check_order_risk(order)
            logger.info(f"Risk check for order {order.orderid}: {'PASS' if risk_ok else 'FAIL'}")
        
    except Exception as e:
        logger.error(f"Error in order management example: {e}")
        raise
    
    finally:
        if 'trading_system' in locals():
            trading_system.session.close()

if __name__ == "__main__":
    print("Comprehensive Trading Framework - Basic Usage Examples")
    print("=" * 60)
    
    examples = {
        "1": ("Basic Trading Example", basic_trading_example),
        "2": ("Strategy Performance Example", strategy_performance_example),
        "3": ("Order Management Example", order_management_example)
    }
    
    print("Available examples:")
    for key, (name, _) in examples.items():
        print(f"  {key}. {name}")
    
    choice = input("\nSelect example to run (1-3, or 'all'): ").strip()
    
    if choice == "all":
        for name, func in examples.values():
            print(f"\n{'='*20} {name} {'='*20}")
            try:
                func()
            except Exception as e:
                logger.error(f"Example failed: {e}")
            print(f"{'='*60}")
    elif choice in examples:
        name, func = examples[choice]
        print(f"\nRunning: {name}")
        func()
    else:
        print("Invalid choice. Exiting.")
