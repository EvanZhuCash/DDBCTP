#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Advanced Order Management System
高级订单管理系统 - 支持订单跟踪、撤单重发、部分成交处理
"""

import queue
import threading
import time
import logging
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field
from enum import Enum
from config import ORDER_CONFIG

logger = logging.getLogger(__name__)

class OrderStatus(Enum):
    """订单状态枚举"""
    PENDING = "PENDING"
    SUBMITTED = "SUBMITTED"
    PARTIAL_FILLED = "PARTIAL_FILLED"
    FULLY_FILLED = "FULLY_FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"
    CANCEL_SUBMITTED = "CANCEL_SUBMITTED"

class OrderAction(Enum):
    """订单操作枚举"""
    CANCEL_REORDER = "CANCEL_REORDER"
    PARTIAL_CONTINUE = "PARTIAL_CONTINUE"
    RISK_CANCEL = "RISK_CANCEL"

@dataclass
class OrderInfo:
    """订单信息结构"""
    orderid: int
    symbol: str
    direction: str
    price: float
    volume: int
    filled_volume: int = 0
    remaining_volume: int = 0
    avg_fill_price: float = 0.0
    status: OrderStatus = OrderStatus.PENDING
    submit_time: float = field(default_factory=time.time)
    last_update_time: float = field(default_factory=time.time)
    retry_count: int = 0
    
    def __post_init__(self):
        self.remaining_volume = self.volume - self.filled_volume

@dataclass
class CancelReorderRequest:
    """撤单重发请求"""
    orderid: int
    symbol: str
    new_price: float
    reason: str
    original_order: OrderInfo

class OrderTracker:
    """订单跟踪器"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.orders: Dict[int, OrderInfo] = {}
        self.lock = threading.Lock()
        
    def add_order(self, order_info: OrderInfo):
        """添加订单到跟踪器"""
        with self.lock:
            self.orders[order_info.orderid] = order_info
            logger.info(f"Added order to tracker: {order_info.orderid}")
    
    def update_order(self, orderid: int, **kwargs):
        """更新订单信息"""
        with self.lock:
            if orderid in self.orders:
                order = self.orders[orderid]
                for key, value in kwargs.items():
                    if hasattr(order, key):
                        setattr(order, key, value)
                order.last_update_time = time.time()
                logger.debug(f"Updated order {orderid}: {kwargs}")
    
    def get_order(self, orderid: int) -> Optional[OrderInfo]:
        """获取订单信息"""
        with self.lock:
            return self.orders.get(orderid)
    
    def get_pending_orders(self, symbol: str = None) -> List[OrderInfo]:
        """获取待处理订单"""
        with self.lock:
            pending_orders = [
                order for order in self.orders.values()
                if order.status in [OrderStatus.SUBMITTED, OrderStatus.PARTIAL_FILLED]
            ]
            
            if symbol:
                pending_orders = [order for order in pending_orders if order.symbol == symbol]
            
            return pending_orders
    
    def remove_order(self, orderid: int):
        """移除订单"""
        with self.lock:
            if orderid in self.orders:
                del self.orders[orderid]
                logger.info(f"Removed order from tracker: {orderid}")

class OrderRiskManager:
    """订单风险管理器"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.daily_loss = 0.0
        self.order_count_per_minute = {}
        self.position_count = {}
        
    def check_order_risk(self, order_info: OrderInfo) -> bool:
        """检查订单风险"""
        # 检查日内亏损限制
        if self.daily_loss >= self.config.get("max_daily_loss", 10000):
            logger.warning(f"Daily loss limit exceeded: {self.daily_loss}")
            return False
        
        # 检查单品种持仓限制
        symbol_position = self.position_count.get(order_info.symbol, 0)
        if symbol_position >= self.config.get("max_position_per_symbol", 50):
            logger.warning(f"Position limit exceeded for {order_info.symbol}: {symbol_position}")
            return False
        
        # 检查下单频率
        current_minute = int(time.time() // 60)
        minute_count = self.order_count_per_minute.get(current_minute, 0)
        if minute_count >= self.config.get("max_order_frequency", 100):
            logger.warning(f"Order frequency limit exceeded: {minute_count}")
            return False
        
        return True
    
    def update_order_count(self):
        """更新订单计数"""
        current_minute = int(time.time() // 60)
        self.order_count_per_minute[current_minute] = self.order_count_per_minute.get(current_minute, 0) + 1
        
        # 清理旧的计数
        old_minutes = [minute for minute in self.order_count_per_minute.keys() if minute < current_minute - 5]
        for minute in old_minutes:
            del self.order_count_per_minute[minute]

class AdvancedOrderManager:
    """高级订单管理器"""
    
    def __init__(self, session, config: Dict[str, Any] = None):
        self.session = session
        self.config = config or ORDER_CONFIG
        
        self.order_tracker = OrderTracker(self.config.get("tracking", {}))
        self.risk_manager = OrderRiskManager(self.config.get("risk", {}))
        
        self.cancel_reorder_queue = queue.Queue()
        self.partial_fill_queue = queue.Queue()
        self.failed_order_queue = queue.Queue()
        
        self.stop_event = threading.Event()
        
    def initialize_ddb_functions(self):
        """初始化DolphinDB订单管理函数"""
        self.session.run("""
            // 高级订单跟踪函数
            def advanced_order_tracking(mutable tracking_config, orders_table, tick_data){
                current_time = tick_data.timestamp[0]
                current_symbol = tick_data.symbol[0] 
                current_price = tick_data.price[0]
                
                // 查找该品种的未成交订单
                pending_orders = select * from orders_table 
                    where symbol = current_symbol and status in ["SUBMITTED", "PARTIAL_FILLED"]
                
                cancel_reorder_actions = table(0:0, `orderid`action`new_price`symbol`reason`priority, 
                    [INT, SYMBOL, DOUBLE, SYMBOL, SYMBOL, INT])
                
                for(i in 0..(size(pending_orders)-1)){
                    order_id = pending_orders.orderid[i]
                    order_price = pending_orders.price[i]
                    order_time = pending_orders.timestamp[i]
                    order_direction = pending_orders.dir[i]
                    order_volume = pending_orders.vol[i]
                    
                    // 计算价格偏离度和时间差
                    price_deviation = abs(current_price - order_price) / order_price
                    time_elapsed = current_time - order_time
                    
                    should_cancel = false
                    new_price = order_price
                    reason = "NONE"
                    priority = 0
                    
                    // 价格偏离超过阈值
                    if(price_deviation > tracking_config["price_tolerance"]){
                        should_cancel = true
                        reason = "PRICE_DEVIATION"
                        priority = 1
                        if(order_direction == "LONG"){
                            new_price = current_price * 1.001  // 买单稍高于市价
                        } else {
                            new_price = current_price * 0.999  // 卖单稍低于市价
                        }
                    }
                    
                    // 时间超过阈值
                    if(time_elapsed > tracking_config["time_threshold"]){
                        should_cancel = true
                        reason = "TIME_THRESHOLD"
                        priority = 2
                        new_price = current_price  // 使用当前市价
                    }
                    
                    // 大单特殊处理
                    if(order_volume > 10){
                        if(time_elapsed > tracking_config["time_threshold"] * 0.5){
                            should_cancel = true
                            reason = "LARGE_ORDER_TIMEOUT"
                            priority = 3
                            new_price = current_price
                        }
                    }
                    
                    if(should_cancel){
                        cancel_reorder_actions.append!(table(order_id as orderid, "CANCEL_REORDER" as action, 
                            new_price as new_price, current_symbol as symbol, reason as reason, priority as priority))
                    }
                }
                
                return cancel_reorder_actions
            }
            
            // 部分成交智能处理
            def intelligent_partial_fill_handler(mutable partial_tracking, mutable config, trade_report){
                order_id = trade_report.orderid[0]
                filled_qty = trade_report.volume[0]
                fill_price = trade_report.price[0]
                symbol = trade_report.symbol[0]
                
                // 更新部分成交记录
                existing_record = select * from partial_tracking where orderid = order_id
                
                if(size(existing_record) == 0){
                    // 新的部分成交记录
                    total_vol = exec vol from orders where orderid = order_id
                    if(size(total_vol) > 0){
                        insert into partial_tracking values(
                            order_id, symbol, total_vol[0], filled_qty, total_vol[0] - filled_qty,
                            fill_price, "PARTIAL_FILLED"
                        )
                    }
                } else {
                    // 更新现有记录
                    new_filled = existing_record.filled_volume[0] + filled_qty
                    new_remaining = existing_record.total_volume[0] - new_filled
                    new_avg_price = (existing_record.avg_price[0] * existing_record.filled_volume[0] + 
                                    fill_price * filled_qty) / new_filled
                    
                    update partial_tracking set 
                        filled_volume = new_filled,
                        remaining_volume = new_remaining,
                        avg_price = new_avg_price,
                        status = iif(new_remaining == 0, "FULLY_FILLED", "PARTIAL_FILLED")
                        where orderid = order_id
                    
                    // 智能决策：是否继续追单
                    fill_rate = new_filled / existing_record.total_volume[0]
                    
                    if(new_remaining > 0){
                        if(fill_rate < 0.3){
                            // 成交率低，考虑撤单重发
                            insert into cancel_reorder_stream values(order_id, "CANCEL_REORDER", fill_price, symbol, "LOW_FILL_RATE")
                        } else if(fill_rate > 0.7){
                            // 成交率高，继续等待
                            // 不做操作
                        } else {
                            // 中等成交率，根据市场情况决定
                            current_price = exec last(price) from tickStream where symbol = symbol
                            if(size(current_price) > 0){
                                price_diff = abs(current_price[0] - fill_price) / fill_price
                                if(price_diff > 0.01){  // 价格偏离1%
                                    insert into cancel_reorder_stream values(order_id, "CANCEL_REORDER", current_price[0], symbol, "PRICE_MOVED")
                                }
                            }
                        }
                    }
                }
            }
            
            // 限价单分层下单策略
            def layered_limit_order_strategy(symbol, direction, total_volume, reference_price, config){
                max_levels = config["max_price_levels"]
                increment = config["price_increment"]
                volume_split = config["volume_split"]
                
                orders = table(0:0, `symbol`direction`price`volume`order_type`level, 
                              [SYMBOL, STRING, DOUBLE, INT, STRING, INT])
                
                for(i in 0..(max_levels-1)){
                    if(direction == "BUY"){
                        order_price = reference_price - (i * increment)
                    } else {
                        order_price = reference_price + (i * increment)
                    }
                    
                    order_volume = int(total_volume * volume_split[i])
                    
                    if(order_volume > 0){
                        orders.append!(table(symbol as symbol, direction as direction, 
                                            order_price as price, order_volume as volume, 
                                            "LIMIT" as order_type, i as level))
                    }
                }
                
                return orders
            }
        """)
    
    def start_order_management_threads(self):
        """启动订单管理线程"""
        threads = []
        
        # 撤单重发处理线程
        cancel_thread = threading.Thread(target=self._cancel_reorder_worker, daemon=True)
        threads.append(cancel_thread)
        
        # 部分成交处理线程
        partial_thread = threading.Thread(target=self._partial_fill_worker, daemon=True)
        threads.append(partial_thread)
        
        # 失败订单重试线程
        retry_thread = threading.Thread(target=self._failed_order_worker, daemon=True)
        threads.append(retry_thread)
        
        for thread in threads:
            thread.start()
        
        logger.info("Order management threads started")
        return threads
    
    def _cancel_reorder_worker(self):
        """撤单重发工作线程"""
        while not self.stop_event.is_set():
            try:
                request = self.cancel_reorder_queue.get(timeout=1)
                self._process_cancel_reorder(request)
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"Error in cancel-reorder worker: {e}")
    
    def _partial_fill_worker(self):
        """部分成交处理工作线程"""
        while not self.stop_event.is_set():
            try:
                fill_data = self.partial_fill_queue.get(timeout=1)
                self._process_partial_fill(fill_data)
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"Error in partial fill worker: {e}")
    
    def _failed_order_worker(self):
        """失败订单重试工作线程"""
        while not self.stop_event.is_set():
            try:
                failed_order = self.failed_order_queue.get(timeout=1)
                self._process_failed_order(failed_order)
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"Error in failed order worker: {e}")
    
    def _process_cancel_reorder(self, request: CancelReorderRequest):
        """处理撤单重发请求"""
        logger.info(f"Processing cancel-reorder for order {request.orderid}, reason: {request.reason}")
        
        # 更新订单状态
        self.order_tracker.update_order(request.orderid, status=OrderStatus.CANCEL_SUBMITTED)
        
        # 这里应该调用实际的撤单和重发逻辑
        # 为了演示，我们只记录日志
        logger.info(f"Would cancel order {request.orderid} and reorder at price {request.new_price}")
    
    def _process_partial_fill(self, fill_data: Dict[str, Any]):
        """处理部分成交"""
        orderid = fill_data.get('orderid')
        filled_volume = fill_data.get('volume')
        
        logger.info(f"Processing partial fill for order {orderid}, filled: {filled_volume}")
        
        # 更新订单跟踪器
        order = self.order_tracker.get_order(orderid)
        if order:
            order.filled_volume += filled_volume
            order.remaining_volume = order.volume - order.filled_volume
            
            if order.remaining_volume <= 0:
                order.status = OrderStatus.FULLY_FILLED
            else:
                order.status = OrderStatus.PARTIAL_FILLED
    
    def _process_failed_order(self, failed_order: OrderInfo):
        """处理失败订单"""
        if failed_order.retry_count < self.config.get("tracking", {}).get("max_retries", 3):
            failed_order.retry_count += 1
            logger.info(f"Retrying failed order {failed_order.orderid}, attempt {failed_order.retry_count}")
            
            # 这里应该重新发送订单
            # 为了演示，我们只记录日志
        else:
            logger.warning(f"Order {failed_order.orderid} exceeded max retries, giving up")
            self.order_tracker.update_order(failed_order.orderid, status=OrderStatus.REJECTED)
    
    def stop(self):
        """停止订单管理器"""
        self.stop_event.set()
        logger.info("Order manager stopped")

if __name__ == "__main__":
    # 测试订单管理器
    import dolphindb as ddb
    
    session = ddb.session()
    session.connect("localhost", 8848, "admin", "123456")
    
    order_manager = AdvancedOrderManager(session)
    order_manager.initialize_ddb_functions()
    
    # 创建测试订单
    test_order = OrderInfo(
        orderid=12345,
        symbol="IC2509",
        direction="LONG",
        price=5000.0,
        volume=10
    )
    
    order_manager.order_tracker.add_order(test_order)
    
    print(f"Added test order: {test_order}")
    print(f"Pending orders: {len(order_manager.order_tracker.get_pending_orders())}")
    
    session.close()
