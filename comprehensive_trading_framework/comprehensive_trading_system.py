#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Comprehensive Trading Framework
基于DolphinDB的策略执行分离和复杂订单管理系统

Features:
- Strategy execution separation (factor_calc + execution_engine)
- Complex order management (tracking, cancel-reorder, partial fills)
- Multi-timeframe support
- Cross-sectional strategies
- Risk management
"""

import dolphindb as ddb
import queue
import threading
import time
import sys
import logging
from threading import Event
from dataclasses import dataclass
from typing import Dict, Any, Optional
from vnpy_ctp import (
    MdApi,
    TdApi,
    THOST_FTDC_D_Buy,
    THOST_FTDC_D_Sell,
    THOST_FTDC_OF_Open,
    THOST_FTDC_OF_Close,
    THOST_FTDC_OF_CloseToday,
    THOST_FTDC_OF_CloseYesterday,
    THOST_FTDC_OPT_LimitPrice,
    THOST_FTDC_AF_Delete
)

# Configuration
CTP_SETTING = {
    "用户名": "239344",
    "密码": "xyzhao@3026528", 
    "经纪商代码": "9999",
    "交易服务器": "180.168.146.187:10201",
    "行情服务器": "180.168.146.187:10211",
    "产品名称": "simnow_client_test",
    "授权编码": "0000000000000000"
}

# Global queues and events
order_queue = queue.Queue()
cancel_reorder_queue = queue.Queue()
order_fail_queue = queue.Queue()
stop_event = Event()

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class OrderRequest:
    """订单请求结构"""
    symbol: str
    exchange: str
    direction: str
    type_: str
    volume: int
    price: float
    offset: str
    orderid: Optional[int] = None

class ComprehensiveTradingSystem:
    """综合交易系统主类"""
    
    def __init__(self, host="localhost", port=8848, username="admin", password="123456"):
        """初始化交易系统"""
        self.session = ddb.session()
        self.session.connect(host, port, username, password)
        self.order_queue_manager = OrderQueueManager()
        
    def create_stream_tables(self):
        """创建流表 - 支持策略执行分离架构"""
        self.session.run("""
            // 基础数据流表
            share streamTable(1000:0, `symbol`timestamp`price`vol, [SYMBOL, TIMESTAMP, DOUBLE, INT]) as tickStream
            share streamTable(1000:0, `timestamp`symbol`price, [TIMESTAMP, SYMBOL, DOUBLE]) as agg1min
            share streamTable(1000:0, `timestamp`symbol`price, [TIMESTAMP, SYMBOL, DOUBLE]) as agg5min
            share streamTable(1000:0, `timestamp`symbol`price, [TIMESTAMP, SYMBOL, DOUBLE]) as agg15min
            
            // 因子流表 - 通用因子计算结果
            share streamTable(1000:0, `symbol`timestamp`price`factor_type`factor_value`signal_type`signal_value, 
                [SYMBOL, TIMESTAMP, DOUBLE, SYMBOL, DOUBLE, SYMBOL, INT]) as factor_stream
            
            // 布林带专用表
            share streamTable(1000:0, `symbol`timestamp`price`bb_upper`bb_middle`bb_lower`long_signal`short_signal`close_signal, 
                [SYMBOL, TIMESTAMP, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT]) as bollinger_stream
            
            // 交易相关流表
            share streamTable(1000:0, `orderID`symbol`timestamp`orderPrice`orderVolume`direction, 
                [INT, SYMBOL, TIMESTAMP, DOUBLE, INT, SYMBOL]) as orderStream
            share streamTable(1000:0, `symbol`timestamp`price`qty`dir`offset`exchange, 
                [SYMBOL, TIMESTAMP, DOUBLE, INT, SYMBOL, SYMBOL, SYMBOL]) as trades
            share streamTable(1000:0, `orderid`timestamp`symbol`exchange`direction`offset`status`volume`traded, 
                [INT, TIMESTAMP, SYMBOL, SYMBOL, SYMBOL, SYMBOL, SYMBOL, INT, INT]) as positions_orders
            
            // 订单管理流表
            share streamTable(1000:0, `orderid`action`new_price`symbol`reason, 
                [INT, SYMBOL, DOUBLE, SYMBOL, SYMBOL]) as cancel_reorder_stream
            share streamTable(1000:0, `orderid`symbol`total_volume`filled_volume`remaining_volume`avg_price`status, 
                [INT, SYMBOL, INT, INT, INT, DOUBLE, SYMBOL]) as partial_fill_tracking
            
            // 持仓和盈亏表
            share streamTable(1:0, `symbol`timestamp`price`pnl`dir, 
                [SYMBOL, TIMESTAMP, DOUBLE, DOUBLE, SYMBOL]) as unrealized_positions
            share streamTable(1:0, `symbol`timestamp_close`price_open`price_close`qty`pnl, 
                [SYMBOL, TIMESTAMP, DOUBLE, DOUBLE, INT, DOUBLE]) as realized_positions
            
            // 横截面策略表
            share streamTable(1000:0, `timestamp`symbol`rank_score`selected, 
                [TIMESTAMP, SYMBOL, DOUBLE, INT]) as cross_section_table
        """)
        
    def create_keyed_tables(self):
        """创建键值表"""
        self.session.run("""
            // 持仓表
            share keyedTable(`symbol`dir, 1:0, `symbol`qty`price`dir, [SYMBOL, INT, DOUBLE, SYMBOL]) as positionTable
            
            // 订单表
            share keyedTable(`orderid, 1:0, `orderid`symbol`timestamp`price`vol`dir`offset`status`type, 
                [INT,SYMBOL,TIMESTAMP,DOUBLE,INT,SYMBOL,SYMBOL,SYMBOL,SYMBOL]) as orders
            
            // 持仓详情表
            share keyedTable(`symbol, 1000:0, `timestamp`symbol`long_pos`long_yd`long_td`short_pos`short_yd`short_td`long_pos_frozen`long_yd_frozen`long_td_frozen`short_pos_frozen`short_yd_frozen`short_td_frozen, 
                [TIMESTAMP,SYMBOL,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT]) as positionholding
        """)
        
    def factor_calc(self):
        """通用因子计算引擎 - 支持多策略多时间间隔"""
        self.session.run("""
            // 创建多时间间隔聚合引擎
            engine1min = createTimeSeriesEngine(name="engine1min", 
                windowSize=60000, step=60000, metrics=<[last(price)]>,
                dummyTable=tickStream, outputTable=agg1min, 
                timeColumn="timestamp", useSystemTime=false, keyColumn="symbol")
            
            engine5min = createTimeSeriesEngine(name="engine5min", 
                windowSize=300000, step=300000, metrics=<[last(price)]>,
                dummyTable=tickStream, outputTable=agg5min, 
                timeColumn="timestamp", useSystemTime=false, keyColumn="symbol")
            
            engine15min = createTimeSeriesEngine(name="engine15min", 
                windowSize=900000, step=900000, metrics=<[last(price)]>,
                dummyTable=tickStream, outputTable=agg15min, 
                timeColumn="timestamp", useSystemTime=false, keyColumn="symbol")
            
            // 布林带策略因子计算
            use ta
            bollinger_metrics = <[
                timestamp, price,
                ta::bBands(price, timePeriod=20, nbdevUp=2, nbdevDn=2, maType=0)[0] as `bb_upper,
                ta::bBands(price, timePeriod=20, nbdevUp=2, nbdevDn=2, maType=0)[1] as `bb_middle,
                ta::bBands(price, timePeriod=20, nbdevUp=2, nbdevDn=2, maType=0)[2] as `bb_lower,
                // 信号计算
                iif(price < ta::bBands(price, timePeriod=20, nbdevUp=2, nbdevDn=2, maType=0)[2], 1, 0) as `long_signal,
                iif(price > ta::bBands(price, timePeriod=20, nbdevUp=2, nbdevDn=2, maType=0)[0], 1, 0) as `short_signal,
                iif(abs(price - ta::bBands(price, timePeriod=20, nbdevUp=2, nbdevDn=2, maType=0)[1]) < 0.01, 1, 0) as `close_signal
            ]>
            
            createReactiveStateEngine(name="bollingerEngine",
                metrics=bollinger_metrics, dummyTable=agg1min, 
                outputTable=bollinger_stream, keyColumn="symbol", keepOrder=true)
            
            // 订阅设置
            subscribeTable(tableName=`tickStream, actionName="engine1min", handler=tableInsert{engine1min}, msgAsTable=true, offset=-1)
            subscribeTable(tableName=`tickStream, actionName="engine5min", handler=tableInsert{engine5min}, msgAsTable=true, offset=-1)
            subscribeTable(tableName=`tickStream, actionName="engine15min", handler=tableInsert{engine15min}, msgAsTable=true, offset=-1)
            subscribeTable(tableName=`agg1min, actionName="bollingerEngine", handler=getStreamEngine("bollingerEngine"), msgAsTable=true, offset=-1)
        """)
        
    def strategy_execution_engine(self):
        """策略执行引擎 - 分离的执行层"""
        self.session.run("""
            posDict = dict(SYMBOL, STRING)  // 持仓状态字典
            
            def execution_engine(mutable posDict, symbol, long_signal, short_signal, close_signal, price, timestamp){
                current_pos = posDict.get(symbol[0], "FLAT")
                
                // 根据当前持仓和信号决定是否执行交易
                if (current_pos == "FLAT"){
                    if (long_signal[0] == 1){
                        insert into orderStream values(rand(1..10000, 1)[0], symbol[0], timestamp[0], price[0], 5, "OPEN_LONG")
                        posDict[symbol[0]] = "LONG"
                    } else if (short_signal[0] == 1){
                        insert into orderStream values(rand(1..10000, 1)[0], symbol[0], timestamp[0], price[0], 5, "OPEN_SHORT")
                        posDict[symbol[0]] = "SHORT"
                    }
                } else if (current_pos == "LONG"){
                    if (close_signal[0] == 1){
                        pos_qty = exec qty from positionTable where symbol = symbol[0] and dir = "LONG"
                        if(size(pos_qty) > 0){
                            insert into orderStream values(rand(1..10000, 1)[0], symbol[0], timestamp[0], price[0], pos_qty[0], "CLOSE_LONG")
                            posDict[symbol[0]] = "FLAT"
                        }
                    }
                } else if (current_pos == "SHORT"){
                    if (close_signal[0] == 1){
                        pos_qty = exec qty from positionTable where symbol = symbol[0] and dir = "SHORT"
                        if(size(pos_qty) > 0){
                            insert into orderStream values(rand(1..10000, 1)[0], symbol[0], timestamp[0], price[0], pos_qty[0], "CLOSE_SHORT")
                            posDict[symbol[0]] = "FLAT"
                        }
                    }
                }
            }
            
            createReactiveStateEngine(name="executionEngine",
                metrics=<[execution_engine(posDict, symbol, long_signal, short_signal, close_signal, price, timestamp)]>,
                dummyTable=bollinger_stream, outputTable=orderStream, keyColumn="symbol")
            
            subscribeTable(tableName=`bollinger_stream, actionName="executionEngine", 
                handler=getStreamEngine("executionEngine"), msgAsTable=true, offset=-1)
        """)
        
    def order_tracking_system(self):
        """订单跟踪系统 - 复杂订单管理"""
        self.session.run("""
            // 订单跟踪配置
            tracking_config = dict(STRING, DOUBLE)
            tracking_config["price_tolerance"] = 0.02  // 价格容忍度2%
            tracking_config["time_threshold"] = 30000  // 30秒未成交则考虑撤单
            
            def order_tracking_logic(mutable tracking_config, orders_table, tick_data){
                current_time = tick_data.timestamp[0]
                current_symbol = tick_data.symbol[0] 
                current_price = tick_data.price[0]
                
                // 查找该品种的未成交订单
                pending_orders = select * from orders_table 
                    where symbol = current_symbol and status in ["SUBMITTED", "PARTIAL_FILLED"]
                
                cancel_reorder_actions = table(0:0, `orderid`action`new_price`symbol`reason, [INT, SYMBOL, DOUBLE, SYMBOL, SYMBOL])
                
                for(i in 0..(size(pending_orders)-1)){
                    order_id = pending_orders.orderid[i]
                    order_price = pending_orders.price[i]
                    order_time = pending_orders.timestamp[i]
                    order_direction = pending_orders.dir[i]
                    
                    // 计算价格偏离度和时间差
                    price_deviation = abs(current_price - order_price) / order_price
                    time_elapsed = current_time - order_time
                    
                    should_cancel = false
                    new_price = order_price
                    reason = "NONE"
                    
                    // 价格偏离超过阈值
                    if(price_deviation > tracking_config["price_tolerance"]){
                        should_cancel = true
                        reason = "PRICE_DEVIATION"
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
                        new_price = current_price  // 使用当前市价
                    }
                    
                    if(should_cancel){
                        cancel_reorder_actions.append!(table(order_id as orderid, "CANCEL_REORDER" as action, 
                            new_price as new_price, current_symbol as symbol, reason as reason))
                    }
                }
                
                return cancel_reorder_actions
            }
            
            // 创建订单跟踪引擎
            createReactiveStateEngine(name="orderTrackingEngine",
                metrics=<[order_tracking_logic(tracking_config, orders, tickStream)]>,
                dummyTable=tickStream, outputTable=cancel_reorder_stream, keyColumn="symbol")
            
            subscribeTable(tableName=`tickStream, actionName="orderTrackingEngine",
                handler=getStreamEngine("orderTrackingEngine"), msgAsTable=true, offset=-1)
        """)

    def partial_fill_handler(self):
        """部分成交处理系统"""
        self.session.run("""
            def handle_partial_fill(mutable partial_tracking, trade_report){
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
                }
            }

            subscribeTable(tableName=`trades, actionName="partialFillHandler",
                handler=handle_partial_fill{partial_fill_tracking}, msgAsTable=true, offset=-1)
        """)

    def positions_management(self):
        """持仓管理系统 - FIFO方法"""
        self.session.run("""
            positions = dict(SYMBOL, ANY)

            def updatePosition(mutable positions, mutable positionTable, mutable realized_positions, msg){
                sym = msg.symbol[0]
                vol = msg.qty[0]
                timestamp = msg.timestamp[0]
                price = msg.price[0]
                dir = msg.dir[0]
                offset = msg.offset[0]
                key = sym + "_" + dir

                if (!(key in positions.keys())){
                    positions[key] = table(1:0, `time1`vol1`price1, [TIMESTAMP, INT, DOUBLE])
                }

                if (offset == "OPEN"){
                    // 开仓：加入队列
                    positions[key] = positions[key].append!(table(timestamp as time1, vol as vol1, price as price1))
                } else if (offset == "CLOSE"){
                    // 平仓：FIFO出队列
                    pre_dir = iif(dir == "LONG", "SHORT", "LONG")
                    pre_key = sym + "_" + pre_dir
                    queue = positions[pre_key]
                    remaining = vol
                    cost = 0.0
                    closedVol = 0
                    pnl = 0.0
                    irow = 0

                    do {
                        if(irow >= size(queue)) break
                        qVol = queue.vol1[irow]
                        qPrice = queue.price1[irow]

                        if (qVol <= remaining){
                            cost += qVol * qPrice
                            remaining -= qVol
                            closedVol += qVol
                            irow += 1
                        } else {
                            cost += remaining * qPrice
                            update positions[pre_key] set vol1 = vol1 - remaining where time1 = time1[irow]
                            closedVol += remaining
                            remaining = 0
                        }
                    } while (remaining > 0 && irow < size(queue))

                    if (irow > 0){
                        positions[pre_key] = positions[pre_key].slice(irow:)
                    }

                    if(closedVol > 0){
                        avg_open = cost / closedVol
                        pnl = iif(dir == "LONG", closedVol * (price - avg_open), closedVol * (avg_open - price))
                        insert into realized_positions values(sym, timestamp, avg_open, price, closedVol, pnl)
                    }
                }

                // 更新持仓表
                for(pos_key in positions.keys()){
                    if(startsWith(pos_key, sym + "_")){
                        queue = positions[pos_key]
                        if (size(queue) > 0) {
                            totalVol = sum(queue.vol1)
                            totalCost = sum(queue.vol1 * queue.price1)
                            avgPrice = totalCost / totalVol
                            sym_part, dir_part = split(pos_key, "_")
                            insert into positionTable values(sym_part, totalVol, avgPrice, dir_part)
                        }
                    }
                }
            }

            // 未实现盈亏计算
            def pnl_calc(cur_price, cost_price, vol, dir){
                return iif(dir=="LONG", vol*(cur_price-cost_price), vol*(cost_price-cur_price))
            }

            LjEngine = createLookupJoinEngine(name="position_lookforjoin",
                leftTable=tickStream, rightTable=positionTable, outputTable=unrealized_positions,
                metrics=<[tickStream.timestamp, tickStream.price, pnl_calc(tickStream.price, positionTable.price, qty, dir), dir]>,
                matchingColumn=`symbol, checkTimes=1000)

            subscribeTable(tableName="trades", actionName="trade_positions", offset=-1,
                handler=updatePosition{positions, positionTable, realized_positions}, msgAsTable=true)
            subscribeTable(tableName="tickStream", actionName="unrealized_position", offset=-1,
                handler=appendForJoin{LjEngine, true}, msgAsTable=true)
        """)

    def cross_section_strategy(self):
        """横截面策略支持"""
        self.session.run("""
            cross_section_config = dict(STRING, INT)
            cross_section_config["top_n"] = 3
            cross_section_config["bottom_n"] = 3
            cross_section_config["rebalance_freq"] = 300000  // 5分钟重新平衡

            def cross_section_calc(mutable config, timestamp, symbols, factor_values){
                // 对所有品种按因子值排序
                factor_data = table(symbols as symbol, factor_values as factor_value)
                ranked_data = select symbol, factor_value,
                    rank(factor_value, false) as rank_score
                    from factor_data
                    order by factor_value desc

                top_n = config["top_n"]
                bottom_n = config["bottom_n"]
                total_count = size(ranked_data)

                // 选择前N和后N的品种
                selected_long = select symbol, rank_score, 1 as selected
                    from ranked_data where rank_score <= top_n
                selected_short = select symbol, rank_score, -1 as selected
                    from ranked_data where rank_score > (total_count - bottom_n)

                result = select timestamp[0] as timestamp, symbol, rank_score, selected
                    from unionAll(selected_long, selected_short)

                return result
            }

            // 定时触发横截面计算 (这里简化为基于数据触发)
            createReactiveStateEngine(name="crossSectionEngine",
                metrics=<[cross_section_calc(cross_section_config, timestamp, symbol, price)]>,
                dummyTable=agg5min, outputTable=cross_section_table, keyColumn="symbol")

            subscribeTable(tableName=`agg5min, actionName="crossSectionEngine",
                handler=getStreamEngine("crossSectionEngine"), msgAsTable=true, offset=-1)
        """)

    def initialize_system(self):
        """初始化完整交易系统"""
        logger.info("Initializing comprehensive trading system...")

        # 启用流计算
        self.session.enableStreaming()

        # 创建表结构
        self.create_stream_tables()
        self.create_keyed_tables()

        # 初始化各个模块
        self.factor_calc()
        self.strategy_execution_engine()
        self.order_tracking_system()
        self.partial_fill_handler()
        self.positions_management()
        self.cross_section_strategy()

        logger.info("Trading system initialized successfully")

    def subscribe_order_streams(self):
        """订阅订单流"""
        self.session.subscribe("localhost", 8848, self._order_handler, "orderStream", offset=-1, throttle=0.1)
        self.session.subscribe("localhost", 8848, self._cancel_reorder_handler, "cancel_reorder_stream", offset=-1, throttle=0.1)

    def _order_handler(self, msg):
        """订单处理器"""
        order_queue.put(msg)

    def _cancel_reorder_handler(self, msg):
        """撤单重发处理器"""
        cancel_reorder_queue.put(msg)

class OrderQueueManager:
    """订单队列管理器 - 处理订单时序问题"""

    def __init__(self):
        self.pending_orders = queue.Queue()
        self.processing_orders = {}
        self.lock = threading.Lock()

    def add_order_request(self, order_request):
        """添加订单请求到队列"""
        with self.lock:
            self.pending_orders.put(order_request)

    def process_order_queue(self, gateway):
        """处理订单队列"""
        while not stop_event.is_set():
            try:
                order_request = self.pending_orders.get(timeout=1)

                # 检查订单状态同步
                sync_result = self.check_order_sync(order_request)

                if sync_result['action'] == 'PROCEED':
                    self.execute_order(gateway, order_request)
                elif sync_result['action'] == 'DELAY':
                    # 延迟后重新加入队列
                    time.sleep(sync_result['delay_ms'] / 1000)
                    self.add_order_request(order_request)

            except queue.Empty:
                continue

    def check_order_sync(self, order_request):
        """检查订单状态同步"""
        # 简化的同步检查逻辑
        return {'action': 'PROCEED', 'delay_ms': 0}

    def execute_order(self, gateway, order_request):
        """执行订单"""
        try:
            gateway.send_order(order_request)
            logger.info(f"Order sent: {order_request.symbol} {order_request.direction} {order_request.volume}@{order_request.price}")
        except Exception as e:
            logger.error(f"Failed to send order: {e}")
            order_fail_queue.put(order_request)

class CTGateway:
    """CTP网关 - 简化版本"""

    def __init__(self, session):
        self.session = session
        self.md_api = None
        self.td_api = None
        self.connect_status = False

    def connect(self, setting):
        """连接CTP"""
        logger.info("Connecting to CTP...")
        # 这里应该实现完整的CTP连接逻辑
        # 为了简化，我们假设连接成功
        self.connect_status = True
        logger.info("CTP connected successfully")

    def send_order(self, order_request):
        """发送订单"""
        if not self.connect_status:
            raise Exception("CTP not connected")

        # 这里应该实现实际的CTP订单发送逻辑
        logger.info(f"Sending order: {order_request}")

    def subscribe(self, symbol):
        """订阅行情"""
        logger.info(f"Subscribing to {symbol}")

    def close(self):
        """关闭连接"""
        self.connect_status = False
        logger.info("CTP connection closed")

def order_processing_thread(gateway, order_queue_manager):
    """订单处理线程"""
    logger.info("Starting order processing thread")
    order_queue_manager.process_order_queue(gateway)
    logger.info("Order processing thread stopped")

def cancel_reorder_thread(gateway):
    """撤单重发处理线程"""
    logger.info("Starting cancel-reorder thread")

    while not stop_event.is_set():
        try:
            cancel_data = cancel_reorder_queue.get(timeout=1)

            order_id = cancel_data['orderid']
            action = cancel_data['action']
            new_price = cancel_data['new_price']
            symbol = cancel_data['symbol']
            reason = cancel_data['reason']

            if action == "CANCEL_REORDER":
                logger.info(f"Processing cancel-reorder for order {order_id}, reason: {reason}")

                # 1. 执行撤单
                # cancel_success = gateway.cancel_order(order_id)

                # 2. 发送新订单 (简化逻辑)
                # new_order = OrderRequest(...)
                # gateway.send_order(new_order)

        except queue.Empty:
            continue
        except Exception as e:
            logger.error(f"Error in cancel-reorder thread: {e}")

    logger.info("Cancel-reorder thread stopped")

def failed_order_retry_thread(gateway):
    """失败订单重试线程"""
    logger.info("Starting failed order retry thread")

    while not stop_event.is_set():
        try:
            failed_order = order_fail_queue.get(timeout=1)

            # 等待一段时间后重试
            time.sleep(1)

            logger.info(f"Retrying failed order: {failed_order}")
            gateway.send_order(failed_order)

        except queue.Empty:
            continue
        except Exception as e:
            logger.error(f"Error in failed order retry thread: {e}")

    logger.info("Failed order retry thread stopped")

def main():
    """主函数 - 启动综合交易系统"""
    logger.info("Starting Comprehensive Trading Framework")

    try:
        # 初始化交易系统
        trading_system = ComprehensiveTradingSystem()
        trading_system.initialize_system()

        # 订阅订单流
        trading_system.subscribe_order_streams()

        # 初始化CTP网关
        gateway = CTGateway(trading_system.session)
        gateway.connect(CTP_SETTING)

        # 订阅行情
        symbols = ["IC2509", "IF2509", "IH2509"]  # 示例品种
        for symbol in symbols:
            gateway.subscribe(symbol)

        # 启动处理线程
        threads = []

        # 订单处理线程
        order_thread = threading.Thread(
            target=order_processing_thread,
            args=(gateway, trading_system.order_queue_manager),
            daemon=True
        )
        threads.append(order_thread)

        # 撤单重发线程
        cancel_thread = threading.Thread(
            target=cancel_reorder_thread,
            args=(gateway,),
            daemon=True
        )
        threads.append(cancel_thread)

        # 失败订单重试线程
        retry_thread = threading.Thread(
            target=failed_order_retry_thread,
            args=(gateway,),
            daemon=True
        )
        threads.append(retry_thread)

        # 启动所有线程
        for thread in threads:
            thread.start()

        logger.info("All threads started. Trading system is running...")

        # 主循环
        try:
            while True:
                time.sleep(1)

                # 可以在这里添加系统监控逻辑
                # 例如：检查连接状态、监控订单队列长度等

        except KeyboardInterrupt:
            logger.info("Received interrupt signal. Shutting down...")

    except Exception as e:
        logger.error(f"Error in main: {e}")

    finally:
        # 清理资源
        logger.info("Cleaning up resources...")
        stop_event.set()

        # 关闭网关
        if 'gateway' in locals():
            gateway.close()

        # 等待线程结束
        time.sleep(2)

        # 关闭DolphinDB连接
        if 'trading_system' in locals():
            trading_system.session.close()

        logger.info("Comprehensive Trading Framework shutdown complete")

if __name__ == "__main__":
    main()
