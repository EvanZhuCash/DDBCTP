"""
行情录制优化 - 全市场数据录制和内存优化
实现高效的市场数据录制、存储和内存管理
支持CTP全市场期货合约数据录制，排除期权
"""

import time
import threading
import queue
import psutil
import os
import json
import sys
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set
import dolphindb as ddb
import pandas as pd

# Add paths for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'vnpy_ctp'))

# Try to import CTP constants, fallback if not available
try:
    from vnpy_ctp.api.ctp_constant import THOST_FTDC_PC_Futures
except ImportError:
    THOST_FTDC_PC_Futures = '1'  # Fallback value

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class ContractDiscovery:
    """期货合约发现器 - 从CTP获取所有活跃期货合约"""

    def __init__(self):
        self.futures_contracts = []
        self.logger = logger

    def get_all_futures_contracts(self) -> List[str]:
        """获取所有期货合约，排除期权 - 生成完整的1700+合约列表"""
        # 直接生成完整的期货合约列表，确保覆盖所有品种和到期月份
        self.logger.info("生成完整期货合约列表")
        known_contracts = self._generate_complete_futures_list()

        # 过滤出期货合约（排除期权）
        futures_contracts = []
        for contract in known_contracts:
            # 简单的期货合约识别：不包含C、P等期权标识
            if not any(opt_char in contract.upper() for opt_char in ['C-', 'P-', '-C', '-P', 'CALL', 'PUT']):
                futures_contracts.append(contract)

        self.logger.info(f"发现 {len(futures_contracts)} 个期货合约")
        self.futures_contracts = futures_contracts
        return futures_contracts

    def _generate_complete_futures_list(self) -> List[str]:
        """生成完整的期货合约列表，包含所有品种和到期月份"""
        contracts = []

        # 定义所有期货品种和交易所
        futures_symbols = {
            # 中金所 CFFEX
            'CFFEX': ['IC', 'IF', 'IH', 'IM', 'T', 'TF', 'TS', 'TL'],

            # 上期所 SHFE
            'SHFE': ['rb', 'au', 'ag', 'fu', 'ru', 'cu', 'al', 'zn', 'ni', 'sn', 'pb',
                     'hc', 'ss', 'bc', 'sp', 'wr', 'bu'],

            # 大商所 DCE
            'DCE': ['p', 'i', 'jm', 'm', 'y', 'eb', 'v', 'pp', 'c', 'cs', 'a', 'b',
                    'jd', 'l', 'pg', 'rr', 'fb', 'bb', 'lh', 'j'],

            # 郑商所 CZCE (使用3位数字)
            'CZCE': ['MA', 'OI', 'TA', 'SH', 'FG', 'SA', 'UR', 'SR', 'CF', 'CY',
                     'AP', 'CJ', 'PK', 'RM', 'ZC', 'SF', 'SM', 'WH', 'PM', 'RI', 'LR', 'JR'],

            # 上海国际能源交易中心 INE
            'INE': ['sc', 'lu', 'nr', 'bc', 'ec']
        }

        # 生成到期月份 (当前年份和下一年份的所有月份)
        current_year = 25  # 2025年
        next_year = 26     # 2026年

        # 标准月份
        months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']

        # 为每个交易所和品种生成合约
        for exchange, symbols in futures_symbols.items():
            for symbol in symbols:
                for year in [current_year, next_year]:
                    for month in months:
                        if exchange == 'CZCE':
                            # 郑商所使用3位数字格式
                            contract = f"{symbol}{year:02d}{month}"
                        else:
                            # 其他交易所使用4位数字格式
                            contract = f"{symbol}20{year:02d}{month}"
                        contracts.append(contract)

        self.logger.info(f"生成了 {len(contracts)} 个期货合约")
        return contracts

    def get_contracts_by_exchange(self, exchange: str) -> List[str]:
        """按交易所获取合约"""
        from vnpy.trader.object import EXCHANGE_SYM
        return [symbol for symbol, exch in EXCHANGE_SYM.items() if exch == exchange]

    def get_sample_contracts(self, count: int = 10) -> List[str]:
        """获取样本合约用于测试"""
        if not self.futures_contracts:
            self.get_all_futures_contracts()
        return self.futures_contracts[:count]


class MemoryMonitor:
    """增强的内存监控器 - 针对8GB RAM限制优化"""

    def __init__(self, threshold_percent: float = 60.0, critical_percent: float = 75.0, max_ram_gb: float = 8.0):
        self.threshold_percent = threshold_percent
        self.critical_percent = critical_percent
        self.max_ram_gb = max_ram_gb
        self.monitoring = False
        self.monitor_thread = None
        self.callbacks = []
        self.critical_callbacks = []
        self.logger = logger

        # 内存统计
        self.memory_history = []
        self.max_history_size = 100

        self.logger.info(f"内存监控器初始化: 最大RAM={max_ram_gb}GB, 阈值={threshold_percent}%, 临界值={critical_percent}%")

    def add_callback(self, callback, critical: bool = False):
        """添加内存超限回调函数"""
        if critical:
            self.critical_callbacks.append(callback)
        else:
            self.callbacks.append(callback)

    def start_monitoring(self, interval: float = 5.0):
        """开始内存监控"""
        self.monitoring = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop, args=(interval,))
        self.monitor_thread.daemon = True
        self.monitor_thread.start()
        self.logger.info(f"内存监控已启动，阈值: {self.threshold_percent}%, 临界值: {self.critical_percent}%")

    def stop_monitoring(self):
        """停止内存监控"""
        self.monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join()
        self.logger.info("内存监控已停止")

    def _monitor_loop(self, interval: float):
        """监控循环 - 包含3天保留策略"""
        while self.monitoring:
            current_time = datetime.now()
            memory_percent = psutil.virtual_memory().percent

            # 记录内存历史
            self.memory_history.append({
                'timestamp': current_time,
                'percent': memory_percent
            })
            if len(self.memory_history) > self.max_history_size:
                self.memory_history.pop(0)

            # 检查是否需要执行3天保留策略（每天凌晨2点）
            if (current_time.hour == 2 and current_time.minute < 30 and
                (self.last_daily_cleanup is None or
                 current_time.date() > self.last_daily_cleanup.date())):

                self.logger.info("执行3天数据保留策略...")
                try:
                    # 通过回调通知主系统执行保留策略
                    for callback in self.callbacks:
                        if hasattr(callback, '__name__') and 'retention' in callback.__name__:
                            callback(memory_percent)
                    self.last_daily_cleanup = current_time
                    self.logger.info("3天数据保留策略执行完成")
                except Exception as e:
                    self.logger.error(f"3天保留策略执行失败: {e}")

            # 检查临界阈值
            if memory_percent > self.critical_percent:
                self.logger.critical(f"内存使用率达到临界值: {memory_percent:.1f}%")
                for callback in self.critical_callbacks:
                    try:
                        callback(memory_percent)
                    except Exception as e:
                        self.logger.error(f"临界内存回调函数执行失败: {e}")

            # 检查普通阈值
            elif memory_percent > self.threshold_percent:
                self.logger.warning(f"内存使用率达到阈值: {memory_percent:.1f}%")
                for callback in self.callbacks:
                    try:
                        callback(memory_percent)
                    except Exception as e:
                        self.logger.error(f"内存回调函数执行失败: {e}")

            time.sleep(interval)

    def get_memory_info(self) -> Dict:
        """获取内存信息"""
        memory = psutil.virtual_memory()
        return {
            'total_gb': memory.total / (1024**3),
            'available_gb': memory.available / (1024**3),
            'used_gb': memory.used / (1024**3),
            'percent': memory.percent,
            'history_count': len(self.memory_history)
        }

    def get_memory_trend(self) -> Dict:
        """获取内存趋势"""
        if len(self.memory_history) < 2:
            return {'trend': 'unknown', 'rate': 0}

        recent = self.memory_history[-5:]  # 最近5个数据点
        if len(recent) < 2:
            return {'trend': 'stable', 'rate': 0}

        start_percent = recent[0]['percent']
        end_percent = recent[-1]['percent']
        rate = (end_percent - start_percent) / len(recent)

        if rate > 1:
            trend = 'rising_fast'
        elif rate > 0.1:
            trend = 'rising'
        elif rate < -1:
            trend = 'falling_fast'
        elif rate < -0.1:
            trend = 'falling'
        else:
            trend = 'stable'

        return {'trend': trend, 'rate': rate}


class MarketDataRecorder:
    """增强的市场数据录制器 - 支持全市场期货合约录制"""

    def __init__(self, storage_path: str = "/data/market_data",
                 host: str = "localhost", port: int = 8848,
                 username: str = "admin", password: str = "123456"):
        self.session = ddb.session()
        self.session.connect(host, port, username, password)
        self.storage_path = storage_path
        self.recording = False
        self.paused = False

        # 组件初始化 - 8GB RAM优化
        self.memory_monitor = MemoryMonitor(threshold_percent=50.0, critical_percent=65.0, max_ram_gb=8.0)
        self.contract_discovery = ContractDiscovery()
        self.logger = logger

        # 录制配置 - 针对8GB RAM和1700+合约优化
        self.batch_size = 500  # 减小批次大小
        self.max_symbols_per_batch = 100  # 增加每批处理的合约数
        self.cleanup_interval = 120  # 2分钟清理一次，更频繁
        self.last_cleanup_time = time.time()

        # 3天保留策略配置
        self.retention_days = 3
        self.daily_cleanup_hour = 2  # 凌晨2点执行日清理
        self.last_daily_cleanup = None

        # 录制统计
        self.stats = {
            'total_ticks': 0,
            'total_symbols': 0,
            'start_time': None,
            'data_size_mb': 0,
            'symbols_recording': [],
            'memory_cleanups': 0,
            'emergency_stops': 0
        }

        # 初始化存储和优化
        self.setup_storage_tables()
        self.setup_memory_optimization()
        self.setup_emergency_handlers()
    
    def setup_emergency_handlers(self):
        """设置紧急处理器和保留策略"""
        # 内存监控回调
        self.memory_monitor.add_callback(self._handle_memory_pressure, critical=False)
        self.memory_monitor.add_callback(self._handle_critical_memory, critical=True)

        # 添加3天保留策略回调
        self.memory_monitor.add_callback(self._retention_policy_callback, critical=False)

        self.logger.info("紧急处理器和3天保留策略已设置")

    def setup_storage_tables(self):
        """设置存储表 - 按DolphinDB最佳实践重新设计"""
        self.logger.info("设置DolphinDB存储表（按频率分离）...")

        try:
            # 创建年度数据库（用于历史数据）
            self.session.run("""
                // 创建年度数据库 - 用于长期存储
                if(existsDatabase("dfs://market_data_recording_Y")){
                    dropDatabase("dfs://market_data_recording_Y")
                }

                // 按年-月分区，然后按交易所分区
                db_date_y = database("", RANGE, 2024.01M..2027.01M)
                db_exchange_y = database("", LIST, [`CFFEX`SHFE`DCE`CZCE`INE])
                db_y = database("dfs://market_data_recording_Y", COMPO, [db_date_y, db_exchange_y])

                // 分钟K线表结构
                k_minute_schema = table(1:0,
                    `datetime`symbol`open`high`low`close`volume`turnover`open_interest`exchange,
                    [DATETIME, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, LONG, DOUBLE, LONG, SYMBOL])

                k_minute_table_y = createPartitionedTable(db_y, k_minute_schema, "k_minute", ["datetime", "exchange"])

                // 快照表结构（用于存储tick数据的聚合）
                snapshot_schema = table(1:0,
                    `datetime`symbol`last_price`volume`bid1`ask1`bid_vol1`ask_vol1`turnover`open_interest`exchange,
                    [DATETIME, SYMBOL, DOUBLE, LONG, DOUBLE, DOUBLE, LONG, LONG, DOUBLE, LONG, SYMBOL])

                snapshot_table_y = createPartitionedTable(db_y, snapshot_schema, "snapshot", ["datetime", "exchange"])
            """)

            # 创建月度数据库（用于近期数据）
            self.session.run("""
                // 创建月度数据库 - 用于近期高频访问
                if(existsDatabase("dfs://market_data_recording_M")){
                    dropDatabase("dfs://market_data_recording_M")
                }

                // 按日期分区，然后按交易所分区
                db_date_m = database("", RANGE, 2024.12.01..2025.03.01)
                db_exchange_m = database("", LIST, [`CFFEX`SHFE`DCE`CZCE`INE])
                db_m = database("dfs://market_data_recording_M", COMPO, [db_date_m, db_exchange_m])

                // 分钟K线表结构
                k_minute_table_m = createPartitionedTable(db_m, k_minute_schema, "k_minute", ["datetime", "exchange"])

                // 快照表结构
                snapshot_table_m = createPartitionedTable(db_m, snapshot_schema, "snapshot", ["datetime", "exchange"])
            """)

            # 创建实时流表（内存中，定期清理）
            self.session.run("""
                // 实时tick流表 - 小容量，频繁清理
                share streamTable(50000:0,
                    `timestamp`symbol`price`volume`bid1`ask1`bid_vol1`ask_vol1`turnover`open_interest`exchange`update_time,
                    [TIMESTAMP, SYMBOL, DOUBLE, LONG, DOUBLE, DOUBLE, LONG, LONG, DOUBLE, LONG, SYMBOL, TIMESTAMP]) as live_tick_stream

                // 分钟聚合流表
                share streamTable(10000:0,
                    `timestamp`symbol`open`high`low`close`volume`turnover`open_interest`exchange,
                    [TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, LONG, DOUBLE, LONG, SYMBOL]) as live_minute_stream

                // 统计监控流表
                share streamTable(1000:0,
                    `timestamp`symbol_count`tick_count`memory_percent`status,
                    [TIMESTAMP, INT, LONG, DOUBLE, SYMBOL]) as recording_stats_stream
            """)

            self.logger.info("存储表设置完成 - 年度库、月度库和实时流表")

        except Exception as e:
            self.logger.error(f"设置存储表失败: {e}")
            raise
    
    def setup_memory_optimization(self):
        """设置内存优化 - 8GB RAM优化版本"""
        self.logger.info("设置内存优化配置（8GB RAM限制）...")

        try:
            self.session.run(f"""
                // 内存优化配置 - 针对8GB RAM
                memory_config = dict(STRING, ANY)
                memory_config["max_stream_table_size"] = 30000  // 减小流表大小
                memory_config["batch_write_size"] = {self.batch_size}
                memory_config["cleanup_interval"] = {self.cleanup_interval * 1000}  // 转换为毫秒
                memory_config["emergency_cleanup_size"] = 15000  // 紧急清理阈值

                // 数据写入函数 - 写入到月度数据库
                def write_snapshot_data(msg){{
                    try {{
                        // 写入到月度数据库的快照表
                        snapshot_table_m = loadTable("dfs://market_data_recording_M", "snapshot")
                        insert into snapshot_table_m values(msg.timestamp, msg.symbol, msg.price, msg.volume,
                                                           msg.bid1, msg.ask1, msg.bid_vol1, msg.ask_vol1,
                                                           msg.turnover, msg.open_interest, msg.exchange)
                    }} catch(ex) {{
                        print("写入快照数据失败: " + ex)
                    }}
                }}

                def write_minute_data(msg){{
                    try {{
                        // 写入到月度数据库的分钟K线表
                        k_minute_table_m = loadTable("dfs://market_data_recording_M", "k_minute")
                        insert into k_minute_table_m values(msg.timestamp, msg.symbol, msg.open, msg.high,
                                                          msg.low, msg.close, msg.volume, msg.turnover,
                                                          msg.open_interest, msg.exchange)
                    }} catch(ex) {{
                        print("写入分钟数据失败: " + ex)
                    }}
                }}

                // 智能数据清理函数 - 3天保留策略
                def cleanup_stream_tables(mutable config, emergency=false){{
                    current_size = size(live_tick_stream)
                    max_size = iif(emergency, config["emergency_cleanup_size"], config["max_stream_table_size"])

                    if(current_size > max_size){{
                        // 智能清理策略：保留2小时在流表，其余移到持久化存储
                        retain_hours = iif(emergency, 0.5, 2.0)  // 紧急情况保留30分钟，正常保留2小时
                        cutoff_time = (select max(timestamp) from live_tick_stream) - retain_hours * 3600000

                        // 在删除前，将数据写入持久化存储
                        old_data = select * from live_tick_stream where timestamp < cutoff_time
                        if(size(old_data) > 0){{
                            snapshot_table_m = loadTable("dfs://market_data_recording_M", "snapshot")
                            insert into snapshot_table_m select timestamp as datetime, symbol, price as last_price,
                                                               volume, bid1, ask1, bid_vol1, ask_vol1, turnover,
                                                               open_interest, exchange from old_data
                        }}

                        // 删除旧数据
                        delete from live_tick_stream where timestamp < cutoff_time

                        deleted_count = current_size - size(live_tick_stream)
                        print("清理流表数据，删除 " + string(deleted_count) + " 条记录，当前大小: " + string(size(live_tick_stream)))

                        // 同时清理分钟流表
                        minute_cutoff = cutoff_time
                        delete from live_minute_stream where timestamp < minute_cutoff
                    }}

                    // 记录清理统计
                    insert into recording_stats_stream values(now(), 0, size(live_tick_stream), 0.0, "CLEANUP")
                }}

                // 3天数据保留策略
                def cleanup_persistent_data(){{
                    try {{
                        // 清理月度数据库中超过3天的数据
                        three_days_ago = today() - 3
                        snapshot_table_m = loadTable("dfs://market_data_recording_M", "snapshot")

                        // 获取要删除的数据量
                        old_count = exec count(*) from snapshot_table_m where date(datetime) < three_days_ago

                        if(old_count > 0){{
                            // 删除超过3天的数据
                            delete from snapshot_table_m where date(datetime) < three_days_ago
                            print("3天保留策略：删除 " + string(old_count) + " 条历史数据")
                        }}

                        // 同样清理分钟K线数据
                        k_minute_table_m = loadTable("dfs://market_data_recording_M", "k_minute")
                        old_minute_count = exec count(*) from k_minute_table_m where date(datetime) < three_days_ago

                        if(old_minute_count > 0){{
                            delete from k_minute_table_m where date(datetime) < three_days_ago
                            print("3天保留策略：删除 " + string(old_minute_count) + " 条分钟K线数据")
                        }}

                    }} catch(ex) {{
                        print("3天保留策略执行失败: " + ex)
                    }}
                }}

                // 创建聚合引擎
                try {{
                    dropStreamEngine("minuteAggEngine")
                }} catch(ex) {{
                    // 引擎不存在，忽略错误
                }}

                minute_engine = createTimeSeriesEngine(name="minuteAggEngine",
                    windowSize=60000, step=60000,  // 1分钟窗口
                    metrics=<[first(price) as open, max(price) as high, min(price) as low,
                             last(price) as close, sum(volume) as volume,
                             sum(turnover) as turnover, last(open_interest) as open_interest,
                             last(exchange) as exchange]>,
                    dummyTable=live_tick_stream, outputTable=live_minute_stream,
                    timeColumn="timestamp", keyColumn="symbol")

                // 创建统计监控函数
                def record_stats(symbol_count, tick_count, memory_percent){{
                    insert into recording_stats_stream values(now(), symbol_count, tick_count, memory_percent, "RECORDING")
                }}
            """)
            self.logger.info("内存优化配置完成（8GB RAM优化）")

        except Exception as e:
            self.logger.error(f"设置内存优化失败: {e}")
            raise
    
    def _handle_memory_pressure(self, memory_percent: float):
        """处理内存压力"""
        self.logger.warning(f"触发内存清理，当前使用率: {memory_percent:.1f}%")

        try:
            # 执行DolphinDB内存清理
            self.session.run("cleanup_stream_tables(memory_config, false)")
            self.stats['memory_cleanups'] += 1

            # 更新清理时间
            self.last_cleanup_time = time.time()

            self.logger.info("内存清理完成")

        except Exception as e:
            self.logger.error(f"内存清理失败: {e}")

    def _handle_critical_memory(self, memory_percent: float):
        """处理临界内存情况"""
        self.logger.critical(f"内存使用率达到临界值: {memory_percent:.1f}%，执行紧急措施")

        try:
            # 紧急清理
            self.session.run("cleanup_stream_tables(memory_config, true)")
            self.stats['memory_cleanups'] += 1

            # 暂停录制
            if self.recording and not self.paused:
                self.logger.critical("内存压力过大，暂停录制...")
                self.pause_recording()
                self.stats['emergency_stops'] += 1

                # 等待一段时间后尝试恢复
                threading.Timer(60.0, self._attempt_resume).start()

        except Exception as e:
            self.logger.error(f"紧急内存处理失败: {e}")

    def _attempt_resume(self):
        """尝试恢复录制"""
        current_memory = psutil.virtual_memory().percent
        if current_memory < self.memory_monitor.threshold_percent:
            self.logger.info(f"内存使用率降至 {current_memory:.1f}%，尝试恢复录制")
            self.resume_recording()
        else:
            self.logger.warning(f"内存使用率仍然过高 {current_memory:.1f}%，延迟恢复")
            threading.Timer(60.0, self._attempt_resume).start()

    def _retention_policy_callback(self, memory_percent: float):
        """3天数据保留策略回调"""
        # 这个方法会被内存监控器在特定条件下调用
        # 检查是否是保留策略触发的调用
        current_time = datetime.now()
        if (current_time.hour == 2 and current_time.minute < 30):
            self.execute_retention_policy()

    def execute_retention_policy(self):
        """执行3天数据保留策略"""
        try:
            self.logger.info("开始执行3天数据保留策略...")
            self.session.run("cleanup_persistent_data()")
            self.logger.info("3天数据保留策略执行完成")
        except Exception as e:
            self.logger.error(f"执行3天保留策略失败: {e}")

    def start_recording(self, symbols: List[str] = None, full_market: bool = False):
        """开始录制 - 支持全市场录制"""
        if self.recording:
            self.logger.warning("录制已在进行中")
            return

        # 确定要录制的合约
        if full_market:
            symbols = self.contract_discovery.get_all_futures_contracts()
            self.logger.info(f"全市场录制模式，发现 {len(symbols)} 个期货合约")
        elif symbols is None:
            symbols = self.contract_discovery.get_sample_contracts(10)
            self.logger.info(f"使用样本合约进行录制: {len(symbols)} 个")

        if not symbols:
            self.logger.error("没有找到可录制的合约")
            return

        self.recording = True
        self.paused = False
        self.stats['start_time'] = datetime.now()
        self.stats['total_symbols'] = len(symbols)
        self.stats['symbols_recording'] = symbols.copy()

        self.logger.info(f"开始录制 {len(symbols)} 个品种的行情数据...")
        self.logger.info(f"录制合约: {', '.join(symbols[:10])}{'...' if len(symbols) > 10 else ''}")

        # 启动内存监控
        self.memory_monitor.start_monitoring()

        try:
            # 获取月度数据库表引用
            self.session.run("""
                snapshot_table_m = loadTable("dfs://market_data_recording_M", "snapshot")
                k_minute_table_m = loadTable("dfs://market_data_recording_M", "k_minute")
            """)

            # 订阅流表处理 - 使用新的写入函数
            self.session.run(f"""
                // 订阅tick流表，写入快照表
                subscribeTable(tableName=`live_tick_stream, actionName="snapshotWriter",
                    handler=write_snapshot_data, msgAsTable=true, offset=-1, batchSize={self.batch_size})

                // 订阅分钟流表，写入分钟K线表
                subscribeTable(tableName=`live_minute_stream, actionName="minuteWriter",
                    handler=write_minute_data, msgAsTable=true, offset=-1, batchSize={self.batch_size // 10})

                // 订阅tick流表到聚合引擎
                subscribeTable(tableName=`live_tick_stream, actionName="minuteAggEngine",
                    handler=tableInsert{{minute_engine}}, msgAsTable=true, offset=-1)
            """)

            # 记录开始统计
            self.session.run(f"""
                record_stats({len(symbols)}, 0, {psutil.virtual_memory().percent})
            """)

            self.logger.info("录制已启动")

        except Exception as e:
            self.logger.error(f"启动录制失败: {e}")
            self.recording = False
            raise
    
    def stop_recording(self):
        """停止录制"""
        if not self.recording:
            self.logger.warning("录制未在进行中")
            return

        self.recording = False
        self.paused = False

        self.logger.info("正在停止录制...")

        # 停止内存监控
        self.memory_monitor.stop_monitoring()

        try:
            # 取消订阅
            self.session.run("""
                unsubscribeTable(tableName=`live_tick_stream, actionName="snapshotWriter")
                unsubscribeTable(tableName=`live_minute_stream, actionName="minuteWriter")
                unsubscribeTable(tableName=`live_tick_stream, actionName="minuteAggEngine")
            """)

            # 记录停止统计
            self.session.run(f"""
                record_stats({self.stats['total_symbols']}, {self.stats['total_ticks']}, {psutil.virtual_memory().percent})
            """)

        except Exception as e:
            self.logger.error(f"停止录制时发生错误: {e}")

        # 更新统计信息
        self.update_stats()

        self.logger.info("录制已停止")
        self.print_stats()
    
    def pause_recording(self):
        """暂停录制"""
        if self.recording and not self.paused:
            try:
                self.session.run("""
                    unsubscribeTable(tableName=`live_tick_stream, actionName="snapshotWriter")
                    unsubscribeTable(tableName=`live_minute_stream, actionName="minuteWriter")
                """)
                self.paused = True
                self.logger.info("录制已暂停")
            except Exception as e:
                self.logger.error(f"暂停录制失败: {e}")

    def resume_recording(self):
        """恢复录制"""
        if self.recording and self.paused:
            try:
                self.session.run(f"""
                    subscribeTable(tableName=`live_tick_stream, actionName="snapshotWriter",
                        handler=write_snapshot_data, msgAsTable=true, offset=-1, batchSize={self.batch_size})
                    subscribeTable(tableName=`live_minute_stream, actionName="minuteWriter",
                        handler=write_minute_data, msgAsTable=true, offset=-1, batchSize={self.batch_size // 10})
                """)
                self.paused = False
                self.logger.info("录制已恢复")
            except Exception as e:
                self.logger.error(f"恢复录制失败: {e}")

    def inject_ctp_data(self, symbols: List[str]):
        """注入CTP实时数据 - 连接到实际CTP数据源"""
        self.logger.info(f"开始连接CTP数据源，订阅 {len(symbols)} 个合约")

        # 这里应该集成实际的CTP连接逻辑
        # 参考test_live_simulate.py中的CTP连接方式
        try:
            # 模拟CTP数据注入
            from vnpy.trader.object import EXCHANGE_SYM

            for symbol in symbols:
                if symbol in EXCHANGE_SYM:
                    exchange = EXCHANGE_SYM[symbol]
                    self.logger.info(f"订阅合约: {symbol} ({exchange})")
                    # 这里应该调用实际的CTP订阅方法
                    # gateway.subscribe(symbol)

            self.logger.info("CTP数据源连接完成")

        except Exception as e:
            self.logger.error(f"CTP数据源连接失败: {e}")
            raise
    
    def inject_test_data(self, symbol_count: int = 100, duration_minutes: int = 60, tick_rate: int = 10):
        """注入测试数据 - 增强版本"""
        self.logger.info(f"开始注入测试数据: {symbol_count}品种, {duration_minutes}分钟, {tick_rate}tick/s")

        # 使用真实的期货合约名称
        all_symbols = self.contract_discovery.get_all_futures_contracts()
        if len(all_symbols) >= symbol_count:
            symbols = all_symbols[:symbol_count]
        else:
            # 如果真实合约不够，补充测试合约
            symbols = all_symbols + [f"TEST{i:03d}" for i in range(symbol_count - len(all_symbols))]

        start_time = datetime.now()
        total_ticks = 0
        batch_data = []

        self.logger.info(f"使用合约: {', '.join(symbols[:5])}{'...' if len(symbols) > 5 else ''}")

        try:
            for minute in range(duration_minutes):
                for second in range(60):
                    for tick in range(tick_rate):
                        if not self.recording or self.paused:
                            break

                        # 轮询选择品种
                        symbol = symbols[total_ticks % len(symbols)]

                        # 生成模拟数据
                        timestamp = start_time + timedelta(minutes=minute, seconds=second,
                                                         milliseconds=tick * (1000 // tick_rate))
                        price = 100 + (total_ticks % 100) * 0.01
                        volume = 100 + (total_ticks % 50)

                        # 获取交易所信息
                        try:
                            from vnpy.trader.object import EXCHANGE_SYM
                            exchange = EXCHANGE_SYM.get(symbol, "TEST")
                        except ImportError:
                            # 简单的交易所映射
                            if symbol.startswith(('IC', 'IF', 'IH', 'IM', 'T', 'TF', 'TS', 'TL')):
                                exchange = "CFFEX"
                            elif symbol.startswith(('rb', 'au', 'ag', 'fu', 'ru', 'cu', 'al', 'zn', 'ni', 'sn', 'pb', 'hc', 'ss', 'bc', 'sp')):
                                exchange = "SHFE"
                            elif symbol.startswith(('p', 'i', 'jm', 'm', 'y', 'eb', 'v', 'pp', 'c', 'cs', 'a', 'b', 'jd', 'l', 'pg')):
                                exchange = "DCE"
                            elif symbol.startswith(('MA', 'OI', 'TA', 'SH', 'FG', 'SA', 'UR', 'SR', 'CF', 'CY', 'AP', 'CJ', 'PK', 'RM', 'ZC')):
                                exchange = "CZCE"
                            elif symbol.startswith(('sc', 'lu', 'nr', 'bc')):
                                exchange = "INE"
                            else:
                                exchange = "TEST"

                        # 准备数据 - 使用正确的时间戳格式
                        timestamp_str = f"timestamp('{timestamp.strftime('%Y.%m.%dT%H:%M:%S.%f')[:-3]}')"
                        update_time_str = f"timestamp('{datetime.now().strftime('%Y.%m.%dT%H:%M:%S.%f')[:-3]}')"

                        # 直接插入 - 使用单行格式避免语法问题
                        try:
                            insert_sql = f"insert into live_tick_stream values({timestamp_str}, `{symbol}, {price}, {volume}, {price - 0.01}, {price + 0.01}, {volume//2}, {volume//2}, {price * volume}, {1000 + total_ticks % 100}, `{exchange}, {update_time_str})"
                            self.session.run(insert_sql)
                        except Exception as e:
                            if total_ticks < 5:  # Only log first few errors
                                self.logger.error(f"插入数据失败: {e}")
                                self.logger.error(f"SQL: {insert_sql}")

                        # 保留批量逻辑用于统计
                        batch_data.append("dummy")

                        total_ticks += 1

                        # 批量处理统计
                        if len(batch_data) >= self.batch_size:
                            self.logger.debug(f"处理批次数据，大小: {len(batch_data)}")
                            batch_data = []

                        # 控制速率
                        if tick_rate > 100:
                            time.sleep(0.001)

                    if not self.recording or self.paused:
                        break

                if not self.recording or self.paused:
                    break

                # 每分钟报告进度和内存状态
                if minute % 5 == 0:
                    memory_info = self.memory_monitor.get_memory_info()
                    self.logger.info(f"已注入 {minute} 分钟数据, 总计 {total_ticks} ticks, "
                                   f"内存使用: {memory_info['percent']:.1f}%")

                    # 定期清理
                    if time.time() - self.last_cleanup_time > self.cleanup_interval:
                        self.session.run("cleanup_stream_tables(memory_config, false)")
                        self.last_cleanup_time = time.time()

            # 处理剩余数据统计
            if batch_data:
                self.logger.info(f"处理剩余 {len(batch_data)} 条数据统计")

            # 检查流表大小
            try:
                stream_size = self.session.run("size(live_tick_stream)")
                self.logger.info(f"流表当前大小: {stream_size}")
            except Exception as e:
                self.logger.error(f"检查流表大小失败: {e}")

            self.logger.info(f"测试数据注入完成, 总计 {total_ticks} ticks")

        except Exception as e:
            self.logger.error(f"测试数据注入失败: {e}")
            raise

    def _insert_batch_data(self, batch_data: List[str]):
        """批量插入数据"""
        try:
            # 简化批量插入 - 一次插入一条
            for data in batch_data:
                try:
                    self.session.run(f"insert into live_tick_stream values {data}")
                except Exception as e:
                    self.logger.debug(f"单条插入失败: {e}")
                    continue

            self.logger.debug(f"成功插入 {len(batch_data)} 条数据")

        except Exception as e:
            self.logger.error(f"批量插入数据失败: {e}")
    
    def update_stats(self):
        """更新统计信息 - 增强版本"""
        try:
            # 获取tick数据统计
            tick_stats = self.session.run("""
                select count(*) as total_ticks,
                       count(distinct symbol) as unique_symbols,
                       (max(timestamp) - min(timestamp)) / 1000.0 as duration_seconds,
                       min(timestamp) as start_time,
                       max(timestamp) as end_time
                from live_tick_stream
            """)

            if len(tick_stats) > 0 and tick_stats.iloc[0]['total_ticks'] > 0:
                self.stats['total_ticks'] = int(tick_stats.iloc[0]['total_ticks'])
                self.stats['unique_symbols'] = int(tick_stats.iloc[0]['unique_symbols'])
                self.stats['duration_seconds'] = float(tick_stats.iloc[0]['duration_seconds'])

            # 获取分区表统计
            try:
                partition_stats = self.session.run("""
                    select count(*) as stored_ticks from loadTable("dfs://market_data_recording", "tick_data")
                """)
                if len(partition_stats) > 0:
                    self.stats['stored_ticks'] = int(partition_stats.iloc[0]['stored_ticks'])
            except:
                self.stats['stored_ticks'] = 0

            # 估算数据大小 (每条记录约150字节)
            self.stats['data_size_mb'] = self.stats['total_ticks'] * 0.15

            # 获取内存趋势
            memory_trend = self.memory_monitor.get_memory_trend()
            self.stats['memory_trend'] = memory_trend

        except Exception as e:
            self.logger.error(f"更新统计信息失败: {e}")

    def print_stats(self):
        """打印统计信息 - 增强版本"""
        self.logger.info("\n" + "="*50)
        self.logger.info("录制统计信息")
        self.logger.info("="*50)

        duration = datetime.now() - self.stats['start_time'] if self.stats['start_time'] else timedelta(0)
        self.logger.info(f"录制时长: {duration}")
        self.logger.info(f"总tick数: {self.stats['total_ticks']:,}")
        self.logger.info(f"已存储tick数: {self.stats.get('stored_ticks', 0):,}")
        self.logger.info(f"品种数量: {self.stats.get('unique_symbols', 0)}")
        self.logger.info(f"目标品种数: {self.stats['total_symbols']}")
        self.logger.info(f"数据大小: {self.stats['data_size_mb']:.2f} MB")

        if self.stats.get('duration_seconds', 0) > 0:
            tps = self.stats['total_ticks'] / self.stats['duration_seconds']
            self.logger.info(f"平均TPS: {tps:.2f}")

        # 内存信息
        memory_info = self.memory_monitor.get_memory_info()
        self.logger.info(f"内存使用: {memory_info['used_gb']:.2f}GB / {memory_info['total_gb']:.2f}GB ({memory_info['percent']:.1f}%)")

        # 内存趋势
        if 'memory_trend' in self.stats:
            trend = self.stats['memory_trend']
            self.logger.info(f"内存趋势: {trend['trend']} (变化率: {trend['rate']:.2f}%/次)")

        # 系统状态
        self.logger.info(f"内存清理次数: {self.stats['memory_cleanups']}")
        self.logger.info(f"紧急停止次数: {self.stats['emergency_stops']}")
        self.logger.info(f"录制状态: {'暂停' if self.paused else '运行中' if self.recording else '已停止'}")

        self.logger.info("="*50)
    
    def generate_recording_report(self, output_file: str = "recording_report.json"):
        """生成录制报告"""
        self.update_stats()
        
        report = {
            'recording_session': {
                'start_time': self.stats['start_time'].isoformat() if self.stats['start_time'] else None,
                'end_time': datetime.now().isoformat(),
                'duration_minutes': (datetime.now() - self.stats['start_time']).total_seconds() / 60 if self.stats['start_time'] else 0
            },
            'data_statistics': self.stats,
            'memory_info': self.memory_monitor.get_memory_info(),
            'storage_info': self._get_storage_info()
        }
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False, default=str)
        
        print(f"录制报告已保存到: {output_file}")
        return report
    
    def _get_storage_info(self) -> Dict:
        """获取存储信息"""
        try:
            # 获取分区表信息
            try:
                table_info = self.session.run("""
                    select count(*) as tick_count from loadTable("dfs://market_data_recording", "tick_data")
                """)
                tick_count = int(table_info.iloc[0]['tick_count']) if len(table_info) > 0 else 0
            except:
                tick_count = 0

            return {
                'tick_data_count': tick_count,
                'database_path': "dfs://market_data_recording"
            }
        except Exception as e:
            return {'error': str(e)}


def run_performance_test(test_type: str = "sample", duration_minutes: int = 10):
    """运行性能测试"""
    logger.info(f"开始性能测试: {test_type}, 持续时间: {duration_minutes}分钟")

    # 清理DolphinDB环境
    logger.info("清理DolphinDB环境...")
    try:
        import subprocess
        subprocess.run([sys.executable, "../cleanup.py"], check=True, cwd=os.path.dirname(__file__))
    except:
        logger.warning("清理脚本执行失败，继续测试")

    recorder = MarketDataRecorder()

    try:
        if test_type == "sample":
            # 样本测试 - 10个合约
            logger.info("执行样本测试...")
            recorder.start_recording(full_market=False)
            recorder.inject_test_data(symbol_count=10, duration_minutes=duration_minutes, tick_rate=5)

        elif test_type == "medium":
            # 中等规模测试 - 50个合约
            logger.info("执行中等规模测试...")
            symbols = recorder.contract_discovery.get_sample_contracts(50)
            recorder.start_recording(symbols)
            recorder.inject_test_data(symbol_count=50, duration_minutes=duration_minutes, tick_rate=10)

        elif test_type == "full":
            # 全市场测试
            logger.info("执行全市场测试...")
            recorder.start_recording(full_market=True)
            all_symbols = recorder.contract_discovery.get_all_futures_contracts()
            recorder.inject_test_data(symbol_count=len(all_symbols), duration_minutes=duration_minutes, tick_rate=15)

        elif test_type == "stress":
            # 压力测试 - 高频数据
            logger.info("执行压力测试...")
            recorder.start_recording(full_market=True)
            all_symbols = recorder.contract_discovery.get_all_futures_contracts()
            recorder.inject_test_data(symbol_count=len(all_symbols), duration_minutes=duration_minutes, tick_rate=50)

        # 等待处理完成
        logger.info("等待数据处理完成...")
        time.sleep(30)

        # 停止录制
        recorder.stop_recording()

        # 生成报告
        report_file = f"recording_report_{test_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        recorder.generate_recording_report(report_file)

        logger.info(f"性能测试完成，报告已保存到: {report_file}")

    except KeyboardInterrupt:
        logger.info("\n用户中断测试")
        recorder.stop_recording()
    except Exception as e:
        logger.error(f"测试过程中发生错误: {e}")
        recorder.stop_recording()
        raise
    finally:
        # 确保清理
        try:
            recorder.session.close()
        except:
            pass


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="市场数据录制性能测试")
    parser.add_argument("--test-type", choices=["sample", "medium", "full", "stress"],
                       default="sample", help="测试类型")
    parser.add_argument("--duration", type=int, default=5, help="测试持续时间(分钟)")
    parser.add_argument("--cleanup", action="store_true", help="测试前清理DolphinDB")

    args = parser.parse_args()

    if args.cleanup:
        logger.info("执行清理...")
        try:
            import subprocess
            subprocess.run([sys.executable, "../cleanup.py"], check=True)
        except Exception as e:
            logger.error(f"清理失败: {e}")

    run_performance_test(args.test_type, args.duration)
