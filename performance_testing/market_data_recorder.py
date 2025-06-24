"""
行情录制优化 - 全市场数据录制和内存优化
实现高效的市场数据录制、存储和内存管理
"""

import time
import threading
import queue
import psutil
import os
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import dolphindb as ddb
import pandas as pd


class MemoryMonitor:
    """内存监控器"""
    
    def __init__(self, threshold_percent: float = 80.0):
        self.threshold_percent = threshold_percent
        self.monitoring = False
        self.monitor_thread = None
        self.callbacks = []
    
    def add_callback(self, callback):
        """添加内存超限回调函数"""
        self.callbacks.append(callback)
    
    def start_monitoring(self, interval: float = 5.0):
        """开始内存监控"""
        self.monitoring = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop, args=(interval,))
        self.monitor_thread.daemon = True
        self.monitor_thread.start()
    
    def stop_monitoring(self):
        """停止内存监控"""
        self.monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join()
    
    def _monitor_loop(self, interval: float):
        """监控循环"""
        while self.monitoring:
            memory_percent = psutil.virtual_memory().percent
            
            if memory_percent > self.threshold_percent:
                print(f"警告: 内存使用率达到 {memory_percent:.1f}%")
                for callback in self.callbacks:
                    try:
                        callback(memory_percent)
                    except Exception as e:
                        print(f"内存回调函数执行失败: {e}")
            
            time.sleep(interval)
    
    def get_memory_info(self) -> Dict:
        """获取内存信息"""
        memory = psutil.virtual_memory()
        return {
            'total_gb': memory.total / (1024**3),
            'available_gb': memory.available / (1024**3),
            'used_gb': memory.used / (1024**3),
            'percent': memory.percent
        }


class MarketDataRecorder:
    """市场数据录制器"""
    
    def __init__(self, storage_path: str = "/data/market_data"):
        self.session = ddb.session()
        self.session.connect("localhost", 8848, "admin", "123456")
        self.storage_path = storage_path
        self.recording = False
        self.memory_monitor = MemoryMonitor()
        self.setup_storage_tables()
        self.setup_memory_optimization()
        
        # 录制统计
        self.stats = {
            'total_ticks': 0,
            'total_symbols': 0,
            'start_time': None,
            'data_size_mb': 0
        }
    
    def setup_storage_tables(self):
        """设置存储表"""
        self.session.run(f"""
            // 创建分布式数据库
            if(existsDatabase("dfs://market_data_recording")){{
                dropDatabase("dfs://market_data_recording")
            }}
            
            // 按日期分区
            db_date = database("", RANGE, 2024.01.01..2025.01.01)
            // 按品种分区  
            db_symbol = database("", HASH, [SYMBOL, 50])
            // 组合分区
            db = database("dfs://market_data_recording", COMPO, [db_date, db_symbol])
            
            // Tick数据表结构
            tick_schema = table(1:0, 
                `timestamp`symbol`price`volume`bid1`ask1`bid_vol1`ask_vol1`turnover`open_interest, 
                [TIMESTAMP, SYMBOL, DOUBLE, LONG, DOUBLE, DOUBLE, LONG, LONG, DOUBLE, LONG])
            
            tick_table = createPartitionedTable(db, tick_schema, "tick_data", ["timestamp", "symbol"])
            
            // 分钟数据表结构
            minute_schema = table(1:0,
                `timestamp`symbol`open`high`low`close`volume`turnover`open_interest,
                [TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, LONG, DOUBLE, LONG])
            
            minute_table = createPartitionedTable(db, minute_schema, "minute_data", ["timestamp", "symbol"])
            
            // 日线数据表结构  
            daily_schema = table(1:0,
                `date`symbol`open`high`low`close`volume`turnover`open_interest,
                [DATE, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, LONG, DOUBLE, LONG])
            
            daily_table = createPartitionedTable(db, daily_schema, "daily_data", ["date", "symbol"])
            
            // 实时流表
            share streamTable(50000:0, 
                `timestamp`symbol`price`volume`bid1`ask1`bid_vol1`ask_vol1`turnover`open_interest, 
                [TIMESTAMP, SYMBOL, DOUBLE, LONG, DOUBLE, DOUBLE, LONG, LONG, DOUBLE, LONG]) as live_tick_stream
            
            // 聚合流表
            share streamTable(10000:0,
                `timestamp`symbol`open`high`low`close`volume`turnover`open_interest,
                [TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, LONG, DOUBLE, LONG]) as live_minute_stream
        """)
    
    def setup_memory_optimization(self):
        """设置内存优化"""
        self.session.run("""
            // 内存优化配置
            memory_config = dict(STRING, ANY)
            memory_config["max_stream_table_size"] = 50000
            memory_config["batch_write_size"] = 10000
            memory_config["cleanup_interval"] = 300000  // 5分钟清理一次
            
            // 批量写入器
            tick_writer = batchTableWriter("dfs://market_data_recording", "tick_data", false)
            tick_writer.setBatchSize(memory_config["batch_write_size"])
            
            minute_writer = batchTableWriter("dfs://market_data_recording", "minute_data", false)
            minute_writer.setBatchSize(memory_config["batch_write_size"])
            
            // 数据清理函数
            def cleanup_stream_tables(mutable config){{
                current_size = size(live_tick_stream)
                max_size = config["max_stream_table_size"]
                
                if(current_size > max_size){{
                    // 保留最新的数据，删除旧数据
                    cutoff_time = (select max(timestamp) from live_tick_stream) - 3600000  // 保留1小时
                    delete from live_tick_stream where timestamp < cutoff_time
                    
                    print("清理流表数据，删除 " + string(current_size - size(live_tick_stream)) + " 条记录")
                }}
                
                // 强制垃圾回收
                gc()
            }}
            
            // 创建聚合引擎
            minute_engine = createTimeSeriesEngine(name="minuteAggEngine",
                windowSize=60000, step=60000,  // 1分钟窗口
                metrics=<[first(price) as open, max(price) as high, min(price) as low, 
                         last(price) as close, sum(volume) as volume, 
                         sum(turnover) as turnover, last(open_interest) as open_interest]>,
                dummyTable=live_tick_stream, outputTable=live_minute_stream,
                timeColumn="timestamp", keyColumn="symbol")
        """)
        
        # 设置内存监控回调
        self.memory_monitor.add_callback(self._handle_memory_pressure)
    
    def _handle_memory_pressure(self, memory_percent: float):
        """处理内存压力"""
        print(f"触发内存清理，当前使用率: {memory_percent:.1f}%")
        
        # 执行DolphinDB内存清理
        self.session.run("""
            cleanup_stream_tables(memory_config)
        """)
        
        # 如果内存压力仍然很大，暂停录制
        if memory_percent > 90:
            print("内存使用率过高，暂停录制...")
            self.pause_recording()
    
    def start_recording(self, symbols: List[str]):
        """开始录制"""
        if self.recording:
            print("录制已在进行中")
            return
        
        self.recording = True
        self.stats['start_time'] = datetime.now()
        self.stats['total_symbols'] = len(symbols)
        
        print(f"开始录制 {len(symbols)} 个品种的行情数据...")
        
        # 启动内存监控
        self.memory_monitor.start_monitoring()
        
        # 订阅流表处理
        self.session.run("""
            // 订阅tick流表，写入分区表
            subscribeTable(tableName=`live_tick_stream, actionName="tickWriter", 
                handler=tick_writer, msgAsTable=true, offset=-1, batchSize=1000)
            
            // 订阅分钟流表，写入分区表
            subscribeTable(tableName=`live_minute_stream, actionName="minuteWriter",
                handler=minute_writer, msgAsTable=true, offset=-1, batchSize=100)
            
            // 订阅tick流表到聚合引擎
            subscribeTable(tableName=`live_tick_stream, actionName="minuteAggEngine",
                handler=tableInsert{minute_engine}, msgAsTable=true, offset=-1)
        """)
        
        print("录制已启动")
    
    def stop_recording(self):
        """停止录制"""
        if not self.recording:
            print("录制未在进行中")
            return
        
        self.recording = False
        
        # 停止内存监控
        self.memory_monitor.stop_monitoring()
        
        # 取消订阅
        self.session.run("""
            unsubscribeTable(tableName=`live_tick_stream, actionName="tickWriter")
            unsubscribeTable(tableName=`live_minute_stream, actionName="minuteWriter")
            unsubscribeTable(tableName=`live_tick_stream, actionName="minuteAggEngine")
        """)
        
        # 刷新批量写入器
        self.session.run("""
            tick_writer.flush()
            minute_writer.flush()
        """)
        
        # 更新统计信息
        self.update_stats()
        
        print("录制已停止")
        self.print_stats()
    
    def pause_recording(self):
        """暂停录制"""
        if self.recording:
            self.session.run("""
                unsubscribeTable(tableName=`live_tick_stream, actionName="tickWriter")
                unsubscribeTable(tableName=`live_minute_stream, actionName="minuteWriter")
            """)
            print("录制已暂停")
    
    def resume_recording(self):
        """恢复录制"""
        if self.recording:
            self.session.run("""
                subscribeTable(tableName=`live_tick_stream, actionName="tickWriter", 
                    handler=tick_writer, msgAsTable=true, offset=-1, batchSize=1000)
                subscribeTable(tableName=`live_minute_stream, actionName="minuteWriter",
                    handler=minute_writer, msgAsTable=true, offset=-1, batchSize=100)
            """)
            print("录制已恢复")
    
    def inject_test_data(self, symbol_count: int = 100, duration_minutes: int = 60, tick_rate: int = 10):
        """注入测试数据"""
        print(f"开始注入测试数据: {symbol_count}品种, {duration_minutes}分钟, {tick_rate}tick/s")
        
        symbols = [f"TEST{i:03d}" for i in range(symbol_count)]
        start_time = datetime.now()
        total_ticks = 0
        
        for minute in range(duration_minutes):
            for second in range(60):
                for tick in range(tick_rate):
                    if not self.recording:
                        break
                    
                    # 轮询选择品种
                    symbol = symbols[(total_ticks) % symbol_count]
                    
                    # 生成模拟数据
                    timestamp = start_time + timedelta(minutes=minute, seconds=second, 
                                                     milliseconds=tick * (1000 // tick_rate))
                    price = 100 + (total_ticks % 100) * 0.01
                    volume = 100 + (total_ticks % 50)
                    
                    # 插入数据
                    timestamp_str = timestamp.strftime("%Y.%m.%d %H:%M:%S.%f")[:-3]
                    
                    self.session.run(f"""
                        insert into live_tick_stream values(
                            {timestamp_str}, `{symbol}, {price}, {volume},
                            {price - 0.01}, {price + 0.01}, {volume//2}, {volume//2},
                            {price * volume}, {1000 + total_ticks % 100}
                        )
                    """)
                    
                    total_ticks += 1
                    
                    # 控制速率
                    if tick_rate > 100:  # 高频率时需要控制
                        time.sleep(0.001)
                
                if not self.recording:
                    break
            
            if not self.recording:
                break
            
            # 每分钟报告进度
            if minute % 10 == 0:
                print(f"已注入 {minute} 分钟数据, 总计 {total_ticks} ticks")
        
        print(f"测试数据注入完成, 总计 {total_ticks} ticks")
    
    def update_stats(self):
        """更新统计信息"""
        try:
            # 获取tick数据统计
            tick_stats = self.session.run("""
                select count(*) as total_ticks, 
                       count(distinct symbol) as unique_symbols,
                       (max(timestamp) - min(timestamp)) / 1000.0 as duration_seconds
                from live_tick_stream
            """)
            
            if len(tick_stats) > 0:
                self.stats['total_ticks'] = int(tick_stats.iloc[0]['total_ticks'])
                self.stats['unique_symbols'] = int(tick_stats.iloc[0]['unique_symbols'])
                self.stats['duration_seconds'] = float(tick_stats.iloc[0]['duration_seconds'])
            
            # 估算数据大小
            self.stats['data_size_mb'] = self.stats['total_ticks'] * 0.1  # 估算每条记录100字节
            
        except Exception as e:
            print(f"更新统计信息失败: {e}")
    
    def print_stats(self):
        """打印统计信息"""
        print("\n=== 录制统计信息 ===")
        print(f"录制时长: {datetime.now() - self.stats['start_time'] if self.stats['start_time'] else 'N/A'}")
        print(f"总tick数: {self.stats['total_ticks']:,}")
        print(f"品种数量: {self.stats['unique_symbols']}")
        print(f"数据大小: {self.stats['data_size_mb']:.2f} MB")
        
        if self.stats.get('duration_seconds', 0) > 0:
            tps = self.stats['total_ticks'] / self.stats['duration_seconds']
            print(f"平均TPS: {tps:.2f}")
        
        # 内存信息
        memory_info = self.memory_monitor.get_memory_info()
        print(f"内存使用: {memory_info['used_gb']:.2f}GB / {memory_info['total_gb']:.2f}GB ({memory_info['percent']:.1f}%)")
    
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


if __name__ == "__main__":
    # 测试行情录制
    recorder = MarketDataRecorder()
    
    try:
        # 开始录制
        test_symbols = [f"TEST{i:03d}" for i in range(50)]  # 50个测试品种
        recorder.start_recording(test_symbols)
        
        # 注入测试数据
        recorder.inject_test_data(symbol_count=50, duration_minutes=5, tick_rate=20)
        
        # 等待处理完成
        time.sleep(10)
        
        # 停止录制
        recorder.stop_recording()
        
        # 生成报告
        recorder.generate_recording_report("performance_testing/recording_report.json")
        
    except KeyboardInterrupt:
        print("\n用户中断录制")
        recorder.stop_recording()
    except Exception as e:
        print(f"录制过程中发生错误: {e}")
        recorder.stop_recording()
