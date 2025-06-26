"""
全CTP期货市场录制测试 - 1700+合约，8GB RAM限制
测试新的数据库分区策略和内存优化
"""

import sys
import os
import time
import threading
import logging
from datetime import datetime
from typing import List, Dict

# Add paths for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from market_data_recorder import MarketDataRecorder, ContractDiscovery

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class FullCTPRecorder:
    """全CTP期货市场录制器"""
    
    def __init__(self):
        self.recorder = MarketDataRecorder()
        self.contract_discovery = ContractDiscovery()
        self.logger = logger
        
        # 录制状态
        self.recording = False
        self.tick_count = 0
        self.start_time = None
        self.contracts = []
        
        self.logger.info("全CTP录制器初始化完成")
    
    def start_full_ctp_recording(self, duration_minutes: int = 30, tick_rate: int = 3):
        """开始全CTP录制 - 模拟真实CTP频率"""
        try:
            self.logger.info(f"开始全CTP录制，持续时间: {duration_minutes}分钟，频率: {tick_rate} ticks/秒")
            
            # 获取所有期货合约
            self.contracts = self.contract_discovery.get_all_futures_contracts()
            self.logger.info(f"发现 {len(self.contracts)} 个期货合约")
            
            # 显示各交易所合约数量
            self._log_contract_distribution()
            
            # 启动录制器
            self.recorder.start_recording(self.contracts, full_market=True)
            
            # 启动数据注入线程
            self.recording = True
            self.start_time = datetime.now()
            
            # 创建多个数据注入线程以提高效率
            num_threads = 4
            contracts_per_thread = len(self.contracts) // num_threads
            
            threads = []
            for i in range(num_threads):
                start_idx = i * contracts_per_thread
                end_idx = start_idx + contracts_per_thread if i < num_threads - 1 else len(self.contracts)
                thread_contracts = self.contracts[start_idx:end_idx]
                
                thread = threading.Thread(
                    target=self._inject_market_data_thread,
                    args=(thread_contracts, duration_minutes, tick_rate, i)
                )
                thread.daemon = True
                threads.append(thread)
                thread.start()
            
            # 创建监控线程
            monitor_thread = threading.Thread(
                target=self._monitor_recording,
                args=(duration_minutes,)
            )
            monitor_thread.daemon = True
            monitor_thread.start()
            
            # 等待所有线程完成
            for thread in threads:
                thread.join()
            
            monitor_thread.join()
            
            # 停止录制
            self.recorder.stop_recording()
            
            self.logger.info("全CTP录制完成")
            
        except Exception as e:
            self.logger.error(f"全CTP录制失败: {e}")
            raise
    
    def _log_contract_distribution(self):
        """记录合约分布情况"""
        exchanges = {
            'CFFEX': [c for c in self.contracts if c.startswith(('IC', 'IF', 'IH', 'IM', 'T'))],
            'SHFE': [c for c in self.contracts if c.startswith(('rb', 'au', 'ag', 'fu', 'ru', 'cu', 'al', 'zn', 'ni', 'sn', 'pb', 'hc', 'ss', 'bc', 'sp', 'wr', 'bu'))],
            'DCE': [c for c in self.contracts if c.startswith(('p', 'i', 'jm', 'm', 'y', 'eb', 'v', 'pp', 'c', 'cs', 'a', 'b', 'jd', 'l', 'pg', 'rr', 'fb', 'bb', 'lh', 'j'))],
            'CZCE': [c for c in self.contracts if c.startswith(('MA', 'OI', 'TA', 'SH', 'FG', 'SA', 'UR', 'SR', 'CF', 'CY', 'AP', 'CJ', 'PK', 'RM', 'ZC', 'SF', 'SM', 'WH', 'PM', 'RI', 'LR', 'JR'))],
            'INE': [c for c in self.contracts if c.startswith(('sc', 'lu', 'nr', 'bc', 'ec'))]
        }
        
        self.logger.info("合约分布:")
        for exchange, contracts in exchanges.items():
            self.logger.info(f"  {exchange}: {len(contracts)} 个合约")
            if contracts:
                self.logger.info(f"    样本: {', '.join(contracts[:3])}...")
    
    def _inject_market_data_thread(self, symbols: List[str], duration_minutes: int, tick_rate: int, thread_id: int):
        """数据注入线程"""
        self.logger.info(f"线程 {thread_id} 开始处理 {len(symbols)} 个合约")
        
        total_seconds = duration_minutes * 60
        thread_tick_count = 0
        
        for second in range(total_seconds):
            if not self.recording:
                break
            
            for tick in range(tick_rate):
                if not self.recording:
                    break
                
                # 轮询选择合约
                symbol = symbols[thread_tick_count % len(symbols)]
                
                # 生成模拟数据
                current_time = datetime.now()
                price = 100 + (thread_tick_count % 1000) * 0.01
                volume = 100 + (thread_tick_count % 100)
                
                # 获取交易所
                exchange = self._get_exchange_for_symbol(symbol)
                
                try:
                    # 插入数据
                    timestamp_str = f"timestamp('{current_time.strftime('%Y.%m.%dT%H:%M:%S.%f')[:-3]}')"
                    update_time_str = f"timestamp('{current_time.strftime('%Y.%m.%dT%H:%M:%S.%f')[:-3]}')"
                    
                    insert_sql = f"insert into live_tick_stream values({timestamp_str}, `{symbol}, {price}, {volume}, {price - 0.01}, {price + 0.01}, {volume//2}, {volume//2}, {price * volume}, {1000 + thread_tick_count % 100}, `{exchange}, {update_time_str})"
                    
                    self.recorder.session.run(insert_sql)
                    thread_tick_count += 1
                    self.tick_count += 1
                    
                except Exception as e:
                    if thread_tick_count < 5:  # 只记录前几个错误
                        self.logger.error(f"线程 {thread_id} 插入数据失败: {e}")
                
                # 控制频率
                time.sleep(1.0 / tick_rate)
            
            # 每30秒报告一次进度
            if second % 30 == 0 and thread_id == 0:  # 只让主线程报告
                elapsed = datetime.now() - self.start_time
                self.logger.info(f"已运行 {elapsed.total_seconds():.0f}秒, 总计插入 {self.tick_count} 个tick")
        
        self.logger.info(f"线程 {thread_id} 完成，插入 {thread_tick_count} 个tick")
    
    def _monitor_recording(self, duration_minutes: int):
        """监控录制状态"""
        total_seconds = duration_minutes * 60
        
        for i in range(total_seconds // 60):  # 每分钟检查一次
            if not self.recording:
                break
            
            time.sleep(60)
            
            try:
                # 检查流表大小
                stream_size = self.recorder.session.run("size(live_tick_stream)")
                
                # 检查内存使用
                memory_info = self.recorder.memory_monitor.get_memory_info()
                
                # 检查内存趋势
                memory_trend = self.recorder.memory_monitor.get_memory_trend()
                
                # 检查月度数据库中的数据
                try:
                    snapshot_count = self.recorder.session.run("""
                        select count(*) as count from loadTable("dfs://market_data_recording_M", "snapshot") where date(datetime) = today()
                    """)
                    stored_snapshots = int(snapshot_count.iloc[0]['count']) if len(snapshot_count) > 0 else 0
                except:
                    stored_snapshots = 0
                
                self.logger.info(f"监控报告: 流表={stream_size}, 已存储={stored_snapshots}, "
                               f"内存={memory_info['percent']:.1f}%, 趋势={memory_trend['trend']}")
                
                # 如果内存使用过高，触发清理
                if memory_info['percent'] > 60:
                    self.logger.warning("内存使用过高，触发清理")
                    self.recorder.session.run("cleanup_stream_tables(memory_config, false)")
                
            except Exception as e:
                self.logger.error(f"监控检查失败: {e}")
    
    def _get_exchange_for_symbol(self, symbol: str) -> str:
        """根据合约代码获取交易所"""
        if symbol.startswith(('IC', 'IF', 'IH', 'IM', 'T')):
            return "CFFEX"
        elif symbol.startswith(('rb', 'au', 'ag', 'fu', 'ru', 'cu', 'al', 'zn', 'ni', 'sn', 'pb', 'hc', 'ss', 'bc', 'sp', 'wr', 'bu')):
            return "SHFE"
        elif symbol.startswith(('p', 'i', 'jm', 'm', 'y', 'eb', 'v', 'pp', 'c', 'cs', 'a', 'b', 'jd', 'l', 'pg', 'rr', 'fb', 'bb', 'lh', 'j')):
            return "DCE"
        elif symbol.startswith(('MA', 'OI', 'TA', 'SH', 'FG', 'SA', 'UR', 'SR', 'CF', 'CY', 'AP', 'CJ', 'PK', 'RM', 'ZC', 'SF', 'SM', 'WH', 'PM', 'RI', 'LR', 'JR')):
            return "CZCE"
        elif symbol.startswith(('sc', 'lu', 'nr', 'bc', 'ec')):
            return "INE"
        else:
            return "UNKNOWN"
    
    def get_final_stats(self) -> Dict:
        """获取最终统计"""
        try:
            # 更新录制器统计
            self.recorder.update_stats()
            
            # 获取流表统计
            stream_size = self.recorder.session.run("size(live_tick_stream)")
            
            # 获取月度数据库统计
            try:
                snapshot_stats = self.recorder.session.run("""
                    select count(*) as count, count(distinct symbol) as symbols 
                    from loadTable("dfs://market_data_recording_M", "snapshot") 
                    where date(datetime) = today()
                """)
                stored_snapshots = int(snapshot_stats.iloc[0]['count']) if len(snapshot_stats) > 0 else 0
                stored_symbols = int(snapshot_stats.iloc[0]['symbols']) if len(snapshot_stats) > 0 else 0
            except Exception as e:
                self.logger.warning(f"获取月度数据库统计失败: {e}")
                stored_snapshots = 0
                stored_symbols = 0
            
            # 计算运行时间
            duration = datetime.now() - self.start_time if self.start_time else None
            
            stats = {
                'total_contracts': len(self.contracts),
                'injected_ticks': self.tick_count,
                'stream_table_size': stream_size,
                'stored_snapshots': stored_snapshots,
                'stored_symbols': stored_symbols,
                'duration': duration,
                'avg_tps': self.tick_count / duration.total_seconds() if duration else 0,
                'memory_info': self.recorder.memory_monitor.get_memory_info(),
                'recorder_stats': self.recorder.stats
            }
            
            return stats
            
        except Exception as e:
            self.logger.error(f"获取统计失败: {e}")
            return {}


def run_full_ctp_test(duration_minutes: int = 10, tick_rate: int = 3):
    """运行全CTP测试"""
    logger.info(f"开始全CTP期货录制测试，持续时间: {duration_minutes}分钟，频率: {tick_rate} ticks/秒")
    
    # 清理环境
    logger.info("清理DolphinDB环境...")
    try:
        import subprocess
        subprocess.run([sys.executable, "cleanup_market_data.py"], check=True, cwd=os.path.dirname(__file__))
    except Exception as e:
        logger.warning(f"清理失败: {e}")
    
    # 创建录制器
    recorder = FullCTPRecorder()
    
    try:
        # 开始录制
        recorder.start_full_ctp_recording(duration_minutes, tick_rate)
        
        # 获取最终统计
        stats = recorder.get_final_stats()
        
        # 打印统计报告
        logger.info("\n" + "="*80)
        logger.info("全CTP期货录制测试完成")
        logger.info("="*80)
        
        for key, value in stats.items():
            if key != 'recorder_stats':
                logger.info(f"{key}: {value}")
        
        # 保存报告
        import json
        report_file = f"reports/full_ctp_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        # 确保reports目录存在
        os.makedirs("reports", exist_ok=True)
        
        # 转换datetime对象为字符串
        def convert_for_json(obj):
            if isinstance(obj, datetime):
                return obj.isoformat()
            elif hasattr(obj, 'total_seconds'):  # timedelta
                return obj.total_seconds()
            elif isinstance(obj, dict):
                return {k: convert_for_json(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_for_json(item) for item in obj]
            else:
                return str(obj) if not isinstance(obj, (int, float, str, bool, type(None))) else obj
        
        json_stats = convert_for_json(stats)
        
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(json_stats, f, indent=2, ensure_ascii=False)
        
        logger.info(f"报告已保存到: {report_file}")
        logger.info("="*80)
        
    except KeyboardInterrupt:
        logger.info("用户中断测试")
    except Exception as e:
        logger.error(f"测试失败: {e}")
        raise


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="全CTP期货数据录制测试")
    parser.add_argument("--duration", type=int, default=5, help="测试持续时间(分钟)")
    parser.add_argument("--tick-rate", type=int, default=3, help="每秒tick数量")
    parser.add_argument("--cleanup", action="store_true", help="测试前清理DolphinDB")
    
    args = parser.parse_args()
    
    if args.cleanup:
        logger.info("执行清理...")
        try:
            import subprocess
            subprocess.run([sys.executable, "cleanup_market_data.py"], check=True)
        except Exception as e:
            logger.error(f"清理失败: {e}")
    
    run_full_ctp_test(args.duration, args.tick_rate)
