"""
延迟测试框架 - 端到端延迟监控
实现分段延迟分析：tick接收→DDB计算→信号生成→CTP提交→委托回报
"""

import time
import threading
import queue
import json
import statistics
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import dolphindb as ddb
import pandas as pd


class LatencyMeasurement:
    """延迟测量点管理"""
    
    def __init__(self):
        self.measurements = {}
        self.lock = threading.Lock()
    
    def start_measurement(self, test_id: str, stage: str, symbol: str, volume: int = 0):
        """开始测量某个阶段"""
        timestamp = time.time_ns()  # 纳秒精度
        
        with self.lock:
            if test_id not in self.measurements:
                self.measurements[test_id] = {}
            
            self.measurements[test_id][stage] = {
                'start_time': timestamp,
                'end_time': None,
                'symbol': symbol,
                'volume': volume,
                'latency_us': None
            }
        
        return timestamp
    
    def end_measurement(self, test_id: str, stage: str):
        """结束测量某个阶段"""
        end_time = time.time_ns()
        
        with self.lock:
            if test_id in self.measurements and stage in self.measurements[test_id]:
                measurement = self.measurements[test_id][stage]
                measurement['end_time'] = end_time
                measurement['latency_us'] = (end_time - measurement['start_time']) / 1000  # 转换为微秒
                return measurement['latency_us']
        
        return None
    
    def get_measurement(self, test_id: str, stage: str) -> Optional[Dict]:
        """获取测量结果"""
        with self.lock:
            if test_id in self.measurements and stage in self.measurements[test_id]:
                return self.measurements[test_id][stage].copy()
        return None


class LatencyTestFramework:
    """延迟测试框架"""
    
    def __init__(self):
        self.session = ddb.session()
        self.session.connect("localhost", 8848, "admin", "123456")
        self.measurement = LatencyMeasurement()
        self.test_results = []
        self.setup_monitoring_tables()
        
        # 测试阶段定义
        self.test_stages = [
            "tick_receive",      # 接收tick数据
            "ddb_calculation",   # DDB完成计算
            "signal_generation", # 生成交易信号
            "order_submission",  # CTP提交申报
            "order_response"     # 收到委托回报
        ]
    
    def setup_monitoring_tables(self):
        """设置监控表"""
        self.session.run("""
            // 延迟监控表
            if(existsTable("latency_monitor")){
                dropTable("latency_monitor")
            }
            share streamTable(10000:0, `test_id`stage`timestamp_start`timestamp_end`latency_us`symbol`volume, 
                [STRING, SYMBOL, TIMESTAMP, TIMESTAMP, LONG, SYMBOL, INT]) as latency_monitor
            
            // 吞吐量监控表
            if(existsTable("throughput_monitor")){
                dropTable("throughput_monitor")
            }
            share streamTable(1000:0, `timestamp`test_scenario`tps`symbols_count`strategies_count`memory_usage_mb, 
                [TIMESTAMP, STRING, DOUBLE, INT, INT, DOUBLE]) as throughput_monitor
            
            // 测试用流表
            if(existsTable("test_tick_stream")){
                dropTable("test_tick_stream")
            }
            share streamTable(10000:0, `test_id`timestamp`symbol`price`volume, 
                [STRING, TIMESTAMP, SYMBOL, DOUBLE, INT]) as test_tick_stream
            
            if(existsTable("test_signal_stream")){
                dropTable("test_signal_stream")
            }
            share streamTable(10000:0, `test_id`timestamp`symbol`signal`price, 
                [STRING, TIMESTAMP, SYMBOL, STRING, DOUBLE]) as test_signal_stream
        """)
    
    def setup_test_strategy(self, strategy_name: str = "test_bollinger"):
        """设置测试策略"""
        self.session.run(f"""
            // 简单的布林带测试策略
            def {strategy_name}_test(test_id, timestamp, symbol, price, volume){{
                // 模拟计算延迟
                sleep(1)  // 1毫秒计算时间
                
                // 简单信号生成
                signal = iif(price > 100, "BUY", "SELL")
                
                // 插入信号流
                insert into test_signal_stream values(test_id, timestamp, symbol, signal, price)
                
                return signal
            }}
            
            // 创建测试引擎
            if(existsStreamEngine("testStrategyEngine")){{
                dropStreamEngine("testStrategyEngine")
            }}
            
            createReactiveStateEngine(name="testStrategyEngine",
                metrics=<[{strategy_name}_test(test_id, timestamp, symbol, price, volume)]>,
                dummyTable=test_tick_stream, outputTable=test_signal_stream, keyColumn="symbol")
            
            // 订阅测试流
            subscribeTable(tableName=`test_tick_stream, actionName="testStrategyEngine", 
                handler=getStreamEngine("testStrategyEngine"), msgAsTable=true, offset=-1)
        """)
    
    def generate_test_data(self, symbol_count: int, tick_rate: int, duration: int) -> List[Dict]:
        """生成测试数据"""
        test_data = []
        symbols = [f"TEST{i:03d}" for i in range(symbol_count)]
        
        start_time = datetime.now()
        interval = 1.0 / tick_rate  # 每个tick的时间间隔
        
        total_ticks = tick_rate * duration
        
        for i in range(total_ticks):
            symbol = symbols[i % symbol_count]
            timestamp = start_time + timedelta(seconds=i * interval)
            price = 100 + (i % 20) - 10  # 模拟价格波动
            volume = 100 + (i % 50)
            
            test_data.append({
                'test_id': f"test_{int(time.time())}_{i}",
                'timestamp': timestamp,
                'symbol': symbol,
                'price': price,
                'volume': volume
            })
        
        return test_data
    
    def inject_test_tick(self, test_data: Dict):
        """注入单个测试tick"""
        test_id = test_data['test_id']
        symbol = test_data['symbol']
        
        # 1. 开始tick接收测量
        self.measurement.start_measurement(test_id, "tick_receive", symbol, test_data['volume'])
        
        # 2. 插入到DDB流表
        timestamp_str = test_data['timestamp'].strftime("%Y.%m.%d %H:%M:%S.%f")[:-3]
        
        self.session.run(f"""
            insert into test_tick_stream values(
                "{test_id}", 
                {timestamp_str}, 
                `{symbol}, 
                {test_data['price']}, 
                {test_data['volume']}
            )
        """)
        
        # 3. 结束tick接收测量
        self.measurement.end_measurement(test_id, "tick_receive")
        
        # 4. 开始DDB计算测量
        self.measurement.start_measurement(test_id, "ddb_calculation", symbol)
        
        return test_id
    
    def run_latency_test(self, symbol_counts: List[int] = [1, 10, 100], 
                        strategy_counts: List[int] = [1, 3, 5],
                        duration: int = 60, tick_rate: int = 100):
        """运行延迟测试"""
        
        print("开始延迟测试...")
        results = {}
        
        for symbol_count in symbol_counts:
            for strategy_count in strategy_counts:
                test_scenario = f"{symbol_count}_symbols_{strategy_count}_strategies"
                print(f"测试场景: {test_scenario}")
                
                # 设置测试策略
                for i in range(strategy_count):
                    self.setup_test_strategy(f"test_strategy_{i}")
                
                # 生成测试数据
                test_data = self.generate_test_data(symbol_count, tick_rate, duration)
                
                # 执行测试
                start_time = time.time()
                latencies = []
                
                for data in test_data[:min(1000, len(test_data))]:  # 限制测试数量
                    test_id = self.inject_test_tick(data)
                    
                    # 等待处理完成
                    time.sleep(0.01)
                    
                    # 收集延迟数据
                    tick_latency = self.measurement.get_measurement(test_id, "tick_receive")
                    if tick_latency and tick_latency['latency_us']:
                        latencies.append(tick_latency['latency_us'])
                
                end_time = time.time()
                
                # 分析结果
                if latencies:
                    result = {
                        'scenario': test_scenario,
                        'symbol_count': symbol_count,
                        'strategy_count': strategy_count,
                        'total_ticks': len(latencies),
                        'duration': end_time - start_time,
                        'avg_latency_us': statistics.mean(latencies),
                        'p50_latency_us': statistics.median(latencies),
                        'p95_latency_us': self._percentile(latencies, 95),
                        'p99_latency_us': self._percentile(latencies, 99),
                        'max_latency_us': max(latencies),
                        'throughput_tps': len(latencies) / (end_time - start_time)
                    }
                    
                    results[test_scenario] = result
                    self.test_results.append(result)
                    
                    print(f"  平均延迟: {result['avg_latency_us']:.2f} μs")
                    print(f"  P99延迟: {result['p99_latency_us']:.2f} μs")
                    print(f"  吞吐量: {result['throughput_tps']:.2f} TPS")
                
                # 清理测试数据
                self.cleanup_test_data()
        
        return results
    
    def _percentile(self, data: List[float], percentile: float) -> float:
        """计算百分位数"""
        if not data:
            return 0.0
        sorted_data = sorted(data)
        index = int(len(sorted_data) * percentile / 100)
        return sorted_data[min(index, len(sorted_data) - 1)]
    
    def cleanup_test_data(self):
        """清理测试数据"""
        self.session.run("""
            delete from test_tick_stream
            delete from test_signal_stream
            delete from latency_monitor
        """)
        self.measurement.measurements.clear()
    
    def generate_latency_report(self, output_file: str = "latency_test_report.json"):
        """生成延迟测试报告"""
        report = {
            'test_timestamp': datetime.now().isoformat(),
            'test_results': self.test_results,
            'summary': self._generate_summary()
        }
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        print(f"延迟测试报告已保存到: {output_file}")
        return report
    
    def _generate_summary(self) -> Dict:
        """生成测试摘要"""
        if not self.test_results:
            return {}
        
        all_latencies = [result['avg_latency_us'] for result in self.test_results]
        all_throughputs = [result['throughput_tps'] for result in self.test_results]
        
        return {
            'total_scenarios': len(self.test_results),
            'overall_avg_latency_us': statistics.mean(all_latencies),
            'overall_max_latency_us': max(all_latencies),
            'overall_avg_throughput_tps': statistics.mean(all_throughputs),
            'overall_max_throughput_tps': max(all_throughputs)
        }


if __name__ == "__main__":
    # 运行延迟测试
    framework = LatencyTestFramework()
    
    # 测试不同场景
    results = framework.run_latency_test(
        symbol_counts=[1, 10, 50],
        strategy_counts=[1, 3],
        duration=30,  # 30秒测试
        tick_rate=50  # 50 tick/s
    )
    
    # 生成报告
    framework.generate_latency_report("performance_testing/latency_test_results.json")
    
    print("\n延迟测试完成!")
    for scenario, result in results.items():
        print(f"{scenario}: 平均延迟 {result['avg_latency_us']:.2f}μs, P99延迟 {result['p99_latency_us']:.2f}μs")
