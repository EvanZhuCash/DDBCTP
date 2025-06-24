"""
性能测试运行器 - 集成所有性能测试模块
统一运行延迟测试、行情录制测试、系统健康监控
"""

import time
import threading
import json
import os
from datetime import datetime
from typing import Dict, List
import argparse

from latency_test_framework import LatencyTestFramework
from market_data_recorder import MarketDataRecorder
from system_health_monitor import SystemHealthMonitor


class PerformanceTestRunner:
    """性能测试运行器"""
    
    def __init__(self, output_dir: str = "performance_testing/results"):
        self.output_dir = output_dir
        self.ensure_output_dir()
        
        # 初始化测试模块
        self.latency_tester = LatencyTestFramework()
        self.data_recorder = MarketDataRecorder()
        self.health_monitor = SystemHealthMonitor()
        
        # 测试结果
        self.test_results = {}
        self.test_start_time = None
        self.test_end_time = None
    
    def ensure_output_dir(self):
        """确保输出目录存在"""
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
    
    def run_comprehensive_test(self, test_config: Dict):
        """运行综合性能测试"""
        print("=" * 60)
        print("开始综合性能测试")
        print("=" * 60)
        
        self.test_start_time = datetime.now()
        
        try:
            # 1. 启动系统健康监控
            print("\n1. 启动系统健康监控...")
            self.health_monitor.start_monitoring(interval=10.0)
            
            # 2. 运行延迟测试
            if test_config.get('run_latency_test', True):
                print("\n2. 运行延迟测试...")
                latency_results = self._run_latency_test(test_config.get('latency_config', {}))
                self.test_results['latency_test'] = latency_results
            
            # 3. 运行行情录制测试
            if test_config.get('run_recording_test', True):
                print("\n3. 运行行情录制测试...")
                recording_results = self._run_recording_test(test_config.get('recording_config', {}))
                self.test_results['recording_test'] = recording_results
            
            # 4. 运行压力测试
            if test_config.get('run_stress_test', True):
                print("\n4. 运行压力测试...")
                stress_results = self._run_stress_test(test_config.get('stress_config', {}))
                self.test_results['stress_test'] = stress_results
            
            # 5. 生成综合报告
            print("\n5. 生成综合报告...")
            self._generate_comprehensive_report()
            
        except Exception as e:
            print(f"测试过程中发生错误: {e}")
            
        finally:
            # 停止监控
            self.health_monitor.stop_monitoring()
            self.test_end_time = datetime.now()
            
            print(f"\n测试完成，总耗时: {self.test_end_time - self.test_start_time}")
    
    def _run_latency_test(self, config: Dict) -> Dict:
        """运行延迟测试"""
        symbol_counts = config.get('symbol_counts', [1, 10, 50])
        strategy_counts = config.get('strategy_counts', [1, 3])
        duration = config.get('duration', 30)
        tick_rate = config.get('tick_rate', 50)
        
        print(f"  延迟测试配置: {symbol_counts}品种, {strategy_counts}策略, {duration}秒, {tick_rate}TPS")
        
        results = self.latency_tester.run_latency_test(
            symbol_counts=symbol_counts,
            strategy_counts=strategy_counts,
            duration=duration,
            tick_rate=tick_rate
        )
        
        # 保存延迟测试报告
        report_file = os.path.join(self.output_dir, "latency_test_report.json")
        self.latency_tester.generate_latency_report(report_file)
        
        return results
    
    def _run_recording_test(self, config: Dict) -> Dict:
        """运行行情录制测试"""
        symbol_count = config.get('symbol_count', 100)
        duration_minutes = config.get('duration_minutes', 10)
        tick_rate = config.get('tick_rate', 20)
        
        print(f"  录制测试配置: {symbol_count}品种, {duration_minutes}分钟, {tick_rate}TPS")
        
        try:
            # 开始录制
            test_symbols = [f"REC{i:03d}" for i in range(symbol_count)]
            self.data_recorder.start_recording(test_symbols)
            
            # 注入测试数据
            self.data_recorder.inject_test_data(
                symbol_count=symbol_count,
                duration_minutes=duration_minutes,
                tick_rate=tick_rate
            )
            
            # 等待处理完成
            time.sleep(10)
            
            # 停止录制
            self.data_recorder.stop_recording()
            
            # 生成录制报告
            report_file = os.path.join(self.output_dir, "recording_report.json")
            recording_report = self.data_recorder.generate_recording_report(report_file)
            
            return {
                'status': 'completed',
                'symbol_count': symbol_count,
                'duration_minutes': duration_minutes,
                'tick_rate': tick_rate,
                'total_ticks': self.data_recorder.stats['total_ticks'],
                'data_size_mb': self.data_recorder.stats['data_size_mb']
            }
            
        except Exception as e:
            return {
                'status': 'failed',
                'error': str(e)
            }
    
    def _run_stress_test(self, config: Dict) -> Dict:
        """运行压力测试"""
        max_symbols = config.get('max_symbols', 200)
        max_strategies = config.get('max_strategies', 10)
        test_duration = config.get('test_duration', 300)  # 5分钟
        
        print(f"  压力测试配置: 最大{max_symbols}品种, {max_strategies}策略, {test_duration}秒")
        
        stress_results = []
        
        # 逐步增加负载
        symbol_steps = [10, 25, 50, 100, max_symbols]
        strategy_steps = [1, 3, 5, max_strategies]
        
        for symbols in symbol_steps:
            if symbols > max_symbols:
                break
                
            for strategies in strategy_steps:
                if strategies > max_strategies:
                    break
                
                print(f"    测试负载: {symbols}品种, {strategies}策略")
                
                try:
                    # 运行短时间的延迟测试
                    result = self.latency_tester.run_latency_test(
                        symbol_counts=[symbols],
                        strategy_counts=[strategies],
                        duration=30,  # 30秒测试
                        tick_rate=100
                    )
                    
                    # 获取系统资源使用情况
                    health_results = self.health_monitor.perform_health_checks()
                    
                    stress_result = {
                        'symbols': symbols,
                        'strategies': strategies,
                        'latency_result': result.get(f"{symbols}_symbols_{strategies}_strategies", {}),
                        'system_health': [r.to_dict() for r in health_results],
                        'timestamp': datetime.now().isoformat()
                    }
                    
                    stress_results.append(stress_result)
                    
                    # 检查是否达到系统极限
                    memory_usage = next((r.details.get('memory_percent', 0) for r in health_results if r.component == 'memory'), 0)
                    if memory_usage > 85:
                        print(f"    内存使用率过高 ({memory_usage:.1f}%)，停止增加负载")
                        break
                    
                except Exception as e:
                    print(f"    压力测试失败: {e}")
                    stress_results.append({
                        'symbols': symbols,
                        'strategies': strategies,
                        'error': str(e),
                        'timestamp': datetime.now().isoformat()
                    })
                    break
            
            # 如果内存使用率过高，停止测试
            if stress_results and 'error' in stress_results[-1]:
                break
        
        return {
            'max_tested_symbols': max([r.get('symbols', 0) for r in stress_results if 'error' not in r]),
            'max_tested_strategies': max([r.get('strategies', 0) for r in stress_results if 'error' not in r]),
            'detailed_results': stress_results
        }
    
    def _generate_comprehensive_report(self):
        """生成综合报告"""
        # 生成系统健康报告
        health_report_file = os.path.join(self.output_dir, "health_report.json")
        health_report = self.health_monitor.generate_health_report(health_report_file)
        
        # 综合报告
        comprehensive_report = {
            'test_session': {
                'start_time': self.test_start_time.isoformat() if self.test_start_time else None,
                'end_time': self.test_end_time.isoformat() if self.test_end_time else None,
                'duration_minutes': (self.test_end_time - self.test_start_time).total_seconds() / 60 if self.test_start_time and self.test_end_time else 0
            },
            'test_results': self.test_results,
            'system_health': health_report,
            'performance_summary': self._generate_performance_summary(),
            'recommendations': self._generate_recommendations()
        }
        
        # 保存综合报告
        report_file = os.path.join(self.output_dir, "comprehensive_performance_report.json")
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(comprehensive_report, f, indent=2, ensure_ascii=False, default=str)
        
        print(f"综合性能报告已保存到: {report_file}")
        
        # 打印摘要
        self._print_summary(comprehensive_report)
    
    def _generate_performance_summary(self) -> Dict:
        """生成性能摘要"""
        summary = {}
        
        # 延迟测试摘要
        if 'latency_test' in self.test_results:
            latency_results = self.test_results['latency_test']
            if latency_results:
                avg_latencies = [result.get('avg_latency_us', 0) for result in latency_results.values()]
                p99_latencies = [result.get('p99_latency_us', 0) for result in latency_results.values()]
                throughputs = [result.get('throughput_tps', 0) for result in latency_results.values()]
                
                summary['latency'] = {
                    'best_avg_latency_us': min(avg_latencies) if avg_latencies else 0,
                    'worst_avg_latency_us': max(avg_latencies) if avg_latencies else 0,
                    'best_p99_latency_us': min(p99_latencies) if p99_latencies else 0,
                    'worst_p99_latency_us': max(p99_latencies) if p99_latencies else 0,
                    'max_throughput_tps': max(throughputs) if throughputs else 0
                }
        
        # 录制测试摘要
        if 'recording_test' in self.test_results:
            recording_result = self.test_results['recording_test']
            summary['recording'] = {
                'total_ticks_recorded': recording_result.get('total_ticks', 0),
                'data_size_mb': recording_result.get('data_size_mb', 0),
                'recording_rate_tps': recording_result.get('total_ticks', 0) / (recording_result.get('duration_minutes', 1) * 60)
            }
        
        # 压力测试摘要
        if 'stress_test' in self.test_results:
            stress_result = self.test_results['stress_test']
            summary['stress'] = {
                'max_symbols_supported': stress_result.get('max_tested_symbols', 0),
                'max_strategies_supported': stress_result.get('max_tested_strategies', 0)
            }
        
        return summary
    
    def _generate_recommendations(self) -> List[str]:
        """生成优化建议"""
        recommendations = []
        
        # 基于延迟测试的建议
        if 'latency_test' in self.test_results:
            latency_results = self.test_results['latency_test']
            if latency_results:
                worst_latency = max([result.get('p99_latency_us', 0) for result in latency_results.values()])
                if worst_latency > 10000:  # 10ms
                    recommendations.append("P99延迟超过10ms，建议优化计算逻辑或增加硬件资源")
                elif worst_latency > 5000:  # 5ms
                    recommendations.append("P99延迟较高，建议监控系统负载并考虑优化")
        
        # 基于录制测试的建议
        if 'recording_test' in self.test_results:
            recording_result = self.test_results['recording_test']
            if recording_result.get('status') == 'failed':
                recommendations.append("行情录制测试失败，建议检查存储配置和内存设置")
        
        # 基于压力测试的建议
        if 'stress_test' in self.test_results:
            stress_result = self.test_results['stress_test']
            max_symbols = stress_result.get('max_tested_symbols', 0)
            if max_symbols < 100:
                recommendations.append("系统支持的最大品种数较少，建议优化内存使用或升级硬件")
            elif max_symbols < 200:
                recommendations.append("系统性能良好，但在高负载下需要监控资源使用")
        
        if not recommendations:
            recommendations.append("系统性能表现良好，建议定期进行性能测试以监控变化")
        
        return recommendations
    
    def _print_summary(self, report: Dict):
        """打印测试摘要"""
        print("\n" + "=" * 60)
        print("性能测试摘要")
        print("=" * 60)
        
        summary = report.get('performance_summary', {})
        
        # 延迟摘要
        if 'latency' in summary:
            latency = summary['latency']
            print(f"\n延迟测试:")
            print(f"  最佳平均延迟: {latency.get('best_avg_latency_us', 0):.2f} μs")
            print(f"  最差平均延迟: {latency.get('worst_avg_latency_us', 0):.2f} μs")
            print(f"  最佳P99延迟: {latency.get('best_p99_latency_us', 0):.2f} μs")
            print(f"  最差P99延迟: {latency.get('worst_p99_latency_us', 0):.2f} μs")
            print(f"  最大吞吐量: {latency.get('max_throughput_tps', 0):.2f} TPS")
        
        # 录制摘要
        if 'recording' in summary:
            recording = summary['recording']
            print(f"\n录制测试:")
            print(f"  录制tick数: {recording.get('total_ticks_recorded', 0):,}")
            print(f"  数据大小: {recording.get('data_size_mb', 0):.2f} MB")
            print(f"  录制速率: {recording.get('recording_rate_tps', 0):.2f} TPS")
        
        # 压力测试摘要
        if 'stress' in summary:
            stress = summary['stress']
            print(f"\n压力测试:")
            print(f"  最大支持品种数: {stress.get('max_symbols_supported', 0)}")
            print(f"  最大支持策略数: {stress.get('max_strategies_supported', 0)}")
        
        # 建议
        recommendations = report.get('recommendations', [])
        if recommendations:
            print(f"\n优化建议:")
            for i, rec in enumerate(recommendations, 1):
                print(f"  {i}. {rec}")
        
        print("\n" + "=" * 60)


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='DolphinDB CTP 性能测试工具')
    parser.add_argument('--config', type=str, help='测试配置文件路径')
    parser.add_argument('--output', type=str, default='performance_testing/results', help='输出目录')
    parser.add_argument('--latency-only', action='store_true', help='只运行延迟测试')
    parser.add_argument('--recording-only', action='store_true', help='只运行录制测试')
    parser.add_argument('--stress-only', action='store_true', help='只运行压力测试')
    
    args = parser.parse_args()
    
    # 默认测试配置
    default_config = {
        'run_latency_test': True,
        'run_recording_test': True,
        'run_stress_test': True,
        'latency_config': {
            'symbol_counts': [1, 10, 50],
            'strategy_counts': [1, 3],
            'duration': 30,
            'tick_rate': 50
        },
        'recording_config': {
            'symbol_count': 100,
            'duration_minutes': 10,
            'tick_rate': 20
        },
        'stress_config': {
            'max_symbols': 200,
            'max_strategies': 10,
            'test_duration': 300
        }
    }
    
    # 加载配置文件
    if args.config and os.path.exists(args.config):
        with open(args.config, 'r', encoding='utf-8') as f:
            config = json.load(f)
    else:
        config = default_config
    
    # 根据命令行参数调整配置
    if args.latency_only:
        config['run_recording_test'] = False
        config['run_stress_test'] = False
    elif args.recording_only:
        config['run_latency_test'] = False
        config['run_stress_test'] = False
    elif args.stress_only:
        config['run_latency_test'] = False
        config['run_recording_test'] = False
    
    # 运行测试
    runner = PerformanceTestRunner(output_dir=args.output)
    runner.run_comprehensive_test(config)


if __name__ == "__main__":
    main()
