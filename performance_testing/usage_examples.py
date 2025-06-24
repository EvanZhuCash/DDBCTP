"""
使用示例 - 演示如何使用各个性能测试模块
"""

import time
import json
from datetime import datetime

def example_latency_test():
    """延迟测试示例"""
    print("=" * 50)
    print("延迟测试示例")
    print("=" * 50)
    
    from latency_test_framework import LatencyTestFramework
    
    # 创建延迟测试框架
    framework = LatencyTestFramework()
    
    print("1. 设置测试策略...")
    framework.setup_test_strategy("example_strategy")
    
    print("2. 运行延迟测试...")
    # 运行小规模测试
    results = framework.run_latency_test(
        symbol_counts=[1, 5],      # 测试1个和5个品种
        strategy_counts=[1],       # 1个策略
        duration=10,               # 10秒测试
        tick_rate=20               # 20 tick/s
    )
    
    print("3. 测试结果:")
    for scenario, result in results.items():
        print(f"  场景: {scenario}")
        print(f"    平均延迟: {result.get('avg_latency_us', 0):.2f} μs")
        print(f"    P99延迟: {result.get('p99_latency_us', 0):.2f} μs")
        print(f"    吞吐量: {result.get('throughput_tps', 0):.2f} TPS")
    
    print("4. 生成报告...")
    framework.generate_latency_report("performance_testing/example_latency_report.json")
    
    print("延迟测试示例完成!\n")

def example_recording_test():
    """行情录制测试示例"""
    print("=" * 50)
    print("行情录制测试示例")
    print("=" * 50)
    
    from market_data_recorder import MarketDataRecorder
    
    # 创建录制器
    recorder = MarketDataRecorder()
    
    try:
        print("1. 开始录制...")
        test_symbols = [f"EXAMPLE{i:02d}" for i in range(10)]  # 10个测试品种
        recorder.start_recording(test_symbols)
        
        print("2. 注入测试数据...")
        recorder.inject_test_data(
            symbol_count=10,        # 10个品种
            duration_minutes=2,     # 2分钟
            tick_rate=10           # 10 tick/s
        )
        
        print("3. 等待处理完成...")
        time.sleep(5)
        
        print("4. 停止录制...")
        recorder.stop_recording()
        
        print("5. 生成报告...")
        recorder.generate_recording_report("performance_testing/example_recording_report.json")
        
        print("行情录制测试示例完成!\n")
        
    except Exception as e:
        print(f"录制测试失败: {e}")
        recorder.stop_recording()

def example_health_monitoring():
    """系统健康监控示例"""
    print("=" * 50)
    print("系统健康监控示例")
    print("=" * 50)
    
    from system_health_monitor import SystemHealthMonitor
    
    # 创建健康监控器
    monitor = SystemHealthMonitor()
    
    try:
        print("1. 执行健康检查...")
        health_results = monitor.perform_health_checks()
        
        print("2. 健康检查结果:")
        for result in health_results:
            status_icon = "✓" if result.status == "HEALTHY" else "⚠" if result.status == "WARNING" else "✗"
            print(f"  {status_icon} {result.component}: {result.status} - {result.message}")
        
        print("3. 启动监控 (10秒)...")
        monitor.start_monitoring(interval=5.0)
        
        # 运行10秒
        time.sleep(10)
        
        print("4. 停止监控...")
        monitor.stop_monitoring()
        
        print("5. 生成健康报告...")
        monitor.generate_health_report("performance_testing/example_health_report.json")
        
        print("系统健康监控示例完成!\n")
        
    except Exception as e:
        print(f"健康监控失败: {e}")
        monitor.stop_monitoring()

def example_comprehensive_test():
    """综合测试示例"""
    print("=" * 50)
    print("综合测试示例")
    print("=" * 50)
    
    from performance_test_runner import PerformanceTestRunner
    
    # 创建测试运行器
    runner = PerformanceTestRunner(output_dir="performance_testing/example_results")
    
    # 定义测试配置
    test_config = {
        'run_latency_test': True,
        'run_recording_test': True,
        'run_stress_test': False,  # 跳过压力测试以节省时间
        'latency_config': {
            'symbol_counts': [1, 5],
            'strategy_counts': [1],
            'duration': 15,
            'tick_rate': 20
        },
        'recording_config': {
            'symbol_count': 20,
            'duration_minutes': 2,
            'tick_rate': 15
        }
    }
    
    print("1. 运行综合测试...")
    runner.run_comprehensive_test(test_config)
    
    print("综合测试示例完成!\n")

def example_custom_alert():
    """自定义警报示例"""
    print("=" * 50)
    print("自定义警报示例")
    print("=" * 50)
    
    from system_health_monitor import AlertManager, HealthCheckResult
    
    # 创建警报管理器
    alert_manager = AlertManager()
    
    print("1. 添加自定义警报规则...")
    
    # 添加自定义警报规则
    def custom_alert_condition(health_results):
        """自定义警报条件：检查是否有任何组件状态为WARNING"""
        return any(r.status == "WARNING" for r in health_results)
    
    alert_manager.add_alert_rule(
        "custom_warning_alert",
        custom_alert_condition,
        "WARNING",
        "检测到系统组件警告状态"
    )
    
    print("2. 添加控制台通知渠道...")
    alert_manager.add_notification_channel("console", {})
    
    print("3. 模拟健康检查结果...")
    # 创建模拟的健康检查结果
    mock_results = [
        HealthCheckResult("cpu", "HEALTHY", "CPU使用率正常: 45.2%"),
        HealthCheckResult("memory", "WARNING", "内存使用率较高: 82.1%"),  # 这会触发警报
        HealthCheckResult("disk", "HEALTHY", "磁盘使用率正常: 65.3%")
    ]
    
    print("4. 检查警报...")
    triggered_alerts = alert_manager.check_alerts(mock_results)
    
    if triggered_alerts:
        print(f"触发了 {len(triggered_alerts)} 个警报")
        for alert in triggered_alerts:
            print(f"  警报: {alert['rule_name']} - {alert['message']}")
    else:
        print("没有触发警报")
    
    print("自定义警报示例完成!\n")

def example_performance_analysis():
    """性能分析示例"""
    print("=" * 50)
    print("性能分析示例")
    print("=" * 50)
    
    # 模拟性能测试结果
    mock_results = {
        "1_symbols_1_strategies": {
            "avg_latency_us": 245.6,
            "p99_latency_us": 892.3,
            "throughput_tps": 18.7
        },
        "5_symbols_1_strategies": {
            "avg_latency_us": 567.8,
            "p99_latency_us": 1234.5,
            "throughput_tps": 16.2
        },
        "10_symbols_1_strategies": {
            "avg_latency_us": 1023.4,
            "p99_latency_us": 2456.7,
            "throughput_tps": 14.8
        }
    }
    
    print("1. 分析延迟趋势...")
    scenarios = list(mock_results.keys())
    latencies = [mock_results[s]["avg_latency_us"] for s in scenarios]
    
    print("  场景延迟对比:")
    for scenario, latency in zip(scenarios, latencies):
        symbol_count = int(scenario.split('_')[0])
        print(f"    {symbol_count:2d} 品种: {latency:7.2f} μs")
    
    print("2. 分析吞吐量趋势...")
    throughputs = [mock_results[s]["throughput_tps"] for s in scenarios]
    
    print("  场景吞吐量对比:")
    for scenario, throughput in zip(scenarios, throughputs):
        symbol_count = int(scenario.split('_')[0])
        print(f"    {symbol_count:2d} 品种: {throughput:7.2f} TPS")
    
    print("3. 性能评估...")
    
    # 延迟评估
    max_latency = max(latencies)
    if max_latency < 1000:
        latency_grade = "优秀"
    elif max_latency < 5000:
        latency_grade = "良好"
    elif max_latency < 10000:
        latency_grade = "一般"
    else:
        latency_grade = "需要优化"
    
    print(f"  延迟评估: {latency_grade} (最大平均延迟: {max_latency:.2f} μs)")
    
    # 吞吐量评估
    min_throughput = min(throughputs)
    if min_throughput > 50:
        throughput_grade = "优秀"
    elif min_throughput > 20:
        throughput_grade = "良好"
    elif min_throughput > 10:
        throughput_grade = "一般"
    else:
        throughput_grade = "需要优化"
    
    print(f"  吞吐量评估: {throughput_grade} (最小吞吐量: {min_throughput:.2f} TPS)")
    
    print("4. 优化建议...")
    recommendations = []
    
    if max_latency > 5000:
        recommendations.append("考虑优化计算逻辑以降低延迟")
    
    if min_throughput < 20:
        recommendations.append("考虑增加硬件资源以提高吞吐量")
    
    # 分析延迟增长趋势
    if len(latencies) >= 2:
        latency_growth = (latencies[-1] - latencies[0]) / latencies[0]
        if latency_growth > 2:  # 延迟增长超过200%
            recommendations.append("延迟随品种数量增长过快，建议优化架构")
    
    if recommendations:
        for i, rec in enumerate(recommendations, 1):
            print(f"    {i}. {rec}")
    else:
        print("    系统性能表现良好，无需特别优化")
    
    print("性能分析示例完成!\n")

def main():
    """运行所有示例"""
    print("DolphinDB CTP 性能测试模块 - 使用示例")
    print("=" * 60)
    print(f"开始时间: {datetime.now()}")
    print()
    
    examples = [
        ("延迟测试", example_latency_test),
        ("行情录制测试", example_recording_test),
        ("系统健康监控", example_health_monitoring),
        ("自定义警报", example_custom_alert),
        ("性能分析", example_performance_analysis),
        ("综合测试", example_comprehensive_test)
    ]
    
    print("可用示例:")
    for i, (name, _) in enumerate(examples, 1):
        print(f"  {i}. {name}")
    
    print("\n选择要运行的示例 (输入数字，或按回车运行所有示例):")
    try:
        choice = input().strip()
        
        if choice == "":
            # 运行所有示例
            for name, func in examples:
                try:
                    func()
                except Exception as e:
                    print(f"{name} 示例运行失败: {e}\n")
        else:
            # 运行指定示例
            index = int(choice) - 1
            if 0 <= index < len(examples):
                name, func = examples[index]
                try:
                    func()
                except Exception as e:
                    print(f"{name} 示例运行失败: {e}")
            else:
                print("无效的选择")
    
    except KeyboardInterrupt:
        print("\n用户中断")
    except Exception as e:
        print(f"运行示例时发生错误: {e}")
    
    print(f"\n结束时间: {datetime.now()}")
    print("=" * 60)

if __name__ == "__main__":
    main()
