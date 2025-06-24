"""
快速测试脚本 - 验证性能测试模块的基本功能
用于快速验证各个模块是否正常工作
"""

import sys
import time
import traceback
from datetime import datetime

def test_imports():
    """测试模块导入"""
    print("1. 测试模块导入...")
    
    try:
        from latency_test_framework import LatencyTestFramework, LatencyMeasurement
        print("  ✓ 延迟测试框架导入成功")
    except Exception as e:
        print(f"  ✗ 延迟测试框架导入失败: {e}")
        return False
    
    try:
        from market_data_recorder import MarketDataRecorder, MemoryMonitor
        print("  ✓ 行情录制模块导入成功")
    except Exception as e:
        print(f"  ✗ 行情录制模块导入失败: {e}")
        return False
    
    try:
        from system_health_monitor import SystemHealthMonitor, AlertManager, HealthCheckResult
        print("  ✓ 系统健康监控模块导入成功")
    except Exception as e:
        print(f"  ✗ 系统健康监控模块导入失败: {e}")
        return False
    
    try:
        from performance_test_runner import PerformanceTestRunner
        print("  ✓ 性能测试运行器导入成功")
    except Exception as e:
        print(f"  ✗ 性能测试运行器导入失败: {e}")
        return False
    
    return True

def test_dolphindb_connection():
    """测试DolphinDB连接"""
    print("\n2. 测试DolphinDB连接...")
    
    try:
        import dolphindb as ddb
        session = ddb.session()
        session.connect("localhost", 8848, "admin", "123456")
        
        # 测试简单查询
        result = session.run("1+1")
        if result == 2:
            print("  ✓ DolphinDB连接成功")
            session.close()
            return True
        else:
            print(f"  ✗ DolphinDB查询结果异常: {result}")
            return False
            
    except Exception as e:
        print(f"  ✗ DolphinDB连接失败: {e}")
        print("  提示: 请确保DolphinDB服务在localhost:8848运行，用户名admin，密码123456")
        return False

def test_latency_measurement():
    """测试延迟测量功能"""
    print("\n3. 测试延迟测量功能...")
    
    try:
        from latency_test_framework import LatencyMeasurement
        
        measurement = LatencyMeasurement()
        
        # 测试基本测量功能
        test_id = "test_001"
        stage = "test_stage"
        symbol = "TEST001"
        
        # 开始测量
        start_time = measurement.start_measurement(test_id, stage, symbol, 100)
        
        # 模拟处理时间
        time.sleep(0.001)  # 1毫秒
        
        # 结束测量
        latency = measurement.end_measurement(test_id, stage)
        
        if latency and latency > 0:
            print(f"  ✓ 延迟测量功能正常，测量延迟: {latency:.2f} μs")
            return True
        else:
            print("  ✗ 延迟测量结果异常")
            return False
            
    except Exception as e:
        print(f"  ✗ 延迟测量测试失败: {e}")
        traceback.print_exc()
        return False

def test_memory_monitor():
    """测试内存监控功能"""
    print("\n4. 测试内存监控功能...")
    
    try:
        from market_data_recorder import MemoryMonitor
        
        monitor = MemoryMonitor(threshold_percent=95.0)  # 设置高阈值避免触发
        
        # 获取内存信息
        memory_info = monitor.get_memory_info()
        
        if all(key in memory_info for key in ['total_gb', 'available_gb', 'used_gb', 'percent']):
            print(f"  ✓ 内存监控功能正常")
            print(f"    总内存: {memory_info['total_gb']:.2f} GB")
            print(f"    已用内存: {memory_info['used_gb']:.2f} GB")
            print(f"    使用率: {memory_info['percent']:.1f}%")
            return True
        else:
            print("  ✗ 内存信息格式异常")
            return False
            
    except Exception as e:
        print(f"  ✗ 内存监控测试失败: {e}")
        traceback.print_exc()
        return False

def test_health_check():
    """测试健康检查功能"""
    print("\n5. 测试健康检查功能...")
    
    try:
        from system_health_monitor import HealthCheckResult
        
        # 创建测试健康检查结果
        result = HealthCheckResult("test_component", "HEALTHY", "测试消息", {"test_key": "test_value"})
        
        # 转换为字典
        result_dict = result.to_dict()
        
        required_keys = ['component', 'status', 'message', 'details', 'timestamp']
        if all(key in result_dict for key in required_keys):
            print("  ✓ 健康检查结果格式正常")
            print(f"    组件: {result_dict['component']}")
            print(f"    状态: {result_dict['status']}")
            print(f"    消息: {result_dict['message']}")
            return True
        else:
            print("  ✗ 健康检查结果格式异常")
            return False
            
    except Exception as e:
        print(f"  ✗ 健康检查测试失败: {e}")
        traceback.print_exc()
        return False

def test_basic_functionality():
    """测试基本功能（不需要DolphinDB）"""
    print("\n6. 测试基本功能...")
    
    try:
        # 测试延迟测试框架初始化
        from latency_test_framework import LatencyTestFramework
        framework = LatencyTestFramework()
        print("  ✓ 延迟测试框架初始化成功")
        
        # 测试数据生成
        test_data = framework.generate_test_data(symbol_count=5, tick_rate=10, duration=1)
        if len(test_data) > 0:
            print(f"  ✓ 测试数据生成成功，生成 {len(test_data)} 条数据")
        else:
            print("  ✗ 测试数据生成失败")
            return False
        
        # 测试性能测试运行器初始化
        from performance_test_runner import PerformanceTestRunner
        runner = PerformanceTestRunner(output_dir="performance_testing/test_results")
        print("  ✓ 性能测试运行器初始化成功")
        
        return True
        
    except Exception as e:
        print(f"  ✗ 基本功能测试失败: {e}")
        traceback.print_exc()
        return False

def test_configuration():
    """测试配置文件"""
    print("\n7. 测试配置文件...")
    
    try:
        import json
        import os
        
        config_file = "performance_testing/test_config_example.json"
        if os.path.exists(config_file):
            with open(config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            required_sections = ['latency_config', 'recording_config', 'stress_config']
            if all(section in config for section in required_sections):
                print("  ✓ 配置文件格式正确")
                print(f"    延迟测试配置: {len(config['latency_config'])} 项")
                print(f"    录制测试配置: {len(config['recording_config'])} 项")
                print(f"    压力测试配置: {len(config['stress_config'])} 项")
                return True
            else:
                print("  ✗ 配置文件缺少必要部分")
                return False
        else:
            print(f"  ✗ 配置文件不存在: {config_file}")
            return False
            
    except Exception as e:
        print(f"  ✗ 配置文件测试失败: {e}")
        return False

def main():
    """主测试函数"""
    print("=" * 60)
    print("DolphinDB CTP 性能测试模块 - 快速验证")
    print("=" * 60)
    print(f"测试时间: {datetime.now()}")
    
    test_results = []
    
    # 运行各项测试
    tests = [
        ("模块导入", test_imports),
        ("DolphinDB连接", test_dolphindb_connection),
        ("延迟测量", test_latency_measurement),
        ("内存监控", test_memory_monitor),
        ("健康检查", test_health_check),
        ("基本功能", test_basic_functionality),
        ("配置文件", test_configuration)
    ]
    
    for test_name, test_func in tests:
        try:
            result = test_func()
            test_results.append((test_name, result))
        except Exception as e:
            print(f"\n{test_name} 测试出现异常: {e}")
            test_results.append((test_name, False))
    
    # 输出测试摘要
    print("\n" + "=" * 60)
    print("测试摘要")
    print("=" * 60)
    
    passed = 0
    failed = 0
    
    for test_name, result in test_results:
        status = "✓ 通过" if result else "✗ 失败"
        print(f"{test_name:20} : {status}")
        
        if result:
            passed += 1
        else:
            failed += 1
    
    print(f"\n总计: {len(test_results)} 项测试")
    print(f"通过: {passed} 项")
    print(f"失败: {failed} 项")
    
    if failed == 0:
        print("\n🎉 所有测试通过！模块可以正常使用。")
        print("\n下一步:")
        print("1. 运行完整性能测试: python performance_test_runner.py")
        print("2. 或运行单项测试: python latency_test_framework.py")
    else:
        print(f"\n⚠️  有 {failed} 项测试失败，请检查相关配置和依赖。")
        
        if any("DolphinDB" in name for name, result in test_results if not result):
            print("\n提示: DolphinDB相关测试失败，请确保:")
            print("- DolphinDB服务正在运行 (localhost:8848)")
            print("- 用户名: admin, 密码: 123456")
            print("- 网络连接正常")
    
    print("\n" + "=" * 60)
    
    return failed == 0

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
