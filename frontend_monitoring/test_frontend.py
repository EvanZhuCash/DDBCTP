#!/usr/bin/env python3
"""
前端监控系统测试脚本
验证各个功能模块是否正常工作
"""

import sys
import time
import requests
import json
from datetime import datetime
import threading

def test_imports():
    """测试模块导入"""
    print("1. 测试模块导入...")
    
    try:
        import flask
        print("  ✓ Flask导入成功")
    except ImportError as e:
        print(f"  ✗ Flask导入失败: {e}")
        return False
    
    try:
        import flask_socketio
        print("  ✓ Flask-SocketIO导入成功")
    except ImportError as e:
        print(f"  ✗ Flask-SocketIO导入失败: {e}")
        return False
    
    try:
        import dolphindb as ddb
        print("  ✓ DolphinDB导入成功")
    except ImportError as e:
        print(f"  ✗ DolphinDB导入失败: {e}")
        return False
    
    try:
        import pandas as pd
        print("  ✓ Pandas导入成功")
    except ImportError as e:
        print(f"  ✗ Pandas导入失败: {e}")
        return False
    
    try:
        import psutil
        print("  ✓ psutil导入成功")
    except ImportError as e:
        print(f"  ✗ psutil导入失败: {e}")
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
            print("  ✓ DolphinDB连接和查询正常")
            session.close()
            return True
        else:
            print(f"  ✗ DolphinDB查询结果异常: {result}")
            return False
            
    except Exception as e:
        print(f"  ✗ DolphinDB连接失败: {e}")
        print("  提示: 请确保DolphinDB服务在localhost:8848运行")
        return False

def test_app_initialization():
    """测试应用初始化"""
    print("\n3. 测试应用初始化...")
    
    try:
        from app import app, ddb_manager, strategy_manager, system_monitor
        print("  ✓ Flask应用初始化成功")
        
        # 测试DolphinDB管理器
        if ddb_manager.connected:
            print("  ✓ DolphinDB管理器连接正常")
        else:
            print("  ✗ DolphinDB管理器连接失败")
            return False
        
        # 测试策略管理器
        try:
            matrix = strategy_manager.get_strategy_matrix()
            print(f"  ✓ 策略管理器正常，加载了 {len(matrix.get('strategies', []))} 个策略")
        except Exception as e:
            print(f"  ✗ 策略管理器测试失败: {e}")
            return False
        
        # 测试系统监控器
        try:
            metrics = system_monitor.collect_system_metrics()
            if metrics:
                print("  ✓ 系统监控器正常")
            else:
                print("  ✗ 系统监控器无法收集指标")
                return False
        except Exception as e:
            print(f"  ✗ 系统监控器测试失败: {e}")
            return False
        
        return True
        
    except Exception as e:
        print(f"  ✗ 应用初始化失败: {e}")
        return False

def test_api_endpoints():
    """测试API端点"""
    print("\n4. 测试API端点...")
    
    # 启动应用服务器（后台线程）
    def run_server():
        from app import app, socketio
        socketio.run(app, host='127.0.0.1', port=5001, debug=False, use_reloader=False)
    
    server_thread = threading.Thread(target=run_server, daemon=True)
    server_thread.start()
    
    # 等待服务器启动
    time.sleep(3)
    
    base_url = "http://127.0.0.1:5001"
    
    # 测试主页
    try:
        response = requests.get(f"{base_url}/", timeout=5)
        if response.status_code == 200:
            print("  ✓ 主页访问正常")
        else:
            print(f"  ✗ 主页访问失败: {response.status_code}")
            return False
    except Exception as e:
        print(f"  ✗ 主页访问异常: {e}")
        return False
    
    # 测试策略矩阵API
    try:
        response = requests.get(f"{base_url}/api/strategy/matrix", timeout=5)
        if response.status_code == 200:
            data = response.json()
            if data.get('success'):
                print("  ✓ 策略矩阵API正常")
            else:
                print(f"  ✗ 策略矩阵API返回错误: {data.get('error')}")
                return False
        else:
            print(f"  ✗ 策略矩阵API访问失败: {response.status_code}")
            return False
    except Exception as e:
        print(f"  ✗ 策略矩阵API异常: {e}")
        return False
    
    # 测试系统状态API
    try:
        response = requests.get(f"{base_url}/api/system/status", timeout=5)
        if response.status_code == 200:
            data = response.json()
            if data.get('success'):
                print("  ✓ 系统状态API正常")
            else:
                print(f"  ✗ 系统状态API返回错误: {data.get('error')}")
                return False
        else:
            print(f"  ✗ 系统状态API访问失败: {response.status_code}")
            return False
    except Exception as e:
        print(f"  ✗ 系统状态API异常: {e}")
        return False
    
    # 测试日志API
    try:
        response = requests.get(f"{base_url}/api/logs", timeout=5)
        if response.status_code == 200:
            data = response.json()
            if data.get('success'):
                print("  ✓ 日志API正常")
            else:
                print(f"  ✗ 日志API返回错误: {data.get('error')}")
                return False
        else:
            print(f"  ✗ 日志API访问失败: {response.status_code}")
            return False
    except Exception as e:
        print(f"  ✗ 日志API异常: {e}")
        return False
    
    return True

def test_strategy_update():
    """测试策略更新功能"""
    print("\n5. 测试策略更新功能...")
    
    base_url = "http://127.0.0.1:5001"
    
    # 测试策略状态更新
    try:
        update_data = {
            "strategy": "bollinger",
            "symbol": "IC2509",
            "enabled": True
        }
        
        response = requests.post(
            f"{base_url}/api/strategy/update",
            json=update_data,
            timeout=5
        )
        
        if response.status_code == 200:
            data = response.json()
            if data.get('success'):
                print("  ✓ 策略更新功能正常")
            else:
                print(f"  ✗ 策略更新失败: {data.get('error')}")
                return False
        else:
            print(f"  ✗ 策略更新请求失败: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"  ✗ 策略更新异常: {e}")
        return False
    
    return True

def test_file_structure():
    """测试文件结构"""
    print("\n6. 测试文件结构...")
    
    import os
    
    required_files = [
        "app.py",
        "requirements.txt",
        "templates/base.html",
        "templates/index.html",
        "templates/strategy.html",
        "templates/monitoring.html",
        "templates/logs.html",
        "static/css/custom.css",
        "static/js/common.js"
    ]
    
    missing_files = []
    for file_path in required_files:
        if not os.path.exists(file_path):
            missing_files.append(file_path)
    
    if missing_files:
        print(f"  ✗ 缺少文件: {', '.join(missing_files)}")
        return False
    else:
        print("  ✓ 所有必需文件都存在")
        return True

def test_template_syntax():
    """测试模板语法"""
    print("\n7. 测试模板语法...")
    
    try:
        from flask import Flask
        from jinja2 import Environment, FileSystemLoader
        
        # 创建Jinja2环境
        env = Environment(loader=FileSystemLoader('templates'))
        
        templates = ['base.html', 'index.html', 'strategy.html', 'monitoring.html', 'logs.html']
        
        for template_name in templates:
            try:
                template = env.get_template(template_name)
                print(f"  ✓ {template_name} 语法正确")
            except Exception as e:
                print(f"  ✗ {template_name} 语法错误: {e}")
                return False
        
        return True
        
    except Exception as e:
        print(f"  ✗ 模板测试失败: {e}")
        return False

def generate_test_report(results):
    """生成测试报告"""
    print("\n" + "="*60)
    print("测试报告")
    print("="*60)
    
    total_tests = len(results)
    passed_tests = sum(1 for result in results.values() if result)
    failed_tests = total_tests - passed_tests
    
    print(f"总测试数: {total_tests}")
    print(f"通过: {passed_tests}")
    print(f"失败: {failed_tests}")
    print(f"成功率: {(passed_tests/total_tests)*100:.1f}%")
    
    print("\n详细结果:")
    for test_name, result in results.items():
        status = "✓ 通过" if result else "✗ 失败"
        print(f"  {test_name}: {status}")
    
    if failed_tests == 0:
        print("\n🎉 所有测试通过！前端系统可以正常使用。")
        print("\n下一步:")
        print("1. 运行前端系统: python run.py")
        print("2. 访问地址: http://localhost:5000")
        print("3. 查看各个功能页面")
    else:
        print(f"\n⚠️  有 {failed_tests} 项测试失败，请检查相关配置。")
    
    print("\n" + "="*60)
    
    return failed_tests == 0

def main():
    """主测试函数"""
    print("DolphinDB CTP 前端监控系统 - 功能测试")
    print("="*60)
    print(f"测试时间: {datetime.now()}")
    
    # 定义测试用例
    test_cases = [
        ("模块导入", test_imports),
        ("DolphinDB连接", test_dolphindb_connection),
        ("应用初始化", test_app_initialization),
        ("API端点", test_api_endpoints),
        ("策略更新", test_strategy_update),
        ("文件结构", test_file_structure),
        ("模板语法", test_template_syntax)
    ]
    
    results = {}
    
    # 运行测试
    for test_name, test_func in test_cases:
        try:
            result = test_func()
            results[test_name] = result
        except Exception as e:
            print(f"\n{test_name} 测试出现异常: {e}")
            results[test_name] = False
    
    # 生成报告
    success = generate_test_report(results)
    
    return success

if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n用户中断测试")
        sys.exit(1)
    except Exception as e:
        print(f"\n测试过程中发生错误: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
