#!/usr/bin/env python3
"""
DolphinDB CTP 前端监控系统完整测试脚本
测试所有前端功能和API接口
"""

import os
import sys
import time
import json
import requests
import subprocess
from datetime import datetime
from pathlib import Path

class FrontendTester:
    def __init__(self):
        self.base_url = "http://localhost:5000"
        self.test_results = []
        self.start_time = None
        
    def log_test(self, test_name, status, message="", duration=0):
        """记录测试结果"""
        result = {
            'test_name': test_name,
            'status': status,
            'message': message,
            'duration': duration,
            'timestamp': datetime.now().isoformat()
        }
        self.test_results.append(result)
        
        status_icon = "✓" if status == "PASS" else "✗" if status == "FAIL" else "⚠"
        print(f"  {status_icon} {test_name}: {message}")
        
    def test_environment(self):
        """测试环境检查"""
        print("\n1. 环境检查...")
        
        # 检查Python版本
        python_version = sys.version_info
        if python_version >= (3, 7):
            self.log_test("Python版本", "PASS", f"Python {python_version.major}.{python_version.minor}")
        else:
            self.log_test("Python版本", "FAIL", f"需要Python 3.7+，当前版本: {python_version.major}.{python_version.minor}")
            
        # 检查必要文件
        required_files = [
            "app.py",
            "requirements.txt",
            "templates/base.html",
            "templates/index.html",
            "templates/strategy.html",
            "templates/monitoring.html",
            "templates/logs.html",
            "static/css/custom.css",
            "static/js/common.js",
            "static/js/strategy.js",
            "static/js/monitoring.js",
            "static/js/logs.js",
            "static/js/dashboard.js",
            "static/js/ui-components.js"
        ]
        
        missing_files = []
        for file_path in required_files:
            if not os.path.exists(file_path):
                missing_files.append(file_path)
                
        if not missing_files:
            self.log_test("文件完整性", "PASS", "所有必要文件存在")
        else:
            self.log_test("文件完整性", "FAIL", f"缺少文件: {', '.join(missing_files)}")
            
    def test_dependencies(self):
        """测试依赖包"""
        print("\n2. 依赖包检查...")
        
        try:
            import flask
            self.log_test("Flask", "PASS", f"版本: {flask.__version__}")
        except ImportError:
            self.log_test("Flask", "FAIL", "未安装Flask")
            
        try:
            import flask_socketio
            self.log_test("Flask-SocketIO", "PASS", f"版本: {flask_socketio.__version__}")
        except ImportError:
            self.log_test("Flask-SocketIO", "FAIL", "未安装Flask-SocketIO")
            
        try:
            import dolphindb
            self.log_test("DolphinDB", "PASS", f"版本: {dolphindb.__version__}")
        except ImportError:
            self.log_test("DolphinDB", "FAIL", "未安装DolphinDB Python API")
            
        try:
            import pandas
            self.log_test("Pandas", "PASS", f"版本: {pandas.__version__}")
        except ImportError:
            self.log_test("Pandas", "FAIL", "未安装Pandas")
            
        try:
            import psutil
            self.log_test("Psutil", "PASS", f"版本: {psutil.__version__}")
        except ImportError:
            self.log_test("Psutil", "FAIL", "未安装Psutil")
            
    def test_server_startup(self):
        """测试服务器启动"""
        print("\n3. 服务器启动测试...")
        
        try:
            # 检查服务器是否已经运行
            response = requests.get(f"{self.base_url}/", timeout=5)
            if response.status_code == 200:
                self.log_test("服务器状态", "PASS", "服务器已运行")
                return True
            else:
                self.log_test("服务器状态", "FAIL", f"服务器响应异常: {response.status_code}")
                return False
        except requests.exceptions.ConnectionError:
            self.log_test("服务器状态", "FAIL", "无法连接到服务器")
            return False
        except Exception as e:
            self.log_test("服务器状态", "FAIL", f"连接错误: {str(e)}")
            return False
            
    def test_page_loading(self):
        """测试页面加载"""
        print("\n4. 页面加载测试...")
        
        pages = [
            ("/", "总览页面"),
            ("/strategy", "策略控制页面"),
            ("/monitoring", "系统监控页面"),
            ("/logs", "日志警报页面")
        ]
        
        for url, name in pages:
            try:
                start_time = time.time()
                response = requests.get(f"{self.base_url}{url}", timeout=10)
                duration = time.time() - start_time
                
                if response.status_code == 200:
                    if "<!DOCTYPE html>" in response.text:
                        self.log_test(name, "PASS", f"加载成功 ({duration:.2f}s)")
                    else:
                        self.log_test(name, "FAIL", "响应不是有效的HTML")
                else:
                    self.log_test(name, "FAIL", f"HTTP {response.status_code}")
            except Exception as e:
                self.log_test(name, "FAIL", f"加载失败: {str(e)}")
                
    def test_static_resources(self):
        """测试静态资源"""
        print("\n5. 静态资源测试...")
        
        static_files = [
            ("/static/css/custom.css", "自定义样式"),
            ("/static/js/common.js", "通用JavaScript"),
            ("/static/js/strategy.js", "策略页面脚本"),
            ("/static/js/monitoring.js", "监控页面脚本"),
            ("/static/js/logs.js", "日志页面脚本"),
            ("/static/js/dashboard.js", "仪表板脚本"),
            ("/static/js/ui-components.js", "UI组件脚本")
        ]
        
        for url, name in static_files:
            try:
                response = requests.get(f"{self.base_url}{url}", timeout=5)
                if response.status_code == 200:
                    size = len(response.content)
                    self.log_test(name, "PASS", f"加载成功 ({size} bytes)")
                else:
                    self.log_test(name, "FAIL", f"HTTP {response.status_code}")
            except Exception as e:
                self.log_test(name, "FAIL", f"加载失败: {str(e)}")
                
    def test_api_endpoints(self):
        """测试API接口"""
        print("\n6. API接口测试...")
        
        api_endpoints = [
            ("/api/system/status", "系统状态API"),
            ("/api/system/health", "系统健康检查API"),
            ("/api/strategy/matrix", "策略矩阵API"),
            ("/api/strategy/performance", "策略性能API"),
            ("/api/logs/recent", "最近日志API"),
            ("/api/dashboard/overview", "总览数据API")
        ]
        
        for url, name in api_endpoints:
            try:
                response = requests.get(f"{self.base_url}{url}", timeout=10)
                if response.status_code == 200:
                    try:
                        data = response.json()
                        if isinstance(data, dict):
                            self.log_test(name, "PASS", "返回有效JSON数据")
                        else:
                            self.log_test(name, "WARN", "返回数据格式异常")
                    except json.JSONDecodeError:
                        self.log_test(name, "FAIL", "返回数据不是有效JSON")
                else:
                    self.log_test(name, "FAIL", f"HTTP {response.status_code}")
            except Exception as e:
                self.log_test(name, "FAIL", f"请求失败: {str(e)}")
                
    def test_responsive_design(self):
        """测试响应式设计"""
        print("\n7. 响应式设计测试...")
        
        # 这里可以添加更多的响应式测试
        # 由于是后端测试，主要检查CSS文件是否包含媒体查询
        try:
            with open("static/css/custom.css", "r", encoding="utf-8") as f:
                css_content = f.read()
                
            if "@media" in css_content:
                media_queries = css_content.count("@media")
                self.log_test("媒体查询", "PASS", f"发现 {media_queries} 个媒体查询")
            else:
                self.log_test("媒体查询", "FAIL", "未发现媒体查询")
                
            if "max-width" in css_content:
                self.log_test("断点设置", "PASS", "包含响应式断点")
            else:
                self.log_test("断点设置", "FAIL", "未发现响应式断点")
                
        except Exception as e:
            self.log_test("CSS文件检查", "FAIL", f"无法读取CSS文件: {str(e)}")
            
    def test_javascript_modules(self):
        """测试JavaScript模块"""
        print("\n8. JavaScript模块测试...")
        
        js_files = [
            ("static/js/common.js", ["Utils", "NotificationManager", "ApiManager"]),
            ("static/js/strategy.js", ["StrategyController"]),
            ("static/js/monitoring.js", ["MonitoringController"]),
            ("static/js/logs.js", ["LogsController"]),
            ("static/js/dashboard.js", ["DashboardController"]),
            ("static/js/ui-components.js", ["UIComponents"])
        ]
        
        for file_path, expected_classes in js_files:
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    js_content = f.read()
                    
                found_classes = []
                for class_name in expected_classes:
                    if f"class {class_name}" in js_content:
                        found_classes.append(class_name)
                        
                if len(found_classes) == len(expected_classes):
                    self.log_test(f"JS模块 - {os.path.basename(file_path)}", "PASS", 
                                f"包含所有预期类: {', '.join(found_classes)}")
                else:
                    missing = set(expected_classes) - set(found_classes)
                    self.log_test(f"JS模块 - {os.path.basename(file_path)}", "FAIL", 
                                f"缺少类: {', '.join(missing)}")
                    
            except Exception as e:
                self.log_test(f"JS模块 - {os.path.basename(file_path)}", "FAIL", 
                            f"无法读取文件: {str(e)}")
                
    def generate_report(self):
        """生成测试报告"""
        print("\n" + "="*60)
        print("测试报告")
        print("="*60)
        
        total_tests = len(self.test_results)
        passed_tests = len([r for r in self.test_results if r['status'] == 'PASS'])
        failed_tests = len([r for r in self.test_results if r['status'] == 'FAIL'])
        warned_tests = len([r for r in self.test_results if r['status'] == 'WARN'])
        
        print(f"总测试数: {total_tests}")
        print(f"通过: {passed_tests}")
        print(f"失败: {failed_tests}")
        print(f"警告: {warned_tests}")
        print(f"成功率: {(passed_tests/total_tests)*100:.1f}%")
        
        if failed_tests > 0:
            print("\n失败的测试:")
            for result in self.test_results:
                if result['status'] == 'FAIL':
                    print(f"  ✗ {result['test_name']}: {result['message']}")
                    
        # 保存详细报告
        report_data = {
            'summary': {
                'total': total_tests,
                'passed': passed_tests,
                'failed': failed_tests,
                'warned': warned_tests,
                'success_rate': (passed_tests/total_tests)*100
            },
            'tests': self.test_results,
            'generated_at': datetime.now().isoformat()
        }
        
        with open('frontend_test_report.json', 'w', encoding='utf-8') as f:
            json.dump(report_data, f, indent=2, ensure_ascii=False)
            
        print(f"\n详细报告已保存到: frontend_test_report.json")
        
        return failed_tests == 0
        
    def run_all_tests(self):
        """运行所有测试"""
        print("DolphinDB CTP 前端监控系统 - 完整测试")
        print("="*60)
        
        self.start_time = time.time()
        
        # 运行所有测试
        self.test_environment()
        self.test_dependencies()
        self.test_server_startup()
        self.test_page_loading()
        self.test_static_resources()
        self.test_api_endpoints()
        self.test_responsive_design()
        self.test_javascript_modules()
        
        # 生成报告
        success = self.generate_report()
        
        total_time = time.time() - self.start_time
        print(f"\n总测试时间: {total_time:.2f}秒")
        
        return success

def main():
    """主函数"""
    tester = FrontendTester()
    success = tester.run_all_tests()
    
    if success:
        print("\n🎉 所有测试通过！前端系统准备就绪。")
        sys.exit(0)
    else:
        print("\n❌ 部分测试失败，请检查上述错误信息。")
        sys.exit(1)

if __name__ == "__main__":
    main()
