"""
安装和设置脚本
用于检查环境、安装依赖、初始化配置
"""

import os
import sys
import subprocess
import json
from pathlib import Path

def check_python_version():
    """检查Python版本"""
    print("检查Python版本...")
    
    if sys.version_info < (3, 7):
        print(f"❌ Python版本过低: {sys.version}")
        print("   需要Python 3.7或更高版本")
        return False
    else:
        print(f"✅ Python版本: {sys.version}")
        return True

def install_dependencies():
    """安装依赖包"""
    print("\n安装依赖包...")
    
    requirements_file = Path(__file__).parent / "requirements.txt"
    
    if not requirements_file.exists():
        print("❌ requirements.txt文件不存在")
        return False
    
    try:
        # 安装依赖
        subprocess.check_call([
            sys.executable, "-m", "pip", "install", "-r", str(requirements_file)
        ])
        print("✅ 依赖包安装完成")
        return True
    
    except subprocess.CalledProcessError as e:
        print(f"❌ 依赖包安装失败: {e}")
        return False

def check_dolphindb_connection():
    """检查DolphinDB连接"""
    print("\n检查DolphinDB连接...")
    
    try:
        import dolphindb as ddb
        
        session = ddb.session()
        session.connect("localhost", 8848, "admin", "123456")
        
        # 测试查询
        result = session.run("1+1")
        session.close()
        
        if result == 2:
            print("✅ DolphinDB连接正常")
            return True
        else:
            print(f"❌ DolphinDB查询结果异常: {result}")
            return False
    
    except ImportError:
        print("❌ DolphinDB Python API未安装")
        print("   请运行: pip install dolphindb")
        return False
    
    except Exception as e:
        print(f"❌ DolphinDB连接失败: {e}")
        print("   请确保:")
        print("   1. DolphinDB服务正在运行")
        print("   2. 服务地址: localhost:8848")
        print("   3. 用户名: admin, 密码: 123456")
        return False

def create_directories():
    """创建必要的目录"""
    print("\n创建目录结构...")
    
    base_dir = Path(__file__).parent
    directories = [
        "results",
        "logs",
        "config",
        "data"
    ]
    
    for dir_name in directories:
        dir_path = base_dir / dir_name
        dir_path.mkdir(exist_ok=True)
        print(f"✅ 创建目录: {dir_path}")
    
    return True

def create_default_config():
    """创建默认配置文件"""
    print("\n创建默认配置...")
    
    base_dir = Path(__file__).parent
    config_dir = base_dir / "config"
    config_file = config_dir / "default_config.json"
    
    if config_file.exists():
        print(f"⚠️  配置文件已存在: {config_file}")
        return True
    
    default_config = {
        "description": "DolphinDB CTP 性能测试默认配置",
        "version": "1.0",
        "dolphindb": {
            "host": "localhost",
            "port": 8848,
            "username": "admin",
            "password": "123456"
        },
        "test_settings": {
            "default_symbol_count": 10,
            "default_duration": 30,
            "default_tick_rate": 50,
            "output_directory": "results"
        },
        "monitoring": {
            "health_check_interval": 10,
            "memory_threshold": 80,
            "cpu_threshold": 80
        }
    }
    
    try:
        with open(config_file, 'w', encoding='utf-8') as f:
            json.dump(default_config, f, indent=2, ensure_ascii=False)
        
        print(f"✅ 创建默认配置: {config_file}")
        return True
    
    except Exception as e:
        print(f"❌ 创建配置文件失败: {e}")
        return False

def check_system_resources():
    """检查系统资源"""
    print("\n检查系统资源...")
    
    try:
        import psutil
        
        # 检查内存
        memory = psutil.virtual_memory()
        memory_gb = memory.total / (1024**3)
        
        if memory_gb < 4:
            print(f"⚠️  系统内存较少: {memory_gb:.1f} GB")
            print("   建议至少4GB内存以获得最佳性能")
        else:
            print(f"✅ 系统内存: {memory_gb:.1f} GB")
        
        # 检查磁盘空间
        disk = psutil.disk_usage('/')
        disk_free_gb = disk.free / (1024**3)
        
        if disk_free_gb < 1:
            print(f"⚠️  磁盘空间不足: {disk_free_gb:.1f} GB")
            print("   建议至少1GB可用空间")
        else:
            print(f"✅ 可用磁盘空间: {disk_free_gb:.1f} GB")
        
        # 检查CPU
        cpu_count = psutil.cpu_count()
        print(f"✅ CPU核心数: {cpu_count}")
        
        return True
    
    except ImportError:
        print("❌ psutil包未安装，无法检查系统资源")
        return False
    
    except Exception as e:
        print(f"❌ 系统资源检查失败: {e}")
        return False

def run_quick_test():
    """运行快速测试"""
    print("\n运行快速验证测试...")
    
    try:
        # 导入快速测试模块
        sys.path.insert(0, str(Path(__file__).parent))
        from quick_test import main as quick_test_main
        
        # 运行快速测试
        success = quick_test_main()
        
        if success:
            print("✅ 快速验证测试通过")
            return True
        else:
            print("❌ 快速验证测试失败")
            return False
    
    except Exception as e:
        print(f"❌ 快速测试运行失败: {e}")
        return False

def print_usage_instructions():
    """打印使用说明"""
    print("\n" + "="*60)
    print("安装完成！使用说明:")
    print("="*60)
    
    print("\n1. 运行完整性能测试:")
    print("   python performance_test_runner.py")
    
    print("\n2. 运行快速验证:")
    print("   python quick_test.py")
    
    print("\n3. 查看使用示例:")
    print("   python usage_examples.py")
    
    print("\n4. 运行单项测试:")
    print("   python latency_test_framework.py      # 延迟测试")
    print("   python market_data_recorder.py        # 录制测试")
    print("   python system_health_monitor.py       # 健康监控")
    
    print("\n5. 自定义配置:")
    print("   编辑 config/default_config.json")
    print("   或使用 test_config_example.json 作为模板")
    
    print("\n6. 查看结果:")
    print("   测试结果保存在 results/ 目录")
    print("   日志文件保存在 logs/ 目录")
    
    print("\n" + "="*60)

def main():
    """主安装函数"""
    print("DolphinDB CTP 性能测试模块 - 安装和设置")
    print("="*60)
    
    steps = [
        ("检查Python版本", check_python_version),
        ("安装依赖包", install_dependencies),
        ("检查DolphinDB连接", check_dolphindb_connection),
        ("创建目录结构", create_directories),
        ("创建默认配置", create_default_config),
        ("检查系统资源", check_system_resources),
        ("运行快速测试", run_quick_test)
    ]
    
    success_count = 0
    total_count = len(steps)
    
    for step_name, step_func in steps:
        print(f"\n[{success_count + 1}/{total_count}] {step_name}")
        print("-" * 40)
        
        try:
            if step_func():
                success_count += 1
            else:
                print(f"⚠️  {step_name} 未完全成功")
        
        except Exception as e:
            print(f"❌ {step_name} 执行失败: {e}")
    
    print("\n" + "="*60)
    print("安装总结")
    print("="*60)
    print(f"完成步骤: {success_count}/{total_count}")
    
    if success_count == total_count:
        print("🎉 所有步骤完成！系统已准备就绪。")
        print_usage_instructions()
    elif success_count >= total_count - 2:
        print("⚠️  大部分步骤完成，系统基本可用。")
        print("   请检查失败的步骤并手动修复。")
        print_usage_instructions()
    else:
        print("❌ 多个步骤失败，请检查环境配置。")
        print("\n常见问题解决:")
        print("1. 确保Python 3.7+已安装")
        print("2. 确保网络连接正常（用于下载依赖）")
        print("3. 确保DolphinDB服务正在运行")
        print("4. 检查防火墙设置")
    
    return success_count == total_count

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n用户中断安装")
    except Exception as e:
        print(f"\n\n安装过程中发生未预期的错误: {e}")
        import traceback
        traceback.print_exc()
