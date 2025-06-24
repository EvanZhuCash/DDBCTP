#!/usr/bin/env python3
"""
DolphinDB CTP 前端监控系统启动脚本
提供开发和生产环境的启动选项
"""

import os
import sys
import argparse
import subprocess
from pathlib import Path

def check_requirements():
    """检查依赖是否安装"""
    try:
        import flask
        import flask_socketio
        import dolphindb
        import pandas
        import psutil
        print("✅ 所有依赖已安装")
        return True
    except ImportError as e:
        print(f"❌ 缺少依赖: {e}")
        print("请运行: pip install -r requirements.txt")
        return False

def check_dolphindb_connection():
    """检查DolphinDB连接"""
    try:
        import dolphindb as ddb
        session = ddb.session()
        session.connect("localhost", 8848, "admin", "123456")
        result = session.run("1+1")
        session.close()
        
        if result == 2:
            print("✅ DolphinDB连接正常")
            return True
        else:
            print("❌ DolphinDB连接异常")
            return False
    except Exception as e:
        print(f"❌ DolphinDB连接失败: {e}")
        print("请确保DolphinDB服务在localhost:8848运行")
        return False

def run_development():
    """运行开发环境"""
    print("🚀 启动开发环境...")
    print("访问地址: http://localhost:5000")
    print("按 Ctrl+C 停止服务")
    
    # 设置环境变量
    os.environ['FLASK_ENV'] = 'development'
    os.environ['FLASK_DEBUG'] = '1'
    
    # 导入并运行应用
    from app import app, socketio
    socketio.run(app, host='0.0.0.0', port=5000, debug=True)

def run_production(host='0.0.0.0', port=5000, workers=1):
    """运行生产环境"""
    print("🚀 启动生产环境...")
    print(f"访问地址: http://{host}:{port}")
    
    # 检查是否安装了gunicorn
    try:
        import gunicorn
    except ImportError:
        print("❌ 生产环境需要安装gunicorn")
        print("请运行: pip install gunicorn")
        return False
    
    # 使用gunicorn启动
    cmd = [
        'gunicorn',
        '--worker-class', 'eventlet',
        '-w', str(workers),
        '--bind', f'{host}:{port}',
        '--timeout', '120',
        '--keep-alive', '2',
        '--max-requests', '1000',
        '--max-requests-jitter', '100',
        'app:app'
    ]
    
    try:
        subprocess.run(cmd)
    except KeyboardInterrupt:
        print("\n👋 服务已停止")
    except Exception as e:
        print(f"❌ 启动失败: {e}")
        return False
    
    return True

def install_dependencies():
    """安装依赖"""
    print("📦 安装依赖包...")
    
    requirements_file = Path(__file__).parent / "requirements.txt"
    if not requirements_file.exists():
        print("❌ requirements.txt文件不存在")
        return False
    
    try:
        subprocess.check_call([
            sys.executable, "-m", "pip", "install", "-r", str(requirements_file)
        ])
        print("✅ 依赖安装完成")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ 依赖安装失败: {e}")
        return False

def create_systemd_service():
    """创建systemd服务文件"""
    service_content = f"""[Unit]
Description=DolphinDB CTP Frontend Monitoring
After=network.target

[Service]
Type=simple
User=www-data
WorkingDirectory={Path(__file__).parent.absolute()}
Environment=PATH={sys.executable}
ExecStart={sys.executable} run.py --mode production
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
"""
    
    service_file = "/etc/systemd/system/ddb-ctp-frontend.service"
    
    try:
        with open(service_file, 'w') as f:
            f.write(service_content)
        
        print(f"✅ 服务文件已创建: {service_file}")
        print("启用服务: sudo systemctl enable ddb-ctp-frontend")
        print("启动服务: sudo systemctl start ddb-ctp-frontend")
        print("查看状态: sudo systemctl status ddb-ctp-frontend")
        return True
    except PermissionError:
        print("❌ 需要管理员权限创建服务文件")
        print(f"请手动创建 {service_file} 文件")
        print("服务文件内容:")
        print(service_content)
        return False

def show_status():
    """显示系统状态"""
    print("📊 系统状态检查")
    print("=" * 50)
    
    # 检查Python版本
    python_version = sys.version.split()[0]
    print(f"Python版本: {python_version}")
    
    # 检查依赖
    check_requirements()
    
    # 检查DolphinDB连接
    check_dolphindb_connection()
    
    # 检查端口占用
    try:
        import socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        result = sock.connect_ex(('localhost', 5000))
        sock.close()
        
        if result == 0:
            print("⚠️  端口5000已被占用")
        else:
            print("✅ 端口5000可用")
    except Exception as e:
        print(f"❌ 端口检查失败: {e}")
    
    print("=" * 50)

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='DolphinDB CTP 前端监控系统')
    parser.add_argument('--mode', choices=['dev', 'prod', 'development', 'production'], 
                       default='dev', help='运行模式')
    parser.add_argument('--host', default='0.0.0.0', help='绑定主机地址')
    parser.add_argument('--port', type=int, default=5000, help='绑定端口')
    parser.add_argument('--workers', type=int, default=1, help='工作进程数（生产模式）')
    parser.add_argument('--install', action='store_true', help='安装依赖')
    parser.add_argument('--status', action='store_true', help='显示系统状态')
    parser.add_argument('--service', action='store_true', help='创建systemd服务')
    
    args = parser.parse_args()
    
    # 显示标题
    print("=" * 60)
    print("🐬 DolphinDB CTP 前端监控系统")
    print("=" * 60)
    
    # 处理特殊命令
    if args.install:
        return install_dependencies()
    
    if args.status:
        return show_status()
    
    if args.service:
        return create_systemd_service()
    
    # 检查基本环境
    if not check_requirements():
        print("\n💡 提示: 运行 python run.py --install 安装依赖")
        return False
    
    if not check_dolphindb_connection():
        print("\n💡 提示: 请确保DolphinDB服务正在运行")
        print("   连接参数: localhost:8848, admin/123456")
        return False
    
    # 运行应用
    if args.mode in ['dev', 'development']:
        run_development()
    elif args.mode in ['prod', 'production']:
        run_production(args.host, args.port, args.workers)
    
    return True

if __name__ == '__main__':
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n👋 用户中断，服务已停止")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ 启动失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
