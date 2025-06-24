"""
前端监控系统 - Flask Web应用主程序
实现策略实时交互、日志警报系统、实时监控面板
"""

import os
import json
import time
import threading
from datetime import datetime, timedelta
from flask import Flask, render_template, request, jsonify, send_from_directory
from flask_socketio import SocketIO, emit
import dolphindb as ddb
import pandas as pd
import psutil


class DolphinDBManager:
    """DolphinDB连接管理器"""
    
    def __init__(self):
        self.session = ddb.session()
        self.connected = False
        self.connect()
        
    def connect(self):
        """连接DolphinDB"""
        try:
            self.session.connect("localhost", 8848, "admin", "123456")
            self.connected = True
            print("✅ DolphinDB连接成功")
        except Exception as e:
            self.connected = False
            print(f"❌ DolphinDB连接失败: {e}")
    
    def execute(self, script):
        """执行DolphinDB脚本"""
        if not self.connected:
            self.connect()
        
        try:
            return self.session.run(script)
        except Exception as e:
            print(f"DolphinDB执行错误: {e}")
            return None
    
    def is_connected(self):
        """检查连接状态"""
        try:
            result = self.session.run("1+1")
            return result == 2
        except:
            return False


class StrategyManager:
    """策略管理器"""
    
    def __init__(self, ddb_manager):
        self.ddb = ddb_manager
        self.strategy_matrix = {}
        self.subscription_status = {}
        self.initialize_tables()
        self.load_strategy_matrix()
    
    def initialize_tables(self):
        """初始化策略控制表"""
        script = """
        // 策略控制矩阵表
        if(existsTable("strategy_control_matrix")){
            dropTable("strategy_control_matrix")
        }
        
        strategy_control_matrix = table(
            ["bollinger", "bollinger", "bollinger", "ma_cross", "ma_cross", "momentum", "arbitrage"] as strategy_name,
            ["IC2509", "IF2509", "IH2509", "IC2509", "IF2509", "T2509", "IC2509"] as symbol,
            [true, true, false, true, false, false, true] as enabled,
            [now(), now(), now(), now(), now(), now(), now()] as last_updated
        )
        
        share strategy_control_matrix as strategy_control_matrix
        
        // 策略订阅状态表
        if(existsTable("subscription_status")){
            dropTable("subscription_status")
        }
        share streamTable(100:0, `strategy_name`symbol`subscription_id`status`last_update, 
            [SYMBOL, SYMBOL, STRING, SYMBOL, TIMESTAMP]) as subscription_status
        
        // 策略性能表
        if(existsTable("strategy_performance")){
            dropTable("strategy_performance")
        }
        share streamTable(1000:0, `timestamp`strategy_name`symbol`pnl`nav`sharpe`max_drawdown`win_rate`total_trades, 
            [TIMESTAMP, SYMBOL, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT]) as strategy_performance
        """
        
        self.ddb.execute(script)
    
    def load_strategy_matrix(self):
        """加载策略控制矩阵"""
        try:
            result = self.ddb.execute("select * from strategy_control_matrix")
            if result is not None and len(result) > 0:
                self.strategy_matrix = {}
                for _, row in result.iterrows():
                    key = f"{row['strategy_name']}_{row['symbol']}"
                    self.strategy_matrix[key] = {
                        'strategy_name': row['strategy_name'],
                        'symbol': row['symbol'],
                        'enabled': bool(row['enabled']),
                        'last_updated': row['last_updated']
                    }
        except Exception as e:
            print(f"加载策略矩阵失败: {e}")
    
    def get_strategy_matrix(self):
        """获取策略控制矩阵"""
        self.load_strategy_matrix()
        
        # 组织数据为前端需要的格式
        strategies = list(set([item['strategy_name'] for item in self.strategy_matrix.values()]))
        symbols = list(set([item['symbol'] for item in self.strategy_matrix.values()]))
        
        matrix_data = {}
        for strategy in strategies:
            matrix_data[strategy] = {}
            for symbol in symbols:
                key = f"{strategy}_{symbol}"
                matrix_data[strategy][symbol] = self.strategy_matrix.get(key, {}).get('enabled', False)
        
        return {
            'strategies': sorted(strategies),
            'symbols': sorted(symbols),
            'matrix': matrix_data,
            'last_updated': datetime.now().isoformat()
        }
    
    def update_strategy_status(self, strategy_name, symbol, enabled):
        """更新策略状态"""
        try:
            script = f"""
            update strategy_control_matrix set 
                enabled = {str(enabled).lower()}, 
                last_updated = now()
            where strategy_name = '{strategy_name}' and symbol = '{symbol}'
            """
            
            self.ddb.execute(script)
            
            # 更新订阅状态
            self.update_subscriptions()
            
            return True
        except Exception as e:
            print(f"更新策略状态失败: {e}")
            return False
    
    def update_subscriptions(self):
        """更新订阅状态"""
        # 这里实现订阅逻辑的更新
        # 在实际项目中，这里会调用相应的订阅管理函数
        pass
    
    def get_strategy_performance(self):
        """获取策略性能数据"""
        try:
            script = """
            select * from strategy_performance 
            where timestamp > (now() - 24*60*60*1000)  // 最近24小时
            order by timestamp desc
            """
            
            result = self.ddb.execute(script)
            if result is not None and len(result) > 0:
                return result.to_dict('records')
            else:
                return []
        except Exception as e:
            print(f"获取策略性能失败: {e}")
            return []


class SystemMonitor:
    """系统监控器"""
    
    def __init__(self, ddb_manager):
        self.ddb = ddb_manager
        self.monitoring = False
        self.monitor_thread = None
    
    def start_monitoring(self):
        """开始监控"""
        if not self.monitoring:
            self.monitoring = True
            self.monitor_thread = threading.Thread(target=self._monitor_loop)
            self.monitor_thread.daemon = True
            self.monitor_thread.start()
    
    def stop_monitoring(self):
        """停止监控"""
        self.monitoring = False
    
    def _monitor_loop(self):
        """监控循环"""
        while self.monitoring:
            try:
                # 收集系统指标
                metrics = self.collect_system_metrics()
                
                # 发送到前端
                socketio.emit('system_metrics', metrics, namespace='/monitor')
                
                time.sleep(5)  # 5秒更新一次
            except Exception as e:
                print(f"监控循环错误: {e}")
                time.sleep(5)
    
    def collect_system_metrics(self):
        """收集系统指标"""
        try:
            # 系统资源
            cpu_percent = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')
            
            # DolphinDB状态
            ddb_connected = self.ddb.is_connected()
            
            # 流表状态
            stream_tables = self.get_stream_table_status()
            
            return {
                'timestamp': datetime.now().isoformat(),
                'system': {
                    'cpu_percent': cpu_percent,
                    'memory_percent': memory.percent,
                    'memory_used_gb': memory.used / (1024**3),
                    'memory_total_gb': memory.total / (1024**3),
                    'disk_percent': (disk.used / disk.total) * 100,
                    'disk_free_gb': disk.free / (1024**3)
                },
                'dolphindb': {
                    'connected': ddb_connected,
                    'stream_tables': stream_tables
                }
            }
        except Exception as e:
            print(f"收集系统指标失败: {e}")
            return {}
    
    def get_stream_table_status(self):
        """获取流表状态"""
        try:
            tables = ['tickStream', 'factor_stream', 'orderStream', 'strategy_performance']
            status = {}
            
            for table in tables:
                script = f"""
                if(existsTable("{table}")){{
                    select count(*) as row_count from {table}
                }} else {{
                    table(0 as row_count)
                }}
                """
                
                result = self.ddb.execute(script)
                if result is not None and len(result) > 0:
                    status[table] = {
                        'exists': True,
                        'row_count': int(result.iloc[0]['row_count'])
                    }
                else:
                    status[table] = {
                        'exists': False,
                        'row_count': 0
                    }
            
            return status
        except Exception as e:
            print(f"获取流表状态失败: {e}")
            return {}


# 创建Flask应用
app = Flask(__name__)
app.config['SECRET_KEY'] = 'dolphindb_ctp_monitor_2024'
socketio = SocketIO(app, cors_allowed_origins="*")

# 初始化管理器
ddb_manager = DolphinDBManager()
strategy_manager = StrategyManager(ddb_manager)
system_monitor = SystemMonitor(ddb_manager)


@app.route('/')
def index():
    """主页"""
    return render_template('index.html')


@app.route('/strategy')
def strategy_page():
    """策略控制页面"""
    return render_template('strategy.html')


@app.route('/monitoring')
def monitoring_page():
    """监控页面"""
    return render_template('monitoring.html')


@app.route('/logs')
def logs_page():
    """日志页面"""
    return render_template('logs.html')


@app.route('/api/strategy/matrix')
def get_strategy_matrix():
    """获取策略控制矩阵"""
    try:
        matrix_data = strategy_manager.get_strategy_matrix()
        return jsonify({
            'success': True,
            'data': matrix_data
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/strategy/update', methods=['POST'])
def update_strategy():
    """更新策略状态"""
    try:
        data = request.json
        strategy_name = data.get('strategy')
        symbol = data.get('symbol')
        enabled = data.get('enabled')
        
        if not all([strategy_name, symbol, enabled is not None]):
            return jsonify({
                'success': False,
                'error': '缺少必要参数'
            }), 400
        
        success = strategy_manager.update_strategy_status(strategy_name, symbol, enabled)
        
        if success:
            # 广播更新到所有客户端
            socketio.emit('strategy_updated', {
                'strategy': strategy_name,
                'symbol': symbol,
                'enabled': enabled,
                'timestamp': datetime.now().isoformat()
            }, namespace='/strategy')
            
            return jsonify({'success': True})
        else:
            return jsonify({
                'success': False,
                'error': '更新失败'
            }), 500
            
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/strategy/performance')
def get_strategy_performance():
    """获取策略性能数据"""
    try:
        performance_data = strategy_manager.get_strategy_performance()
        return jsonify({
            'success': True,
            'data': performance_data
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/system/status')
def get_system_status():
    """获取系统状态"""
    try:
        metrics = system_monitor.collect_system_metrics()
        return jsonify({
            'success': True,
            'data': metrics
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/logs')
def get_logs():
    """获取日志数据"""
    try:
        # 模拟日志数据
        logs = []
        for i in range(20):
            logs.append({
                'timestamp': (datetime.now() - timedelta(minutes=i)).isoformat(),
                'level': ['INFO', 'WARNING', 'ERROR'][i % 3],
                'component': ['Strategy', 'DolphinDB', 'System'][i % 3],
                'message': f'模拟日志消息 {i+1}'
            })
        
        return jsonify({
            'success': True,
            'data': logs
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


# WebSocket事件处理
@socketio.on('connect', namespace='/strategy')
def strategy_connect():
    """策略页面连接"""
    print('策略页面客户端连接')
    emit('connected', {'message': '策略监控已连接'})


@socketio.on('connect', namespace='/monitor')
def monitor_connect():
    """监控页面连接"""
    print('监控页面客户端连接')
    emit('connected', {'message': '系统监控已连接'})
    
    # 开始系统监控
    system_monitor.start_monitoring()


@socketio.on('disconnect', namespace='/monitor')
def monitor_disconnect():
    """监控页面断开连接"""
    print('监控页面客户端断开连接')


if __name__ == '__main__':
    # 确保模板目录存在
    os.makedirs('templates', exist_ok=True)
    os.makedirs('static/css', exist_ok=True)
    os.makedirs('static/js', exist_ok=True)
    
    print("启动DolphinDB CTP前端监控系统...")
    print("访问地址: http://localhost:5000")
    
    # 启动应用
    socketio.run(app, host='0.0.0.0', port=5000, debug=True)
