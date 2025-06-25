"""
前端监控系统 - Flask Web应用主程序
实现策略实时交互、日志警报系统、实时监控面板
"""

import os
import json
import time
import threading
import random
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
        """初始化策略控制表和相关数据表"""
        script = """
        // 创建策略控制矩阵表（基于test_hist.py的策略）
        strategy_control_matrix = table(
            ["bollinger_bands"] as strategy_name,
            ["IC2311"] as symbol,
            [true] as enabled,
            [now()] as last_updated
        )

        share strategy_control_matrix as strategy_control_matrix

        // 创建策略性能表
        share streamTable(1000:0, `timestamp`strategy_name`symbol`pnl`nav`sharpe`max_drawdown`win_rate`total_trades,
            [TIMESTAMP, SYMBOL, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT]) as strategy_performance

        // 创建实时tick数据表（如果不存在）
        try {
            share streamTable(1000:0, `symbol`timestamp`last_price`bid_price`ask_price`bid_volume`ask_volume`volume`turnover`spread,
                [SYMBOL, TIMESTAMP, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, DOUBLE, DOUBLE]) as live_tick_stream
        } catch(ex) {
            // Table already exists, ignore
        }

        // 创建价差统计表（如果不存在）
        try {
            share streamTable(1000:0, `symbol`timestamp`spread`spread_pct`mid_price,
                [SYMBOL, TIMESTAMP, DOUBLE, DOUBLE, DOUBLE]) as spread_stats
        } catch(ex) {
            // Table already exists, ignore
        }

        // 创建交易信号表（如果不存在）
        try {
            share streamTable(1000:0, `symbol`timestamp`close`signal,
                [SYMBOL, TIMESTAMP, DOUBLE, STRING]) as signalST
        } catch(ex) {
            // Table already exists, ignore
        }

        // 创建订单流表（如果不存在）
        try {
            share streamTable(1000:0, `order_id`symbol`timestamp`price`qty`signal_type,
                [INT, SYMBOL, TIMESTAMP, DOUBLE, INT, STRING]) as orderStream
        } catch(ex) {
            // Table already exists, ignore
        }
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
            # 尝试从DolphinDB获取真实性能数据
            result = self.ddb.execute("select * from strategy_performance order by timestamp desc limit 50")

            if result is not None and len(result) > 0:
                performance_data = []
                for _, row in result.iterrows():
                    performance_data.append({
                        'timestamp': row['timestamp'].isoformat() if hasattr(row['timestamp'], 'isoformat') else str(row['timestamp']),
                        'total_pnl': float(row['pnl']) if 'pnl' in row else 0,
                        'sharpe_ratio': float(row['sharpe']) if 'sharpe' in row else 0,
                        'max_drawdown': float(row['max_drawdown']) if 'max_drawdown' in row else 0,
                        'win_rate': float(row['win_rate']) if 'win_rate' in row else 0,
                        'total_trades': int(row['total_trades']) if 'total_trades' in row else 0
                    })
                return performance_data[::-1]  # 按时间正序
            else:
                # 如果没有真实数据，返回模拟数据
                now = datetime.now()
                performance_data = []

                for i in range(20):
                    timestamp = now - timedelta(minutes=i)
                    performance_data.append({
                        'timestamp': timestamp.isoformat(),
                        'total_pnl': random.uniform(-1000, 2000),
                        'sharpe_ratio': random.uniform(0.5, 2.0),
                        'max_drawdown': random.uniform(0, 500),
                        'win_rate': random.uniform(0.4, 0.8),
                        'total_trades': random.randint(0, 50)
                    })

                return performance_data[::-1]  # 按时间正序
        except Exception as e:
            print(f"获取策略性能错误: {e}")
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

@app.route('/api/strategy/signals')
def get_recent_signals():
    """获取最近的交易信号"""
    try:
        signals = ddb_manager.session.run("select * from signalST order by timestamp desc limit 20")
        signal_data = []

        if signals is not None and len(signals) > 0:
            for _, row in signals.iterrows():
                signal_data.append({
                    'timestamp': row['timestamp'].isoformat() if hasattr(row['timestamp'], 'isoformat') else str(row['timestamp']),
                    'symbol': str(row['symbol']),
                    'price': float(row['close']),
                    'signal': str(row['signal'])
                })

        return jsonify({
            'success': True,
            'data': signal_data
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/strategy/positions')
def get_current_positions():
    """获取当前持仓"""
    try:
        positions = ddb_manager.session.run("select * from positionTable")
        position_data = []

        if positions is not None and len(positions) > 0:
            for _, row in positions.iterrows():
                position_data.append({
                    'symbol': str(row['symbol']),
                    'qty': int(row['qty']),
                    'price': float(row['price']),
                    'direction': str(row['dir'])
                })

        return jsonify({
            'success': True,
            'data': position_data
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/market/live_ticks')
def get_live_ticks():
    """获取实时tick数据"""
    try:
        ticks = ddb_manager.session.run("select * from live_tick_stream order by timestamp desc limit 20")
        tick_data = []

        if ticks is not None and len(ticks) > 0:
            for _, row in ticks.iterrows():
                tick_data.append({
                    'symbol': str(row['symbol']),
                    'timestamp': row['timestamp'].isoformat() if hasattr(row['timestamp'], 'isoformat') else str(row['timestamp']),
                    'last_price': float(row['last_price']),
                    'bid_price': float(row['bid_price']),
                    'ask_price': float(row['ask_price']),
                    'bid_volume': int(row['bid_volume']),
                    'ask_volume': int(row['ask_volume']),
                    'volume': int(row['volume']),
                    'turnover': float(row['turnover']),
                    'spread': float(row['spread'])
                })

        return jsonify({
            'success': True,
            'data': tick_data
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/market/spread_stats')
def get_spread_stats():
    """获取价差统计"""
    try:
        spreads = ddb_manager.session.run("select * from spread_stats order by timestamp desc limit 50")
        spread_data = []

        if spreads is not None and len(spreads) > 0:
            for _, row in spreads.iterrows():
                spread_data.append({
                    'symbol': str(row['symbol']),
                    'timestamp': row['timestamp'].isoformat() if hasattr(row['timestamp'], 'isoformat') else str(row['timestamp']),
                    'spread': float(row['spread']),
                    'spread_pct': float(row['spread_pct']),
                    'mid_price': float(row['mid_price'])
                })

        return jsonify({
            'success': True,
            'data': spread_data
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
