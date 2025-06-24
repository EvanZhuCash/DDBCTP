# 前端监控系统开发方案

## 1. 策略实时交互界面

### 1.1 策略控制矩阵设计
```python
# Flask Web应用框架
from flask import Flask, render_template, request, jsonify
import dolphindb as ddb
import json

class StrategyControlPanel:
    """策略控制面板"""
    
    def __init__(self):
        self.app = Flask(__name__)
        self.session = ddb.session()
        self.session.connect("localhost", 8848, "admin", "123456")
        self.setup_routes()
        
    def setup_routes(self):
        """设置路由"""
        
        @self.app.route('/')
        def dashboard():
            return render_template('dashboard.html')
        
        @self.app.route('/api/strategy_matrix')
        def get_strategy_matrix():
            """获取策略控制矩阵"""
            matrix = self.session.run("""
              select * from strategy_control_matrix order by strategy_name, symbol
            """)
            return jsonify(matrix.to_dict('records'))
        
        @self.app.route('/api/update_strategy', methods=['POST'])
        def update_strategy():
            """更新策略状态"""
            data = request.json
            strategy = data['strategy']
            symbol = data['symbol'] 
            enabled = data['enabled']
            
            # 更新DolphinDB中的策略状态
            self.session.run(f"""
              update strategy_control_matrix set enabled = {enabled}
              where strategy_name = '{strategy}' and symbol = '{symbol}'
            """)
            
            # 触发订阅更新
            self.update_subscriptions()
            
            return jsonify({"status": "success"})

def setup_strategy_control_tables(self):
    """设置策略控制表"""
    self.session.run("""
      // 策略控制矩阵表
      if(existsTable("strategy_control_matrix")){
        dropTable("strategy_control_matrix")
      }
      
      strategy_control_matrix = table(
        ["bollinger", "bollinger", "bollinger", "ma_cross", "ma_cross", "momentum"] as strategy_name,
        ["IC2509", "IF2509", "IH2509", "IC2509", "IF2509", "T2509"] as symbol,
        [true, true, false, true, false, false] as enabled,
        [now(), now(), now(), now(), now(), now()] as last_updated
      )
      
      share strategy_control_matrix as strategy_control_matrix
      
      // 策略订阅状态表
      share streamTable(100:0, `strategy_name`symbol`subscription_id`status, 
        [SYMBOL, SYMBOL, STRING, SYMBOL]) as subscription_status
    """)
```

### 1.2 动态订阅管理
```python
def dynamic_subscription_manager(self):
    """动态订阅管理"""
    self.session.run("""
      // 订阅管理器
      subscription_manager = dict(STRING, ANY)
      subscription_manager["active_subscriptions"] = dict(STRING, STRING)
      
      def update_strategy_subscriptions(mutable manager, control_matrix){
        current_subs = manager["active_subscriptions"]
        required_subs = dict(STRING, STRING)
        
        // 计算需要的订阅
        enabled_strategies = select * from control_matrix where enabled = true
        
        for(i in 0..(size(enabled_strategies)-1)){
          strategy = enabled_strategies.strategy_name[i]
          symbol = enabled_strategies.symbol[i]
          sub_key = strategy + "_" + symbol
          required_subs[sub_key] = strategy + ":" + symbol
        }
        
        // 取消不需要的订阅
        for(sub_key in current_subs.keys()){
          if(!(sub_key in required_subs.keys())){
            unsubscribeTable(tableName=`factor_stream, actionName=sub_key)
            current_subs.erase!(sub_key)
            print("Unsubscribed: " + sub_key)
          }
        }
        
        // 添加新的订阅
        for(sub_key in required_subs.keys()){
          if(!(sub_key in current_subs.keys())){
            strategy_symbol = required_subs[sub_key].split(":")
            strategy_name = strategy_symbol[0]
            symbol = strategy_symbol[1]
            
            // 创建策略特定的处理函数
            handler_func = create_strategy_handler(strategy_name, symbol)
            subscribeTable(tableName=`factor_stream, actionName=sub_key, 
                          handler=handler_func, msgAsTable=true, offset=-1)
            
            current_subs[sub_key] = required_subs[sub_key]
            print("Subscribed: " + sub_key)
          }
        }
        
        return size(current_subs)
      }
      
      // 创建策略处理函数
      def create_strategy_handler(strategy_name, symbol){
        if(strategy_name == "bollinger"){
          return bollinger_signal_handler{symbol}
        } else if(strategy_name == "ma_cross"){
          return ma_cross_signal_handler{symbol}
        } else {
          return default_signal_handler{symbol}
        }
      }
    """)
```

### 1.3 前端界面实现
```html
<!-- templates/dashboard.html -->
<!DOCTYPE html>
<html>
<head>
    <title>策略控制面板</title>
    <script src="https://cdn.jsdelivr.net/npm/vue@2"></script>
    <script src="https://cdn.jsdelivr.net/npm/axios/dist/axios.min.js"></script>
    <style>
        .strategy-matrix { margin: 20px; }
        .matrix-table { border-collapse: collapse; width: 100%; }
        .matrix-table th, .matrix-table td { border: 1px solid #ddd; padding: 8px; text-align: center; }
        .enabled { background-color: #d4edda; }
        .disabled { background-color: #f8d7da; }
        .toggle-btn { cursor: pointer; padding: 5px 10px; border: none; border-radius: 3px; }
    </style>
</head>
<body>
    <div id="app">
        <h1>DolphinDB CTP 策略控制面板</h1>
        
        <div class="strategy-matrix">
            <h2>策略控制矩阵</h2>
            <table class="matrix-table">
                <thead>
                    <tr>
                        <th>策略/品种</th>
                        <th v-for="symbol in symbols" :key="symbol">{{ symbol }}</th>
                    </tr>
                </thead>
                <tbody>
                    <tr v-for="strategy in strategies" :key="strategy">
                        <td><strong>{{ strategy }}</strong></td>
                        <td v-for="symbol in symbols" :key="symbol" 
                            :class="getMatrixStatus(strategy, symbol) ? 'enabled' : 'disabled'">
                            <button class="toggle-btn" 
                                    :style="getButtonStyle(strategy, symbol)"
                                    @click="toggleStrategy(strategy, symbol)">
                                {{ getMatrixStatus(strategy, symbol) ? '1' : '0' }}
                            </button>
                        </td>
                    </tr>
                </tbody>
            </table>
        </div>
        
        <div class="status-panel">
            <h2>系统状态</h2>
            <p>活跃订阅数: {{ activeSubscriptions }}</p>
            <p>最后更新: {{ lastUpdate }}</p>
        </div>
    </div>

    <script>
        new Vue({
            el: '#app',
            data: {
                strategies: ['bollinger', 'ma_cross', 'momentum', 'arbitrage'],
                symbols: ['IC2509', 'IF2509', 'IH2509', 'T2509', 'TF2509'],
                strategyMatrix: {},
                activeSubscriptions: 0,
                lastUpdate: new Date().toLocaleString()
            },
            mounted() {
                this.loadStrategyMatrix();
                setInterval(this.loadStrategyMatrix, 5000); // 5秒刷新
            },
            methods: {
                async loadStrategyMatrix() {
                    try {
                        const response = await axios.get('/api/strategy_matrix');
                        this.strategyMatrix = {};
                        response.data.forEach(row => {
                            const key = `${row.strategy_name}_${row.symbol}`;
                            this.strategyMatrix[key] = row.enabled;
                        });
                        this.lastUpdate = new Date().toLocaleString();
                    } catch (error) {
                        console.error('Failed to load strategy matrix:', error);
                    }
                },
                
                getMatrixStatus(strategy, symbol) {
                    const key = `${strategy}_${symbol}`;
                    return this.strategyMatrix[key] || false;
                },
                
                getButtonStyle(strategy, symbol) {
                    const enabled = this.getMatrixStatus(strategy, symbol);
                    return {
                        backgroundColor: enabled ? '#28a745' : '#dc3545',
                        color: 'white'
                    };
                },
                
                async toggleStrategy(strategy, symbol) {
                    const currentStatus = this.getMatrixStatus(strategy, symbol);
                    try {
                        await axios.post('/api/update_strategy', {
                            strategy: strategy,
                            symbol: symbol,
                            enabled: !currentStatus
                        });
                        await this.loadStrategyMatrix();
                    } catch (error) {
                        console.error('Failed to update strategy:', error);
                    }
                }
            }
        });
    </script>
</body>
</html>
```

## 2. 日志警报系统

### 2.1 实时日志处理
```python
class LoggingSystem:
    """日志警报系统"""
    
    def __init__(self):
        self.session = ddb.session()
        self.session.connect("localhost", 8848, "admin", "123456")
        self.setup_logging_tables()
        
    def setup_logging_tables(self):
        """设置日志表"""
        self.session.run("""
          // 系统日志表
          share streamTable(10000:0, `timestamp`level`module`message`details, 
            [TIMESTAMP, SYMBOL, SYMBOL, STRING, STRING]) as system_logs
          
          // 交易日志表
          share streamTable(10000:0, `timestamp`symbol`action`price`volume`status`message, 
            [TIMESTAMP, SYMBOL, SYMBOL, DOUBLE, INT, SYMBOL, STRING]) as trading_logs
          
          // 警报表
          share streamTable(1000:0, `timestamp`alert_type`severity`message`acknowledged, 
            [TIMESTAMP, SYMBOL, SYMBOL, STRING, BOOL]) as alerts
          
          // 警报规则表
          alert_rules = table(
            ["HIGH_LATENCY", "MEMORY_USAGE", "ORDER_FAILURE", "CONNECTION_LOST"] as rule_name,
            ["CRITICAL", "WARNING", "ERROR", "CRITICAL"] as severity,
            [true, true, true, true] as enabled,
            ["latency > 10000", "memory_usage > 0.9", "order_status = 'FAILED'", "connection_status = false"] as condition
          )
          share alert_rules as alert_rules
        """)
    
    def setup_alert_engine(self):
        """设置警报引擎"""
        self.session.run("""
          def check_alert_conditions(logs_data, rules_table){
            triggered_alerts = table(0:0, `timestamp`alert_type`severity`message, 
                                   [TIMESTAMP, SYMBOL, SYMBOL, STRING])
            
            for(i in 0..(size(rules_table)-1)){
              rule = rules_table[i]
              if(!rule.enabled) continue
              
              // 检查警报条件
              if(evaluate_condition(logs_data, rule.condition)){
                alert_message = "Alert triggered: " + rule.rule_name
                triggered_alerts.append!(table(
                  now() as timestamp,
                  rule.rule_name as alert_type,
                  rule.severity as severity,
                  alert_message as message
                ))
              }
            }
            
            return triggered_alerts
          }
          
          // 创建警报引擎
          createReactiveStateEngine(name="alertEngine",
            metrics=<[check_alert_conditions(system_logs, alert_rules)]>,
            dummyTable=system_logs, outputTable=alerts, keyColumn="timestamp")
        """)
```

### 2.2 警报通知系统
```python
def alert_notification_system(self):
    """警报通知系统"""
    
    def alert_handler(alert_data):
        """警报处理函数"""
        for alert in alert_data:
            severity = alert['severity']
            message = alert['message']
            alert_type = alert['alert_type']
            
            # 根据严重程度选择通知方式
            if severity == 'CRITICAL':
                self.send_email_alert(alert)
                self.send_sms_alert(alert)
                self.send_webhook_alert(alert)
            elif severity == 'ERROR':
                self.send_email_alert(alert)
                self.send_webhook_alert(alert)
            elif severity == 'WARNING':
                self.send_webhook_alert(alert)
            
            # 记录警报处理日志
            self.log_alert_action(alert)
    
    def send_email_alert(self, alert):
        """发送邮件警报"""
        import smtplib
        from email.mime.text import MIMEText
        
        subject = f"[{alert['severity']}] {alert['alert_type']}"
        body = f"""
        警报时间: {alert['timestamp']}
        警报类型: {alert['alert_type']}
        严重程度: {alert['severity']}
        详细信息: {alert['message']}
        """
        
        # 发送邮件逻辑
        pass
    
    def send_webhook_alert(self, alert):
        """发送Webhook警报"""
        import requests
        
        webhook_url = "http://your-webhook-endpoint.com/alerts"
        payload = {
            "timestamp": alert['timestamp'],
            "type": alert['alert_type'],
            "severity": alert['severity'],
            "message": alert['message']
        }
        
        try:
            response = requests.post(webhook_url, json=payload, timeout=5)
            response.raise_for_status()
        except Exception as e:
            print(f"Failed to send webhook alert: {e}")
    
    # 订阅警报流
    self.session.subscribe("localhost", 8848, alert_handler, "alerts", offset=-1)
```

## 3. 实时监控面板

### 3.1 策略性能监控
```python
def strategy_performance_monitor(self):
    """策略性能监控"""
    self.session.run("""
      // 策略性能统计表
      share streamTable(1000:0, `timestamp`strategy_name`symbol`pnl`nav`sharpe`max_drawdown`win_rate, 
        [TIMESTAMP, SYMBOL, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE]) as strategy_performance
      
      def calculate_strategy_metrics(strategy_name, symbol, trades_data){
        if(size(trades_data) == 0) return null
        
        // 计算PnL
        total_pnl = sum(trades_data.pnl)
        
        // 计算NAV
        nav = 1.0 + total_pnl / 1000000  // 假设初始资金100万
        
        // 计算Sharpe比率
        returns = trades_data.pnl / 1000000
        sharpe = iif(std(returns) > 0, avg(returns) / std(returns) * sqrt(252), null)
        
        // 计算最大回撤
        cumulative_pnl = cumsum(trades_data.pnl)
        cummax_pnl = cummax(cumulative_pnl)
        drawdowns = (cumulative_pnl - cummax_pnl) / 1000000
        max_drawdown = min(drawdowns)
        
        // 计算胜率
        winning_trades = sum(trades_data.pnl > 0)
        win_rate = winning_trades / double(size(trades_data))
        
        return table(
          now() as timestamp,
          strategy_name as strategy_name,
          symbol as symbol,
          total_pnl as pnl,
          nav as nav,
          sharpe as sharpe,
          max_drawdown as max_drawdown,
          win_rate as win_rate
        )
      }
    """)
```

### 3.2 系统健康监控
```python
def system_health_monitor(self):
    """系统健康监控"""
    self.session.run("""
      // 系统健康状态表
      share streamTable(1000:0, `timestamp`cpu_usage`memory_usage`disk_usage`network_latency`active_connections, 
        [TIMESTAMP, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT]) as system_health
      
      def collect_system_metrics(){
        // 收集系统指标
        memory_info = getMemoryUsage()
        memory_usage_pct = memory_info["used"] / memory_info["total"]
        
        // 网络延迟检测
        network_latency = test_network_latency("localhost", 8848)
        
        // 活跃连接数
        active_connections = getConnectionCount()
        
        return table(
          now() as timestamp,
          0.0 as cpu_usage,  // 需要外部工具获取
          memory_usage_pct as memory_usage,
          0.0 as disk_usage,  // 需要外部工具获取
          network_latency as network_latency,
          active_connections as active_connections
        )
      }
      
      // 定时收集系统指标
      timer_system_health = createTimeSeriesEngine(name="systemHealthTimer",
        windowSize=5000, step=5000,  // 每5秒收集一次
        metrics=<[collect_system_metrics()]>,
        dummyTable=system_logs, outputTable=system_health)
    """)
```

### 3.3 实时图表展示
```javascript
// 前端实时图表 (使用Chart.js)
class RealTimeCharts {
    constructor() {
        this.charts = {};
        this.websocket = null;
        this.setupWebSocket();
        this.initCharts();
    }
    
    setupWebSocket() {
        this.websocket = new WebSocket('ws://localhost:8080/realtime');
        
        this.websocket.onmessage = (event) => {
            const data = JSON.parse(event.data);
            this.updateCharts(data);
        };
    }
    
    initCharts() {
        // PnL图表
        this.charts.pnl = new Chart(document.getElementById('pnlChart'), {
            type: 'line',
            data: {
                labels: [],
                datasets: [{
                    label: 'Cumulative PnL',
                    data: [],
                    borderColor: 'rgb(75, 192, 192)',
                    tension: 0.1
                }]
            },
            options: {
                responsive: true,
                scales: {
                    x: { type: 'time' },
                    y: { beginAtZero: false }
                }
            }
        });
        
        // NAV图表
        this.charts.nav = new Chart(document.getElementById('navChart'), {
            type: 'line',
            data: {
                labels: [],
                datasets: [{
                    label: 'NAV',
                    data: [],
                    borderColor: 'rgb(255, 99, 132)',
                    tension: 0.1
                }]
            }
        });
        
        // 系统资源图表
        this.charts.system = new Chart(document.getElementById('systemChart'), {
            type: 'line',
            data: {
                labels: [],
                datasets: [
                    {
                        label: 'Memory Usage %',
                        data: [],
                        borderColor: 'rgb(54, 162, 235)',
                        yAxisID: 'y'
                    },
                    {
                        label: 'Network Latency (ms)',
                        data: [],
                        borderColor: 'rgb(255, 205, 86)',
                        yAxisID: 'y1'
                    }
                ]
            },
            options: {
                scales: {
                    y: { type: 'linear', position: 'left' },
                    y1: { type: 'linear', position: 'right' }
                }
            }
        });
    }
    
    updateCharts(data) {
        if (data.type === 'strategy_performance') {
            this.updateStrategyCharts(data);
        } else if (data.type === 'system_health') {
            this.updateSystemCharts(data);
        }
    }
    
    updateStrategyCharts(data) {
        const pnlChart = this.charts.pnl;
        const navChart = this.charts.nav;
        
        // 更新PnL图表
        pnlChart.data.labels.push(new Date(data.timestamp));
        pnlChart.data.datasets[0].data.push(data.pnl);
        
        // 保持最近100个数据点
        if (pnlChart.data.labels.length > 100) {
            pnlChart.data.labels.shift();
            pnlChart.data.datasets[0].data.shift();
        }
        
        pnlChart.update('none');
        
        // 更新NAV图表
        navChart.data.labels.push(new Date(data.timestamp));
        navChart.data.datasets[0].data.push(data.nav);
        
        if (navChart.data.labels.length > 100) {
            navChart.data.labels.shift();
            navChart.data.datasets[0].data.shift();
        }
        
        navChart.update('none');
    }
}

// 初始化图表
const charts = new RealTimeCharts();
```

## 4. WebSocket实时数据推送

### 4.1 WebSocket服务器
```python
import asyncio
import websockets
import json
import threading

class WebSocketServer:
    """WebSocket实时数据推送服务器"""
    
    def __init__(self):
        self.clients = set()
        self.session = ddb.session()
        self.session.connect("localhost", 8848, "admin", "123456")
        
    async def register_client(self, websocket, path):
        """注册客户端"""
        self.clients.add(websocket)
        try:
            await websocket.wait_closed()
        finally:
            self.clients.remove(websocket)
    
    async def broadcast_data(self, data):
        """广播数据到所有客户端"""
        if self.clients:
            message = json.dumps(data)
            await asyncio.gather(
                *[client.send(message) for client in self.clients],
                return_exceptions=True
            )
    
    def start_data_streaming(self):
        """启动数据流推送"""
        def strategy_performance_handler(data):
            asyncio.run(self.broadcast_data({
                'type': 'strategy_performance',
                'data': data
            }))
        
        def system_health_handler(data):
            asyncio.run(self.broadcast_data({
                'type': 'system_health', 
                'data': data
            }))
        
        # 订阅DolphinDB数据流
        self.session.subscribe("localhost", 8848, strategy_performance_handler, 
                              "strategy_performance", offset=-1)
        self.session.subscribe("localhost", 8848, system_health_handler,
                              "system_health", offset=-1)
    
    def run_server(self):
        """运行WebSocket服务器"""
        start_server = websockets.serve(self.register_client, "localhost", 8080)
        
        # 启动数据流
        threading.Thread(target=self.start_data_streaming, daemon=True).start()
        
        asyncio.get_event_loop().run_until_complete(start_server)
        asyncio.get_event_loop().run_forever()
```

## 5. 实施计划

### 第1-2周：策略交互界面
- 开发策略控制矩阵界面
- 实现动态订阅管理
- 测试策略开关功能

### 第3周：日志警报系统
- 实现实时日志处理
- 开发警报引擎和通知系统
- 集成邮件/短信/Webhook通知

### 第4周：实时监控面板
- 开发策略性能监控
- 实现系统健康监控
- 集成WebSocket实时推送

### 第5-6周：优化和集成
- 性能优化和压力测试
- 用户体验优化
- 系统集成测试

## 6. 技术栈

### 6.1 后端技术
- Python Flask (Web框架)
- DolphinDB Python API
- WebSocket (实时通信)
- SQLite (配置存储)

### 6.2 前端技术
- Vue.js (前端框架)
- Chart.js (图表库)
- Bootstrap (UI框架)
- WebSocket (实时数据)
