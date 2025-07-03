# 综合交易系统框架开发方案

## 1. 项目概述

### 1.1 框架目标
基于用户偏好和记忆，开发一个完整的DolphinDB交易系统框架，包含：
- 分离的策略和执行组件
- 全面的架构文档和源代码
- 详细的部署说明
- 实时tick数据可视化
- 信号和PnL更新的前端监控
- 8GB RAM限制优化
- 3天数据保留策略

### 1.2 系统架构
```
┌─────────────────────────────────────────────────────────────────┐
│                    Comprehensive Trading Framework              │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────┐ │
│  │ Data Layer  │  │Strategy Layer│  │Execution Lay│  │Frontend │ │
│  │             │  │             │  │             │  │Monitor  │ │
│  │ - Tick Data │  │ - Factor    │  │ - Order Mgmt│  │ - Real  │ │
│  │ - K-line    │  │   Calc      │  │ - Risk Ctrl │  │   Time  │ │
│  │ - Market    │  │ - Signal    │  │ - Position  │  │ - PnL   │ │
│  │   Data      │  │   Gen       │  │   Mgmt      │  │ - Alerts│ │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

## 2. 数据层设计

### 2.1 流表架构
```python
def setup_streaming_tables(session):
    """设置流表架构"""
    session.run("""
      // 原始tick数据流表 - 3天保留
      share streamTable(100000:0, 
        `timestamp`symbol`price`volume`bid1`ask1`bid_vol1`ask_vol1`gateway, 
        [TIMESTAMP, SYMBOL, DOUBLE, LONG, DOUBLE, DOUBLE, LONG, LONG, SYMBOL]) as tick_stream
      
      // K线数据流表 - 多时间周期
      share streamTable(50000:0,
        `timestamp`symbol`open`high`low`close`volume`interval,
        [TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, LONG, SYMBOL]) as kline_stream
      
      // 因子计算结果流表
      share streamTable(10000:0,
        `timestamp`symbol`factor_name`factor_value`signal_strength,
        [TIMESTAMP, SYMBOL, SYMBOL, DOUBLE, DOUBLE]) as factor_stream
      
      // 信号生成流表
      share streamTable(5000:0,
        `timestamp`symbol`strategy_name`signal_type`signal_value`confidence,
        [TIMESTAMP, SYMBOL, SYMBOL, SYMBOL, INT, DOUBLE]) as signal_stream
      
      // 订单流表
      share streamTable(10000:0,
        `timestamp`symbol`direction`price`volume`order_type`status`order_id,
        [TIMESTAMP, SYMBOL, SYMBOL, DOUBLE, INT, SYMBOL, SYMBOL, STRING]) as order_stream
      
      // 成交流表
      share streamTable(10000:0,
        `timestamp`symbol`direction`price`volume`commission`trade_id,
        [TIMESTAMP, SYMBOL, SYMBOL, DOUBLE, INT, DOUBLE, STRING]) as trade_stream
      
      // 持仓流表
      share streamTable(1000:0,
        `timestamp`symbol`position`avg_price`unrealized_pnl`realized_pnl,
        [TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE]) as position_stream
      
      // PnL流表
      share streamTable(5000:0,
        `timestamp`strategy_name`symbol`trade_pnl`position_pnl`total_pnl`nav,
        [TIMESTAMP, SYMBOL, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE]) as pnl_stream
    """)
```

### 2.2 键值表设计
```python
def setup_keyed_tables(session):
    """设置键值表"""
    session.run("""
      // 策略配置键值表
      strategy_config = keyedTable(`strategy_name`symbol, 
        table(
          ["bollinger", "ma_cross", "momentum"] as strategy_name,
          ["IC2509", "IF2509", "IH2509"] as symbol,
          [true, true, false] as enabled,
          [20, 10, 14] as param1,
          [2.0, 1.5, 0.7] as param2
        ))
      share strategy_config as strategy_config
      
      // 风控参数键值表
      risk_config = keyedTable(`symbol,
        table(
          ["IC2509", "IF2509", "IH2509", "T2509"] as symbol,
          [100, 100, 50, 200] as max_position,
          [50000, 50000, 25000, 100000] as max_order_value,
          [0.02, 0.02, 0.02, 0.01] as max_position_ratio
        ))
      share risk_config as risk_config
      
      // 实时持仓键值表
      current_positions = keyedTable(`symbol,
        table(
          string() as symbol,
          double() as position,
          double() as avg_price,
          double() as unrealized_pnl
        ))
      share current_positions as current_positions
      
      // 策略状态键值表
      strategy_status = keyedTable(`strategy_name`symbol,
        table(
          string() as strategy_name,
          string() as symbol,
          bool() as enabled,
          timestamp() as last_signal_time,
          int() as signal_count
        ))
      share strategy_status as strategy_status
    """)
```

## 3. 策略层设计

### 3.1 因子计算引擎
```python
def setup_factor_engines(session):
    """设置因子计算引擎"""
    session.run("""
      // 技术指标计算函数
      use ta
      
      def calculate_bollinger_bands(prices, period=20, std_dev=2){
        if(size(prices) < period) return null
        
        sma = mavg(prices, period)
        std = mstd(prices, period)
        upper = sma + std_dev * std
        lower = sma - std_dev * std
        
        return dict(["upper", "middle", "lower"], [upper[-1], sma[-1], lower[-1]])
      }
      
      def calculate_ma_cross(prices, fast_period=5, slow_period=20){
        if(size(prices) < slow_period) return null
        
        fast_ma = mavg(prices, fast_period)
        slow_ma = mavg(prices, slow_period)
        
        return dict(["fast_ma", "slow_ma", "cross"], 
                   [fast_ma[-1], slow_ma[-1], fast_ma[-1] - slow_ma[-1]])
      }
      
      def calculate_momentum(prices, period=14){
        if(size(prices) < period + 1) return null
        
        momentum = prices[-1] / prices[-period-1] - 1
        return dict(["momentum"], [momentum])
      }
      
      // 因子计算引擎
      createReactiveStateEngine(name="factorEngine",
        metrics=<[
          timestamp,
          symbol,
          calculate_bollinger_bands(price) as bollinger,
          calculate_ma_cross(price) as ma_cross,
          calculate_momentum(price) as momentum
        ]>,
        dummyTable=kline_stream, outputTable=factor_stream, 
        keyColumn="symbol", keepOrder=true)
    """)
```

### 3.2 信号生成引擎
```python
def setup_signal_engines(session):
    """设置信号生成引擎"""
    session.run("""
      // 布林带策略信号
      def bollinger_signal(factor_data, config){
        if(isNull(factor_data.bollinger)) return null
        
        bb = factor_data.bollinger
        price = factor_data.price
        
        signal_type = ""
        signal_value = 0
        confidence = 0.0
        
        if(price < bb["lower"]){
          signal_type = "LONG_OPEN"
          signal_value = 1
          confidence = (bb["lower"] - price) / bb["lower"]
        } else if(price > bb["upper"]){
          signal_type = "SHORT_OPEN"
          signal_value = -1
          confidence = (price - bb["upper"]) / bb["upper"]
        } else if(abs(price - bb["middle"]) / bb["middle"] < 0.001){
          signal_type = "CLOSE"
          signal_value = 0
          confidence = 0.8
        }
        
        return dict(["signal_type", "signal_value", "confidence"], 
                   [signal_type, signal_value, confidence])
      }
      
      // 均线交叉策略信号
      def ma_cross_signal(factor_data, config){
        if(isNull(factor_data.ma_cross)) return null
        
        ma = factor_data.ma_cross
        cross_value = ma["cross"]
        
        signal_type = ""
        signal_value = 0
        confidence = abs(cross_value) / ma["slow_ma"]
        
        if(cross_value > 0){
          signal_type = "LONG_OPEN"
          signal_value = 1
        } else if(cross_value < 0){
          signal_type = "SHORT_OPEN"
          signal_value = -1
        }
        
        return dict(["signal_type", "signal_value", "confidence"], 
                   [signal_type, signal_value, confidence])
      }
      
      // 信号生成引擎
      createReactiveStateEngine(name="signalEngine",
        metrics=<[
          timestamp,
          symbol,
          "bollinger" as strategy_name,
          bollinger_signal(factor_stream, strategy_config) as signal_data
        ]>,
        dummyTable=factor_stream, outputTable=signal_stream,
        keyColumn="symbol", keepOrder=true)
    """)
```

## 4. 执行层设计

### 4.1 订单管理引擎
```python
def setup_order_management(session):
    """设置订单管理引擎"""
    session.run("""
      // 订单ID生成器
      order_id_counter = 0
      
      def generate_order_id(){
        order_id_counter += 1
        return "ORD" + string(now().date()) + "_" + string(order_id_counter)
      }
      
      // 风险检查函数
      def risk_check(symbol, direction, volume, price, current_pos, risk_params){
        // 持仓限制检查
        new_position = current_pos + (direction == "BUY" ? volume : -volume)
        if(abs(new_position) > risk_params["max_position"]){
          return dict(["approved", "reason"], [false, "POSITION_LIMIT"])
        }
        
        // 订单金额检查
        order_value = volume * price
        if(order_value > risk_params["max_order_value"]){
          return dict(["approved", "reason"], [false, "ORDER_VALUE_LIMIT"])
        }
        
        return dict(["approved", "reason"], [true, "APPROVED"])
      }
      
      // TWAP执行算法
      def twap_execution(symbol, total_volume, duration_minutes, direction){
        slice_count = duration_minutes
        slice_volume = total_volume / slice_count
        
        orders = table(0:0, `timestamp`symbol`direction`volume`order_type,
                      [TIMESTAMP, SYMBOL, SYMBOL, INT, SYMBOL])
        
        for(i in 0..(slice_count-1)){
          execution_time = now() + i * 60000  // 每分钟执行一次
          orders.append!(table(
            execution_time as timestamp,
            symbol as symbol,
            direction as direction,
            slice_volume as volume,
            "MARKET" as order_type
          ))
        }
        
        return orders
      }
      
      // 订单执行引擎
      createReactiveStateEngine(name="orderEngine",
        metrics=<[
          process_signal(signal_stream, current_positions, risk_config)
        ]>,
        dummyTable=signal_stream, outputTable=order_stream,
        keyColumn="symbol", keepOrder=true)
    """)
```

### 4.2 持仓管理引擎
```python
def setup_position_management(session):
    """设置持仓管理引擎"""
    session.run("""
      // 持仓更新函数
      def update_position(mutable positions, trade_data){
        symbol = trade_data.symbol[0]
        direction = trade_data.direction[0]
        volume = trade_data.volume[0]
        price = trade_data.price[0]
        
        current_pos = positions.get(symbol, dict(["position", "avg_price"], [0.0, 0.0]))
        
        old_position = current_pos["position"]
        old_avg_price = current_pos["avg_price"]
        
        // 计算新持仓
        volume_change = direction == "BUY" ? volume : -volume
        new_position = old_position + volume_change
        
        // 计算新均价
        if(new_position == 0){
          new_avg_price = 0.0
        } else if(old_position == 0){
          new_avg_price = price
        } else if((old_position > 0 && volume_change > 0) || (old_position < 0 && volume_change < 0)){
          // 加仓
          total_cost = old_position * old_avg_price + volume_change * price
          new_avg_price = total_cost / new_position
        } else {
          // 减仓或反向
          new_avg_price = old_avg_price
        }
        
        // 更新持仓
        positions[symbol] = dict(["position", "avg_price"], [new_position, new_avg_price])
        
        return table(
          now() as timestamp,
          symbol as symbol,
          new_position as position,
          new_avg_price as avg_price,
          0.0 as unrealized_pnl  // 将在PnL引擎中计算
        )
      }
      
      // 持仓管理引擎
      createReactiveStateEngine(name="positionEngine",
        metrics=<[update_position(current_positions, trade_stream)]>,
        dummyTable=trade_stream, outputTable=position_stream,
        keyColumn="symbol", keepOrder=true)
    """)
```

## 5. 前端监控系统

### 5.1 实时数据展示
```python
def setup_frontend_monitoring(session):
    """设置前端监控系统"""
    session.run("""
      // 实时监控数据聚合
      share streamTable(1000:0,
        `timestamp`symbol`last_price`bid_ask_spread`volume_1min`signal_count`pnl_1min,
        [TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, LONG, INT, DOUBLE]) as monitor_stream
      
      def aggregate_monitor_data(tick_data, signal_data, pnl_data){
        monitor_data = table(0:0,
          `timestamp`symbol`last_price`bid_ask_spread`volume_1min`signal_count`pnl_1min,
          [TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, LONG, INT, DOUBLE])
        
        // 聚合tick数据
        tick_agg = select 
          last(timestamp) as timestamp,
          symbol,
          last(price) as last_price,
          last(ask1 - bid1) as bid_ask_spread,
          sum(volume) as volume_1min
          from tick_data
          group by symbol
        
        // 聚合信号数据
        signal_agg = select
          symbol,
          count(*) as signal_count
          from signal_data
          group by symbol
        
        // 聚合PnL数据
        pnl_agg = select
          symbol,
          last(total_pnl) as pnl_1min
          from pnl_data
          group by symbol
        
        // 合并数据
        result = lj(tick_agg, signal_agg, "symbol")
        result = lj(result, pnl_agg, "symbol")
        
        return result
      }
      
      // 监控数据聚合引擎
      createTimeSeriesEngine(name="monitorEngine",
        windowSize=60000, step=10000,  // 1分钟窗口，10秒更新
        metrics=<[aggregate_monitor_data(tick_stream, signal_stream, pnl_stream)]>,
        dummyTable=tick_stream, outputTable=monitor_stream,
        timeColumn="timestamp", keyColumn="symbol")
    """)
```

### 5.2 WebSocket实时推送
```python
# frontend/websocket_server.py
import asyncio
import websockets
import json
import dolphindb as ddb
from datetime import datetime

class TradingMonitorServer:
    """交易监控WebSocket服务器"""
    
    def __init__(self):
        self.clients = set()
        self.session = ddb.session()
        self.session.connect("localhost", 8848, "admin", "123456")
        
    async def register_client(self, websocket, path):
        """注册客户端连接"""
        self.clients.add(websocket)
        print(f"Client connected: {websocket.remote_address}")
        
        try:
            await websocket.wait_closed()
        finally:
            self.clients.remove(websocket)
            print(f"Client disconnected: {websocket.remote_address}")
    
    async def broadcast_data(self, data_type, data):
        """广播数据到所有客户端"""
        if self.clients:
            message = json.dumps({
                'type': data_type,
                'timestamp': datetime.now().isoformat(),
                'data': data
            })
            
            # 发送给所有连接的客户端
            disconnected = set()
            for client in self.clients:
                try:
                    await client.send(message)
                except websockets.exceptions.ConnectionClosed:
                    disconnected.add(client)
            
            # 清理断开的连接
            self.clients -= disconnected
    
    def start_data_streaming(self):
        """启动数据流订阅"""
        
        def tick_handler(data):
            """处理tick数据"""
            tick_data = []
            for i in range(len(data)):
                tick_data.append({
                    'symbol': data['symbol'][i],
                    'price': data['price'][i],
                    'volume': data['volume'][i],
                    'bid': data['bid1'][i],
                    'ask': data['ask1'][i],
                    'spread': data['ask1'][i] - data['bid1'][i]
                })
            
            asyncio.run(self.broadcast_data('tick', tick_data))
        
        def signal_handler(data):
            """处理信号数据"""
            signal_data = []
            for i in range(len(data)):
                signal_data.append({
                    'symbol': data['symbol'][i],
                    'strategy': data['strategy_name'][i],
                    'signal_type': data['signal_type'][i],
                    'signal_value': data['signal_value'][i],
                    'confidence': data['confidence'][i]
                })
            
            asyncio.run(self.broadcast_data('signal', signal_data))
        
        def pnl_handler(data):
            """处理PnL数据"""
            pnl_data = []
            for i in range(len(data)):
                pnl_data.append({
                    'strategy': data['strategy_name'][i],
                    'symbol': data['symbol'][i],
                    'trade_pnl': data['trade_pnl'][i],
                    'position_pnl': data['position_pnl'][i],
                    'total_pnl': data['total_pnl'][i],
                    'nav': data['nav'][i]
                })
            
            asyncio.run(self.broadcast_data('pnl', pnl_data))
        
        # 订阅DolphinDB流表
        self.session.subscribe("localhost", 8848, tick_handler, "tick_stream", offset=-1)
        self.session.subscribe("localhost", 8848, signal_handler, "signal_stream", offset=-1)
        self.session.subscribe("localhost", 8848, pnl_handler, "pnl_stream", offset=-1)
    
    def run_server(self, host="localhost", port=8080):
        """运行WebSocket服务器"""
        print(f"Starting WebSocket server on {host}:{port}")
        
        # 启动数据流订阅
        import threading
        threading.Thread(target=self.start_data_streaming, daemon=True).start()
        
        # 启动WebSocket服务器
        start_server = websockets.serve(self.register_client, host, port)
        asyncio.get_event_loop().run_until_complete(start_server)
        asyncio.get_event_loop().run_forever()

if __name__ == "__main__":
    server = TradingMonitorServer()
    server.run_server()
```

## 6. 数据保留策略

### 6.1 3天数据保留实现
```python
def setup_data_retention(session):
    """设置3天数据保留策略"""
    session.run("""
      // 数据清理函数
      def cleanup_old_data(){
        cutoff_time = now() - 3 * 24 * 60 * 60 * 1000  // 3天前
        
        // 清理tick数据
        delete from tick_stream where timestamp < cutoff_time
        
        // 清理K线数据
        delete from kline_stream where timestamp < cutoff_time
        
        // 清理因子数据
        delete from factor_stream where timestamp < cutoff_time
        
        // 清理信号数据
        delete from signal_stream where timestamp < cutoff_time
        
        // 清理订单数据（保留更长时间用于审计）
        order_cutoff = now() - 7 * 24 * 60 * 60 * 1000  // 7天前
        delete from order_stream where timestamp < order_cutoff
        
        // 清理成交数据（保留更长时间用于审计）
        delete from trade_stream where timestamp < order_cutoff
        
        print("Data cleanup completed at: " + string(now()))
      }
      
      // 定时清理任务 - 每天凌晨2点执行
      timer_cleanup = createTimeSeriesEngine(name="dataCleanup",
        windowSize=24*60*60*1000, step=24*60*60*1000,  // 24小时
        metrics=<[cleanup_old_data()]>,
        dummyTable=tick_stream, outputTable=tick_stream)
    """)
```

## 7. 实施计划

### 第1-2周：核心架构开发
- 流表和键值表设计实现
- 因子计算引擎开发
- 信号生成引擎开发

### 第3-4周：执行层开发
- 订单管理引擎实现
- 持仓管理引擎开发
- TWAP执行算法实现

### 第5-6周：前端监控系统
- WebSocket实时推送开发
- 前端界面实现
- 实时图表和警报系统

### 第7-8周：优化和文档
- 8GB RAM优化
- 3天数据保留实现
- 完整文档编写
- 部署脚本开发

## 8. 线程化订单处理

### 8.1 多线程订单处理架构
```python
def setup_threaded_order_processing(session):
    """设置线程化订单处理"""
    session.run("""
      // 订单队列表
      share streamTable(1000:0,
        `timestamp`order_id`symbol`direction`volume`price`priority`status,
        [TIMESTAMP, STRING, SYMBOL, SYMBOL, INT, DOUBLE, INT, SYMBOL]) as order_queue

      // 订单处理线程配置
      thread_config = dict(STRING, INT)
      thread_config["order_threads"] = 4
      thread_config["risk_threads"] = 2
      thread_config["execution_threads"] = 2

      // 线程化订单处理函数
      def threaded_order_processor(order_data, thread_id){
        // 风险检查
        risk_result = parallel_risk_check(order_data, thread_id)
        if(!risk_result["approved"]) return null

        // 订单路由
        gateway = select_optimal_gateway(order_data.symbol)

        // 执行订单
        execution_result = execute_order_async(order_data, gateway, thread_id)

        return execution_result
      }

      // 创建多线程订单处理引擎
      for(i in 0..(thread_config["order_threads"]-1)){
        engine_name = "orderProcessor_" + string(i)
        createReactiveStateEngine(name=engine_name,
          metrics=<[threaded_order_processor(order_queue, i)]>,
          dummyTable=order_queue, outputTable=order_stream,
          keyColumn="order_id", keepOrder=false)
      }
    """)
```

### 8.2 简单Tick计数器
```python
def setup_tick_counter_orders(session):
    """设置基于tick计数的订单生成"""
    session.run("""
      // Tick计数器
      tick_counters = dict(SYMBOL, INT)

      def tick_counter_strategy(mutable counters, tick_data, threshold=100){
        symbol = tick_data.symbol[0]
        current_count = counters.get(symbol, 0) + 1
        counters[symbol] = current_count

        // 每100个tick生成一个订单
        if(current_count % threshold == 0){
          order_data = table(
            now() as timestamp,
            "TICK_" + string(current_count) as order_id,
            symbol as symbol,
            iif(current_count % 200 == 0, "BUY", "SELL") as direction,
            1 as volume,
            tick_data.price[0] as price,
            1 as priority,
            "PENDING" as status
          )

          return order_data
        }

        return null
      }

      // Tick计数订单生成引擎
      createReactiveStateEngine(name="tickCounterEngine",
        metrics=<[tick_counter_strategy(tick_counters, tick_stream, 100)]>,
        dummyTable=tick_stream, outputTable=order_queue,
        keyColumn="symbol", keepOrder=true)
    """)
```

## 9. 前端主题优化

### 9.1 避免蓝色主题配置
```css
/* frontend/static/css/trading_theme.css */
:root {
  /* 主色调 - 避免蓝色 */
  --primary-color: #28a745;      /* 绿色 */
  --secondary-color: #6c757d;    /* 灰色 */
  --success-color: #28a745;      /* 成功绿 */
  --danger-color: #dc3545;       /* 危险红 */
  --warning-color: #ffc107;      /* 警告黄 */
  --info-color: #17a2b8;         /* 信息青色 */

  /* 背景色 */
  --bg-primary: #ffffff;         /* 主背景白色 */
  --bg-secondary: #f8f9fa;       /* 次背景浅灰 */
  --bg-dark: #343a40;            /* 深色背景 */

  /* 文字色 */
  --text-primary: #212529;       /* 主文字深灰 */
  --text-secondary: #6c757d;     /* 次文字灰色 */
  --text-light: #ffffff;         /* 浅色文字 */

  /* 边框色 */
  --border-color: #dee2e6;       /* 边框灰色 */
  --border-dark: #495057;        /* 深色边框 */
}

/* 实时数据表格样式 */
.tick-data-table {
  background-color: var(--bg-primary);
  border: 1px solid var(--border-color);
  color: var(--text-primary);
}

.tick-data-table th {
  background-color: var(--bg-secondary);
  color: var(--text-primary);
  border-bottom: 2px solid var(--border-dark);
}

.tick-data-table td {
  border-bottom: 1px solid var(--border-color);
  padding: 8px 12px;
}

/* 价格变动颜色 */
.price-up {
  color: var(--success-color);
  font-weight: bold;
}

.price-down {
  color: var(--danger-color);
  font-weight: bold;
}

.price-unchanged {
  color: var(--text-secondary);
}

/* 信号指示器 */
.signal-long {
  background-color: var(--success-color);
  color: var(--text-light);
  padding: 2px 6px;
  border-radius: 3px;
}

.signal-short {
  background-color: var(--danger-color);
  color: var(--text-light);
  padding: 2px 6px;
  border-radius: 3px;
}

.signal-close {
  background-color: var(--warning-color);
  color: var(--text-primary);
  padding: 2px 6px;
  border-radius: 3px;
}
```

### 9.2 实时Tick和价差状态显示
```html
<!-- frontend/templates/live_monitor.html -->
<!DOCTYPE html>
<html>
<head>
    <title>实时交易监控</title>
    <link rel="stylesheet" href="/static/css/trading_theme.css">
    <script src="https://cdn.jsdelivr.net/npm/vue@2"></script>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
</head>
<body>
    <div id="app">
        <div class="container-fluid">
            <!-- 实时Tick数据展示 -->
            <div class="row">
                <div class="col-md-8">
                    <h3>实时Tick数据</h3>
                    <table class="tick-data-table table table-striped">
                        <thead>
                            <tr>
                                <th>品种</th>
                                <th>最新价</th>
                                <th>涨跌</th>
                                <th>买一价</th>
                                <th>卖一价</th>
                                <th>价差</th>
                                <th>成交量</th>
                                <th>更新时间</th>
                            </tr>
                        </thead>
                        <tbody>
                            <tr v-for="tick in tickData" :key="tick.symbol">
                                <td>{{ tick.symbol }}</td>
                                <td :class="getPriceClass(tick.price_change)">
                                    {{ tick.price.toFixed(2) }}
                                </td>
                                <td :class="getPriceClass(tick.price_change)">
                                    {{ tick.price_change > 0 ? '+' : '' }}{{ tick.price_change.toFixed(2) }}
                                </td>
                                <td>{{ tick.bid.toFixed(2) }}</td>
                                <td>{{ tick.ask.toFixed(2) }}</td>
                                <td :class="getSpreadClass(tick.spread)">
                                    {{ tick.spread.toFixed(3) }}
                                </td>
                                <td>{{ tick.volume }}</td>
                                <td>{{ formatTime(tick.timestamp) }}</td>
                            </tr>
                        </tbody>
                    </table>
                </div>

                <!-- 信号和PnL状态 -->
                <div class="col-md-4">
                    <h3>策略信号</h3>
                    <div class="signal-panel">
                        <div v-for="signal in latestSignals" :key="signal.id" class="signal-item">
                            <span class="signal-symbol">{{ signal.symbol }}</span>
                            <span :class="getSignalClass(signal.signal_type)">
                                {{ signal.signal_type }}
                            </span>
                            <span class="signal-confidence">{{ (signal.confidence * 100).toFixed(1) }}%</span>
                        </div>
                    </div>

                    <h3>实时PnL</h3>
                    <div class="pnl-panel">
                        <div class="pnl-summary">
                            <div class="pnl-item">
                                <label>总PnL:</label>
                                <span :class="getPnlClass(totalPnl)">{{ totalPnl.toFixed(2) }}</span>
                            </div>
                            <div class="pnl-item">
                                <label>今日PnL:</label>
                                <span :class="getPnlClass(dailyPnl)">{{ dailyPnl.toFixed(2) }}</span>
                            </div>
                            <div class="pnl-item">
                                <label>净值:</label>
                                <span>{{ nav.toFixed(4) }}</span>
                            </div>
                        </div>
                    </div>
                </div>
            </div>

            <!-- 价差状态图表 -->
            <div class="row mt-4">
                <div class="col-12">
                    <h3>价差状态监控</h3>
                    <canvas id="spreadChart" width="400" height="200"></canvas>
                </div>
            </div>
        </div>
    </div>

    <script>
        new Vue({
            el: '#app',
            data: {
                tickData: [],
                latestSignals: [],
                totalPnl: 0,
                dailyPnl: 0,
                nav: 1.0000,
                websocket: null,
                spreadChart: null
            },
            mounted() {
                this.initWebSocket();
                this.initSpreadChart();
            },
            methods: {
                initWebSocket() {
                    this.websocket = new WebSocket('ws://localhost:8080');

                    this.websocket.onmessage = (event) => {
                        const data = JSON.parse(event.data);
                        this.handleWebSocketData(data);
                    };
                },

                handleWebSocketData(data) {
                    switch(data.type) {
                        case 'tick':
                            this.updateTickData(data.data);
                            break;
                        case 'signal':
                            this.updateSignals(data.data);
                            break;
                        case 'pnl':
                            this.updatePnL(data.data);
                            break;
                    }
                },

                updateTickData(ticks) {
                    ticks.forEach(tick => {
                        const existingIndex = this.tickData.findIndex(t => t.symbol === tick.symbol);
                        if (existingIndex >= 0) {
                            // 计算价格变动
                            const oldPrice = this.tickData[existingIndex].price;
                            tick.price_change = tick.price - oldPrice;
                            this.tickData.splice(existingIndex, 1, tick);
                        } else {
                            tick.price_change = 0;
                            this.tickData.push(tick);
                        }
                    });

                    // 更新价差图表
                    this.updateSpreadChart();
                },

                updateSignals(signals) {
                    this.latestSignals = signals.slice(-10); // 保留最新10个信号
                },

                updatePnL(pnlData) {
                    this.totalPnl = pnlData.reduce((sum, item) => sum + item.total_pnl, 0);
                    this.dailyPnl = pnlData.reduce((sum, item) => sum + item.trade_pnl, 0);
                    this.nav = pnlData.length > 0 ? pnlData[0].nav : 1.0000;
                },

                getPriceClass(change) {
                    if (change > 0) return 'price-up';
                    if (change < 0) return 'price-down';
                    return 'price-unchanged';
                },

                getSpreadClass(spread) {
                    return spread > 0.1 ? 'text-warning' : 'text-success';
                },

                getSignalClass(signalType) {
                    if (signalType.includes('LONG')) return 'signal-long';
                    if (signalType.includes('SHORT')) return 'signal-short';
                    return 'signal-close';
                },

                getPnlClass(pnl) {
                    return pnl >= 0 ? 'text-success' : 'text-danger';
                },

                formatTime(timestamp) {
                    return new Date(timestamp).toLocaleTimeString();
                },

                initSpreadChart() {
                    const ctx = document.getElementById('spreadChart').getContext('2d');
                    this.spreadChart = new Chart(ctx, {
                        type: 'line',
                        data: {
                            labels: [],
                            datasets: [{
                                label: '平均价差',
                                data: [],
                                borderColor: '#28a745',
                                backgroundColor: 'rgba(40, 167, 69, 0.1)',
                                tension: 0.1
                            }]
                        },
                        options: {
                            responsive: true,
                            scales: {
                                y: {
                                    beginAtZero: true,
                                    title: {
                                        display: true,
                                        text: '价差 (点)'
                                    }
                                },
                                x: {
                                    title: {
                                        display: true,
                                        text: '时间'
                                    }
                                }
                            }
                        }
                    });
                },

                updateSpreadChart() {
                    if (!this.spreadChart || this.tickData.length === 0) return;

                    const avgSpread = this.tickData.reduce((sum, tick) => sum + tick.spread, 0) / this.tickData.length;
                    const now = new Date().toLocaleTimeString();

                    this.spreadChart.data.labels.push(now);
                    this.spreadChart.data.datasets[0].data.push(avgSpread);

                    // 保持最近50个数据点
                    if (this.spreadChart.data.labels.length > 50) {
                        this.spreadChart.data.labels.shift();
                        this.spreadChart.data.datasets[0].data.shift();
                    }

                    this.spreadChart.update('none');
                }
            }
        });
    </script>
</body>
</html>
```

## 10. 预期收益

### 10.1 系统完整性
- 端到端交易系统
- 完整的架构文档
- 详细的部署说明

### 10.2 实时监控能力
- 实时tick数据展示
- 信号和PnL实时更新
- 系统状态监控

### 10.3 资源优化
- 8GB RAM限制优化
- 3天数据保留策略
- 高效的内存使用

### 10.4 用户体验优化
- 避免蓝色主题，提高可读性
- 实时tick和价差状态显示
- 简单tick计数器订单生成
- 线程化订单处理提升性能
