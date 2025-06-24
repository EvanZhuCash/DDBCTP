# 复杂订单管理改造方案

## 1. 订单跟踪和撤单重发 (任务d)

### 1.1 当前问题分析
现有`cancel_order`函数功能单一，需要升级为完整的订单跟踪系统。

### 1.2 order_tracking函数设计
```python
def order_tracking(self):
    """订单跟踪函数 - 监控价格变化并生成撤单重发指令"""
    self.session.run("""
      // 订单跟踪配置
      tracking_config = dict(STRING, DOUBLE)
      tracking_config["price_tolerance"] = 0.02  // 价格容忍度2%
      tracking_config["time_threshold"] = 30000  // 30秒未成交则考虑撤单
      
      def order_tracking_logic(mutable tracking_config, orders_table, tick_data, positions_orders){
        current_time = tick_data.timestamp[0]
        current_symbol = tick_data.symbol[0] 
        current_price = tick_data.price[0]
        
        // 查找该品种的未成交订单
        pending_orders = select * from orders_table 
          where symbol = current_symbol and status in ["SUBMITTED", "PARTIAL_FILLED"]
        
        cancel_reorder_actions = table(0:0, `orderid`action`new_price, [INT, STRING, DOUBLE])
        
        for(i in 0..(size(pending_orders)-1)){
          order_id = pending_orders.orderid[i]
          order_price = pending_orders.price[i]
          order_time = pending_orders.timestamp[i]
          order_direction = pending_orders.direction[i]
          
          // 计算价格偏离度
          price_deviation = abs(current_price - order_price) / order_price
          time_elapsed = current_time - order_time
          
          should_cancel = false
          new_price = order_price
          
          // 价格偏离超过阈值
          if(price_deviation > tracking_config["price_tolerance"]){
            should_cancel = true
            // 计算新的委托价格
            if(order_direction == "BUY"){
              new_price = current_price * 1.001  // 买单稍高于市价
            } else {
              new_price = current_price * 0.999  // 卖单稍低于市价
            }
          }
          
          // 时间超过阈值
          if(time_elapsed > tracking_config["time_threshold"]){
            should_cancel = true
            new_price = current_price  // 使用当前市价
          }
          
          if(should_cancel){
            cancel_reorder_actions.append!(table(order_id as orderid, "CANCEL_REORDER" as action, new_price as new_price))
          }
        }
        
        return cancel_reorder_actions
      }
      
      // 创建订单跟踪引擎
      createReactiveStateEngine(name="orderTrackingEngine",
        metrics=<[order_tracking_logic(tracking_config, orders, tickStream, positions_orders)]>,
        dummyTable=tickStream, outputTable=cancel_reorder_stream, keyColumn="symbol")
    """)
```

### 1.3 撤单重发执行线程
```python
def thread_cancel_reorder(self):
    """撤单重发执行线程"""
    def cancel_reorder_handler(data):
        for row in data:
            order_id = row['orderid']
            action = row['action'] 
            new_price = row['new_price']
            
            if action == "CANCEL_REORDER":
                # 1. 先执行撤单
                cancel_success = self.cancel_order_by_id(order_id)
                
                if cancel_success:
                    # 2. 等待撤单确认
                    time.sleep(0.1)
                    
                    # 3. 检查撤单状态
                    order_status = self.check_order_status(order_id)
                    
                    if order_status == "CANCELLED":
                        # 4. 发送新订单
                        original_order = self.get_order_info(order_id)
                        self.send_new_order(
                            symbol=original_order['symbol'],
                            direction=original_order['direction'],
                            price=new_price,
                            volume=original_order['remaining_volume']
                        )
    
    # 订阅撤单重发流
    self.session.subscribe("localhost", 8848, cancel_reorder_handler, 
                          "cancel_reorder_stream", offset=-1, throttle=0.1)
```

## 2. 时延处理机制 (任务e)

### 2.1 订单状态同步检查
```python
def order_state_sync_check(self):
    """订单状态同步检查 - 处理撤单和下单之间的时延"""
    self.session.run("""
      def sync_check_before_order(positions_orders, new_order_request){
        symbol = new_order_request.symbol[0]
        
        // 检查最近的撤单回报
        recent_cancels = select * from positions_orders 
          where symbol = symbol and status = "CANCELLED" 
          and timestamp > (now() - 5000)  // 最近5秒的撤单
        
        // 检查是否有未处理的撤单
        pending_cancels = select * from positions_orders
          where symbol = symbol and status = "CANCEL_SUBMITTED"
        
        if(size(pending_cancels) > 0){
          // 有待处理的撤单，延迟下单
          return table(symbol as symbol, "DELAY" as action, 1000 as delay_ms)
        } else {
          // 可以正常下单
          return table(symbol as symbol, "PROCEED" as action, 0 as delay_ms)
        }
      }
    """)
```

### 2.2 订单队列管理
```python
class OrderQueueManager:
    """订单队列管理器 - 处理订单时序问题"""
    
    def __init__(self):
        self.pending_orders = queue.Queue()
        self.processing_orders = {}
        self.lock = threading.Lock()
    
    def add_order_request(self, order_request):
        """添加订单请求到队列"""
        with self.lock:
            self.pending_orders.put(order_request)
    
    def process_order_queue(self):
        """处理订单队列"""
        while True:
            try:
                order_request = self.pending_orders.get(timeout=1)
                
                # 检查订单状态同步
                sync_result = self.check_order_sync(order_request)
                
                if sync_result['action'] == 'PROCEED':
                    self.execute_order(order_request)
                elif sync_result['action'] == 'DELAY':
                    # 延迟后重新加入队列
                    time.sleep(sync_result['delay_ms'] / 1000)
                    self.add_order_request(order_request)
                    
            except queue.Empty:
                continue
    
    def execute_order(self, order_request):
        """执行订单"""
        # 实际的订单执行逻辑
        pass
```

## 3. 部分成交处理

### 3.1 部分成交跟踪表
```sql
-- 部分成交跟踪表
share streamTable(1000:0, `orderid`symbol`total_volume`filled_volume`remaining_volume`avg_price`status, 
  [INT, SYMBOL, INT, INT, INT, DOUBLE, SYMBOL]) as partial_fill_tracking
```

### 3.2 部分成交处理逻辑
```python
def partial_fill_handler(self):
    """部分成交处理"""
    self.session.run("""
      def handle_partial_fill(mutable partial_tracking, trade_report){
        order_id = trade_report.orderid[0]
        filled_qty = trade_report.volume[0]
        fill_price = trade_report.price[0]
        
        // 更新部分成交记录
        existing_record = select * from partial_tracking where orderid = order_id
        
        if(size(existing_record) == 0){
          // 新的部分成交记录
          insert into partial_tracking values(
            order_id, trade_report.symbol[0], trade_report.total_volume[0],
            filled_qty, trade_report.total_volume[0] - filled_qty,
            fill_price, "PARTIAL_FILLED"
          )
        } else {
          // 更新现有记录
          new_filled = existing_record.filled_volume[0] + filled_qty
          new_remaining = existing_record.total_volume[0] - new_filled
          new_avg_price = (existing_record.avg_price[0] * existing_record.filled_volume[0] + 
                          fill_price * filled_qty) / new_filled
          
          update partial_tracking set 
            filled_volume = new_filled,
            remaining_volume = new_remaining,
            avg_price = new_avg_price,
            status = iif(new_remaining == 0, "FULLY_FILLED", "PARTIAL_FILLED")
            where orderid = order_id
        }
        
        // 如果还有剩余数量，决定是否继续追单
        if(new_remaining > 0){
          return table(order_id as orderid, "CONTINUE_ORDER" as action, new_remaining as volume)
        } else {
          return table(order_id as orderid, "COMPLETED" as action, 0 as volume)
        }
      }
    """)
```

## 4. 限价单仓位管理

### 4.1 限价单管理策略
```python
def limit_order_management(self):
    """限价单管理 - Limit Order Pegging"""
    self.session.run("""
      limit_order_config = dict(STRING, ANY)
      limit_order_config["max_price_levels"] = 3      // 最多3个价位
      limit_order_config["price_increment"] = 0.01    // 价格递增
      limit_order_config["volume_split"] = [0.4, 0.3, 0.3]  // 数量分配比例
      
      def limit_order_strategy(mutable config, symbol, direction, total_volume, reference_price){
        orders = table(0:0, `symbol`direction`price`volume`order_type, 
                      [SYMBOL, STRING, DOUBLE, INT, STRING])
        
        max_levels = config["max_price_levels"]
        increment = config["price_increment"]
        volume_split = config["volume_split"]
        
        for(i in 0..(max_levels-1)){
          if(direction == "BUY"){
            order_price = reference_price - (i * increment)
          } else {
            order_price = reference_price + (i * increment)
          }
          
          order_volume = int(total_volume * volume_split[i])
          
          orders.append!(table(symbol as symbol, direction as direction, 
                              order_price as price, order_volume as volume, 
                              "LIMIT" as order_type))
        }
        
        return orders
      }
    """)
```

## 5. 实施计划

### 第1-2周：订单跟踪系统
- 重构cancel_order为order_tracking函数
- 实现价格偏离和时间阈值监控
- 开发撤单重发执行线程

### 第3周：时延处理机制  
- 实现订单状态同步检查
- 开发订单队列管理器
- 测试并发订单处理

### 第4周：部分成交和限价单管理
- 实现部分成交跟踪和处理
- 开发限价单分层下单策略
- 集成测试和性能优化

## 6. 风险控制

### 6.1 订单风控检查
```python
def order_risk_check(self):
    """订单风控检查"""
    checks = [
        "position_limit_check",    # 持仓限制检查
        "daily_loss_limit_check",  # 日内亏损限制
        "order_frequency_check",   # 下单频率检查
        "price_deviation_check"    # 价格偏离检查
    ]
    return all(checks)
```

### 6.2 异常处理机制
- 网络断线重连
- 订单状态不一致处理
- 系统异常恢复
- 数据完整性检查
