# 回测系统开发方案

## 1. 回测框架设计 (基于replay函数)

### 1.1 整体架构
```python
class BacktestEngine:
    """回测引擎 - 基于DolphinDB replay函数"""
    
    def __init__(self, start_date, end_date, initial_capital=1000000):
        self.start_date = start_date
        self.end_date = end_date
        self.initial_capital = initial_capital
        self.session = ddb.session()
        self.session.connect("localhost", 8848, "admin", "123456")
        
    def setup_backtest_environment(self):
        """设置回测环境"""
        self.session.run("""
          // 创建回测专用表
          share streamTable(1000:0, `timestamp`symbol`open`high`low`close`volume, 
            [TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, LONG]) as hist_data_stream
          
          share streamTable(1000:0, `timestamp`symbol`signal`price`volume`direction, 
            [TIMESTAMP, SYMBOL, STRING, DOUBLE, INT, STRING]) as backtest_signals
          
          share streamTable(1000:0, `timestamp`symbol`price`volume`direction`pnl`commission, 
            [TIMESTAMP, SYMBOL, DOUBLE, INT, STRING, DOUBLE, DOUBLE]) as backtest_trades
          
          share streamTable(1000:0, `timestamp`total_value`cash`positions_value`daily_return`cumulative_return, 
            [TIMESTAMP, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE]) as backtest_nav
        """)
```

### 1.2 replay函数回测实现
```python
def run_backtest(self, strategy_name, symbols, data_frequency="1min"):
    """运行回测"""
    self.session.run(f"""
      // 加载历史数据
      hist_data = loadTable("dfs://stock_data", "minute_data")
      hist_subset = select * from hist_data 
        where date between {self.start_date} : {self.end_date} 
        and symbol in {symbols}
      
      // 回测状态变量
      backtest_state = dict(STRING, ANY)
      backtest_state["cash"] = {self.initial_capital}
      backtest_state["positions"] = dict(SYMBOL, INT)
      backtest_state["position_prices"] = dict(SYMBOL, DOUBLE)
      backtest_state["total_commission"] = 0.0
      
      // 策略逻辑函数
      def {strategy_name}_backtest_logic(mutable state, timestamp, symbol, open, high, low, close, volume){{
        // 检查市场限制
        market_check = check_market_constraints(symbol, close, volume, timestamp)
        if(!market_check["can_trade"]) return null
        
        // 生成交易信号
        signal = {strategy_name}_signal_generator(timestamp, symbol, open, high, low, close, volume)
        
        if(signal != "NO_TRADE"){{
          // 执行交易
          trade_result = execute_backtest_trade(state, timestamp, symbol, signal, close, volume)
          
          // 更新NAV
          update_nav(state, timestamp)
          
          return trade_result
        }}
        
        return null
      }}
      
      // 使用replay函数进行回测
      replay(inputTables=hist_subset, 
             outputTables=backtest_trades,
             dateColumn="timestamp", 
             timeColumn="timestamp",
             replayRate=1000,  // 1000倍速回测
             handler={strategy_name}_backtest_logic)
    """)
```

## 2. 策略指标计算模块

### 2.1 常用指标计算
```python
def calculate_performance_metrics(self):
    """计算回测性能指标"""
    self.session.run("""
      // Sharpe比率计算
      def calculate_sharpe(returns, risk_free_rate=0.03){
        if(size(returns) < 2) return null
        excess_returns = returns - risk_free_rate/252  // 日化无风险利率
        return avg(excess_returns) / std(excess_returns) * sqrt(252)
      }
      
      // 最大回撤计算
      def calculate_max_drawdown(nav_series){
        if(size(nav_series) < 2) return null
        cummax_nav = cummax(nav_series)
        drawdowns = (nav_series - cummax_nav) / cummax_nav
        return min(drawdowns)
      }
      
      // 年化收益率
      def calculate_annual_return(start_value, end_value, days){
        if(days <= 0 or start_value <= 0) return null
        return pow(end_value / start_value, 365.0 / days) - 1
      }
      
      // 胜率计算
      def calculate_win_rate(pnl_series){
        if(size(pnl_series) == 0) return null
        winning_trades = sum(pnl_series > 0)
        return winning_trades / double(size(pnl_series))
      }
      
      // 盈亏比
      def calculate_profit_loss_ratio(pnl_series){
        winning_pnl = pnl_series[pnl_series > 0]
        losing_pnl = pnl_series[pnl_series < 0]
        
        if(size(winning_pnl) == 0 or size(losing_pnl) == 0) return null
        
        avg_win = avg(winning_pnl)
        avg_loss = abs(avg(losing_pnl))
        
        return avg_win / avg_loss
      }
    """)
```

### 2.2 NAV计算逻辑
```python
def nav_calculation_logic(self):
    """NAV计算逻辑 - 考虑加减仓"""
    self.session.run("""
      def update_nav_with_positions(mutable state, timestamp, current_prices){
        cash = state["cash"]
        positions = state["positions"]
        position_prices = state["position_prices"]
        
        // 计算当前持仓市值
        positions_value = 0.0
        
        for(symbol in positions.keys()){
          if(positions[symbol] != 0){
            current_price = current_prices.get(symbol, position_prices[symbol])
            position_value = positions[symbol] * current_price
            positions_value += position_value
          }
        }
        
        total_value = cash + positions_value
        
        // 计算日收益率
        prev_nav = select last(total_value) from backtest_nav
        daily_return = 0.0
        if(size(prev_nav) > 0 and prev_nav.total_value[0] > 0){
          daily_return = (total_value - prev_nav.total_value[0]) / prev_nav.total_value[0]
        }
        
        // 计算累计收益率
        cumulative_return = (total_value - {self.initial_capital}) / {self.initial_capital}
        
        // 插入NAV记录
        insert into backtest_nav values(
          timestamp, total_value, cash, positions_value, 
          daily_return, cumulative_return
        )
        
        return total_value
      }
    """)
```

## 3. 市场限制处理

### 3.1 涨跌停限制
```python
def market_constraints_handler(self):
    """市场限制处理"""
    self.session.run("""
      def check_market_constraints(symbol, price, volume, timestamp){
        result = dict(STRING, ANY)
        result["can_trade"] = true
        result["reason"] = ""
        
        // 获取涨跌停价格
        limit_info = get_price_limits(symbol, timestamp)
        upper_limit = limit_info["upper_limit"]
        lower_limit = limit_info["lower_limit"]
        
        // 检查涨跌停
        if(price >= upper_limit){
          result["can_trade"] = false
          result["reason"] = "UPPER_LIMIT"
          return result
        }
        
        if(price <= lower_limit){
          result["can_trade"] = false  
          result["reason"] = "LOWER_LIMIT"
          return result
        }
        
        // 检查成交量
        min_volume_threshold = 100  // 最小成交量阈值
        if(volume < min_volume_threshold){
          result["can_trade"] = false
          result["reason"] = "INSUFFICIENT_VOLUME"
          return result
        }
        
        // 检查交易时间
        trading_hours = get_trading_hours(symbol)
        current_time = time(timestamp)
        
        is_trading_time = false
        for(session in trading_hours){
          if(current_time >= session["start"] and current_time <= session["end"]){
            is_trading_time = true
            break
          }
        }
        
        if(!is_trading_time){
          result["can_trade"] = false
          result["reason"] = "NON_TRADING_HOURS"
          return result
        }
        
        return result
      }
      
      // 获取涨跌停价格
      def get_price_limits(symbol, timestamp){
        // 这里需要根据实际的涨跌停规则实现
        // 期货一般是±4%或±5%
        prev_close = get_previous_close(symbol, timestamp)
        
        if(symbol like "IC*" or symbol like "IF*" or symbol like "IH*"){
          // 股指期货 ±10%
          limit_pct = 0.10
        } else if(symbol like "T*" or symbol like "TF*"){
          // 国债期货 ±2%
          limit_pct = 0.02
        } else {
          // 商品期货 ±4%
          limit_pct = 0.04
        }
        
        return dict(["upper_limit", "lower_limit"], 
                   [prev_close * (1 + limit_pct), prev_close * (1 - limit_pct)])
      }
    """)
```

### 3.2 流动性检查
```python
def liquidity_check(self):
    """流动性检查"""
    self.session.run("""
      def check_liquidity(symbol, order_volume, market_volume, timestamp){
        // 检查订单量是否超过市场成交量的一定比例
        max_participation_rate = 0.1  // 最大参与率10%
        
        if(order_volume > market_volume * max_participation_rate){
          // 需要拆分订单
          recommended_volume = int(market_volume * max_participation_rate)
          return dict(["can_trade", "recommended_volume", "need_split"], 
                     [false, recommended_volume, true])
        }
        
        return dict(["can_trade", "recommended_volume", "need_split"], 
                   [true, order_volume, false])
      }
    """)
```

## 4. 交易成本模型

### 4.1 手续费计算
```python
def commission_model(self):
    """手续费模型"""
    self.session.run("""
      def calculate_commission(symbol, volume, price, direction){
        commission_rate = get_commission_rate(symbol)
        
        // 基础手续费
        base_commission = volume * price * commission_rate
        
        // 最小手续费
        min_commission = get_min_commission(symbol)
        
        commission = max(base_commission, min_commission)
        
        // 平今仓手续费（期货特有）
        if(direction == "CLOSE_TODAY"){
          close_today_rate = get_close_today_rate(symbol)
          commission = volume * price * close_today_rate
        }
        
        return commission
      }
      
      def get_commission_rate(symbol){
        // 不同品种的手续费率
        if(symbol like "IC*" or symbol like "IF*" or symbol like "IH*"){
          return 0.000023  // 股指期货万分之2.3
        } else if(symbol like "T*"){
          return 0.00003   // 国债期货万分之3
        } else {
          return 0.0001    // 商品期货万分之1
        }
      }
    """)
```

## 5. 回测报告生成

### 5.1 性能报告
```python
def generate_backtest_report(self, strategy_name):
    """生成回测报告"""
    results = self.session.run("""
      // 计算各项指标
      nav_data = select * from backtest_nav order by timestamp
      trade_data = select * from backtest_trades order by timestamp
      
      if(size(nav_data) == 0) return null
      
      start_value = nav_data.total_value[0]
      end_value = nav_data.total_value[-1]
      days = (nav_data.timestamp[-1] - nav_data.timestamp[0]) / (24*60*60*1000)
      
      // 基础指标
      total_return = (end_value - start_value) / start_value
      annual_return = calculate_annual_return(start_value, end_value, days)
      max_drawdown = calculate_max_drawdown(nav_data.total_value)
      sharpe_ratio = calculate_sharpe(nav_data.daily_return)
      
      // 交易指标
      total_trades = size(trade_data)
      win_rate = calculate_win_rate(trade_data.pnl)
      profit_loss_ratio = calculate_profit_loss_ratio(trade_data.pnl)
      total_commission = sum(trade_data.commission)
      
      // 返回结果字典
      return dict(
        ["total_return", "annual_return", "max_drawdown", "sharpe_ratio",
         "total_trades", "win_rate", "profit_loss_ratio", "total_commission"],
        [total_return, annual_return, max_drawdown, sharpe_ratio,
         total_trades, win_rate, profit_loss_ratio, total_commission]
      )
    """)
    
    return results
```

## 6. 实施计划

### 第1周：回测框架搭建
- 设计回测引擎架构
- 实现replay函数回测逻辑
- 创建回测专用数据表

### 第2周：指标计算模块
- 实现Sharpe、回撤、收益率等指标
- 开发NAV计算逻辑
- 测试指标计算准确性

### 第3周：市场限制处理
- 实现涨跌停检查
- 开发流动性约束处理
- 添加交易时间限制

### 第4周：优化和测试
- 性能优化和并行化
- 回测报告生成
- 全面测试验证

## 7. 技术要点

### 7.1 数据处理优化
- 使用分区表提高查询效率
- 批量处理减少I/O开销
- 内存表缓存常用数据

### 7.2 回测加速
- 向量化计算
- 并行回测多个策略
- 增量计算优化
