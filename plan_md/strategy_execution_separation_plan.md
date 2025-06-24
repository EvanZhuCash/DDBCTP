# 策略执行分离改造方案

## 1. 函数重命名和重构

### 1.1 bband_calc → factor_calc
```python
def factor_calc(self):
    """通用因子计算函数，支持多种策略"""
    self.session.run("""
      // 创建多时间间隔聚合引擎
      engine1min = createTimeSeriesEngine(name="engine1min", 
        windowSize=60000, step=60000, metrics=<[last(price)]>,
        dummyTable=tickStream, outputTable=agg1min, 
        timeColumn="timestamp", useSystemTime=false, keyColumn="symbol")
      
      engine5min = createTimeSeriesEngine(name="engine5min", 
        windowSize=300000, step=300000, metrics=<[last(price)]>,
        dummyTable=tickStream, outputTable=agg5min, 
        timeColumn="timestamp", useSystemTime=false, keyColumn="symbol")
      
      // 布林带策略因子计算
      use ta
      bollinger_metrics = <[
        timestamp,
        price,
        ta::bBands(price, timePeriod=20, nbdevUp=2, nbdevDn=2, maType=0)[0] as `bb_upper,
        ta::bBands(price, timePeriod=20, nbdevUp=2, nbdevDn=2, maType=0)[1] as `bb_middle,
        ta::bBands(price, timePeriod=20, nbdevUp=2, nbdevDn=2, maType=0)[2] as `bb_lower,
        // 添加开平仓条件列
        iif(price < ta::bBands(price, timePeriod=20, nbdevUp=2, nbdevDn=2, maType=0)[2], 1, 0) as `long_signal,
        iif(price > ta::bBands(price, timePeriod=20, nbdevUp=2, nbdevDn=2, maType=0)[0], 1, 0) as `short_signal,
        iif(abs(price - ta::bBands(price, timePeriod=20, nbdevUp=2, nbdevDn=2, maType=0)[1]) < 0.01, 1, 0) as `close_signal
      ]>
      
      createReactiveStateEngine(name="factorEngine",
        metrics=bollinger_metrics, dummyTable=agg1min, 
        outputTable=factor_stream, keyColumn="symbol", keepOrder=true)
    """)
```

### 1.2 bollST → factor_stream 表结构重设计
```sql
-- 原始bollST表
share streamTable(1000:0, `symbol`timestamp`close`upper`mean`lower, 
  [SYMBOL, TIMESTAMP, DOUBLE, DOUBLE, DOUBLE, DOUBLE]) as bollST

-- 新的factor_stream表 (通用因子流表)
share streamTable(1000:0, `symbol`timestamp`price`factor_type`factor_value`signal_type`signal_value, 
  [SYMBOL, TIMESTAMP, DOUBLE, SYMBOL, DOUBLE, SYMBOL, INT]) as factor_stream

-- 布林带专用表
share streamTable(1000:0, `symbol`timestamp`price`bb_upper`bb_middle`bb_lower`long_signal`short_signal`close_signal, 
  [SYMBOL, TIMESTAMP, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT]) as bollinger_stream
```

## 2. 策略执行分离

### 2.1 策略端 (Strategy Layer)
```python
def strategy_factor_calc(self, strategy_name="bollinger"):
    """策略因子计算 - 只负责计算技术指标和信号"""
    if strategy_name == "bollinger":
        self.session.run("""
          def bollinger_strategy(symbol, timestamp, price, bb_upper, bb_middle, bb_lower, prev_price){
            // 只计算信号，不执行交易
            long_condition = (price < bb_lower) && (prev_price >= bb_lower)
            short_condition = (price > bb_upper) && (prev_price <= bb_upper)
            close_long_condition = price >= bb_middle
            close_short_condition = price <= bb_middle
            
            signals = dict(STRING, INT)
            signals["OPEN_LONG"] = iif(long_condition, 1, 0)
            signals["OPEN_SHORT"] = iif(short_condition, 1, 0)
            signals["CLOSE_LONG"] = iif(close_long_condition, 1, 0)
            signals["CLOSE_SHORT"] = iif(close_short_condition, 1, 0)
            
            return signals
          }
        """)
```

### 2.2 执行端 (Execution Layer)
```python
def strategy_run(self):
    """执行端 - 只负责根据信号执行交易"""
    self.session.run("""
      posDict = dict(SYMBOL, STRING)  // 持仓状态字典
      
      def execution_engine(mutable posDict, positionTable, symbol, signals){
        current_pos = posDict.get(symbol[0], "FLAT")
        
        // 根据当前持仓和信号决定是否执行交易
        if (current_pos == "FLAT"){
          if (signals["OPEN_LONG"] == 1){
            insert into orderStream values(symbol[0], now(), "BUY", "OPEN")
            posDict[symbol[0]] = "LONG"
          } else if (signals["OPEN_SHORT"] == 1){
            insert into orderStream values(symbol[0], now(), "SELL", "OPEN")
            posDict[symbol[0]] = "SHORT"
          }
        } else if (current_pos == "LONG"){
          if (signals["CLOSE_LONG"] == 1){
            insert into orderStream values(symbol[0], now(), "SELL", "CLOSE")
            posDict[symbol[0]] = "FLAT"
          }
        } else if (current_pos == "SHORT"){
          if (signals["CLOSE_SHORT"] == 1){
            insert into orderStream values(symbol[0], now(), "BUY", "CLOSE")
            posDict[symbol[0]] = "FLAT"
          }
        }
      }
      
      createReactiveStateEngine(name="executionEngine",
        metrics=<[execution_engine(posDict, positionTable, symbol, signals)]>,
        dummyTable=factor_stream, outputTable=orderStream, keyColumn="symbol")
    """)
```

## 3. 横截面策略支持 (任务b)

### 3.1 横截面计算表
```sql
-- 横截面计算结果表
share streamTable(1000:0, `timestamp`symbol`rank_score`selected, 
  [TIMESTAMP, SYMBOL, DOUBLE, INT]) as cross_section_table

-- 横截面策略配置表
share streamTable(100:0, `strategy_name`top_n`bottom_n`rebalance_freq, 
  [SYMBOL, INT, INT, INT]) as cross_section_config
```

### 3.2 横截面策略实现
```python
def cross_section_strategy(self):
    """横截面策略 - 支持配对交易和排序选股"""
    self.session.run("""
      def cross_section_calc(timestamp, symbols, factor_values, config){
        // 对所有品种按因子值排序
        ranked_data = select symbol, factor_value, 
          rank(factor_value, false) as rank_score 
          from table(symbols, factor_values) 
          order by factor_value desc
        
        top_n = config["top_n"]
        bottom_n = config["bottom_n"]
        
        // 选择前N和后N的品种
        selected_long = select symbol, 1 as position_type 
          from ranked_data where rank_score <= top_n
        selected_short = select symbol, -1 as position_type 
          from ranked_data where rank_score > (size(ranked_data) - bottom_n)
        
        return unionAll(selected_long, selected_short)
      }
      
      // 定时触发横截面计算
      timer_cross_section = createTimeSeriesEngine(name="cross_section_timer",
        windowSize=300000, step=300000,  // 5分钟重新计算一次
        metrics=<[cross_section_calc(timestamp, symbol, factor_value, config)]>,
        dummyTable=factor_stream, outputTable=cross_section_table)
    """)
```

## 4. 多时间间隔支持 (任务c)

### 4.1 多时间间隔聚合引擎
```python
def create_multi_timeframe_engines(self):
    """创建多个时间间隔的聚合引擎"""
    self.session.run("""
      // 10秒聚合
      engine10s = createTimeSeriesEngine(name="engine10s", 
        windowSize=10000, step=10000, metrics=<[last(price), max(price), min(price), avg(price)]>,
        dummyTable=tickStream, outputTable=agg10s, 
        timeColumn="timestamp", keyColumn="symbol")
      
      // 1分钟聚合  
      engine1min = createTimeSeriesEngine(name="engine1min", 
        windowSize=60000, step=60000, metrics=<[last(price), max(price), min(price), avg(price)]>,
        dummyTable=tickStream, outputTable=agg1min, 
        timeColumn="timestamp", keyColumn="symbol")
      
      // 5分钟聚合
      engine5min = createTimeSeriesEngine(name="engine5min", 
        windowSize=300000, step=300000, metrics=<[last(price), max(price), min(price), avg(price)]>,
        dummyTable=tickStream, outputTable=agg5min, 
        timeColumn="timestamp", keyColumn="symbol")
      
      // 15分钟聚合
      engine15min = createTimeSeriesEngine(name="engine15min", 
        windowSize=900000, step=900000, metrics=<[last(price), max(price), min(price), avg(price)]>,
        dummyTable=tickStream, outputTable=agg15min, 
        timeColumn="timestamp", keyColumn="symbol")
    """)
```

## 5. 新策略生成步骤

### 5.1 创建新策略的标准流程
1. **定义策略参数表**
2. **实现策略因子计算函数**  
3. **配置因子流表订阅**
4. **实现执行逻辑**
5. **配置策略开关控制**

### 5.2 示例：创建均线策略
```python
def create_ma_strategy(self):
    """创建移动平均策略示例"""
    # 1. 创建策略专用表
    self.session.run("""
      share streamTable(1000:0, `symbol`timestamp`price`ma5`ma20`ma_signal, 
        [SYMBOL, TIMESTAMP, DOUBLE, DOUBLE, DOUBLE, INT]) as ma_stream
    """)
    
    # 2. 实现因子计算
    self.session.run("""
      ma_metrics = <[
        timestamp, price,
        mavg(price, 5) as ma5,
        mavg(price, 20) as ma20,
        iif(mavg(price, 5) > mavg(price, 20), 1, -1) as ma_signal
      ]>
      
      createReactiveStateEngine(name="maStrategy",
        metrics=ma_metrics, dummyTable=agg1min, 
        outputTable=ma_stream, keyColumn="symbol")
    """)
    
    # 3. 订阅和执行
    self.session.run("""
      subscribeTable(tableName=`agg1min, actionName="maStrategy", 
        handler=getStreamEngine("maStrategy"), msgAsTable=true)
    """)
```

## 6. 实施时间表

- **第1周**: 重构现有bband_calc和strategy_run函数
- **第2周**: 实现factor_stream通用表结构和多时间间隔引擎
- **第3周**: 开发横截面策略框架和新策略生成模板
