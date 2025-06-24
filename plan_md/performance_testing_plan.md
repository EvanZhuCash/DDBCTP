# 单节点性能测试方案

## 1. 延迟测试框架

### 1.1 测试架构设计
```python
class LatencyTestFramework:
    """延迟测试框架"""
    
    def __init__(self):
        self.session = ddb.session()
        self.session.connect("localhost", 8848, "admin", "123456")
        self.test_results = []
        
    def setup_latency_monitoring(self):
        """设置延迟监控"""
        self.session.run("""
          // 延迟监控表
          share streamTable(1000:0, `test_id`stage`timestamp_in`timestamp_out`latency_us`symbol`volume, 
            [INT, SYMBOL, TIMESTAMP, TIMESTAMP, LONG, SYMBOL, INT]) as latency_monitor
          
          // 吞吐量监控表  
          share streamTable(1000:0, `timestamp`tps`symbols_count`strategies_count`memory_usage, 
            [TIMESTAMP, DOUBLE, INT, INT, DOUBLE]) as throughput_monitor
        """)
```

### 1.2 端到端延迟测试
```python
def test_end_to_end_latency(self, symbol_counts=[1, 10, 100], strategy_counts=[1, 5, 10]):
    """端到端延迟测试"""
    
    for symbol_count in symbol_counts:
        for strategy_count in strategy_counts:
            print(f"Testing {symbol_count} symbols, {strategy_count} strategies")
            
            # 1. 设置测试环境
            test_symbols = [f"TEST{i:03d}" for i in range(symbol_count)]
            self.setup_test_strategies(strategy_count)
            
            # 2. 延迟测量点
            latency_stages = [
                "tick_receive",      # 接收tick数据
                "ddb_calculation",   # DDB完成计算
                "signal_generation", # 生成交易信号
                "order_submission",  # CTP提交申报
                "order_response"     # 收到委托回报
            ]
            
            # 3. 执行测试
            test_results = self.run_latency_test(test_symbols, latency_stages)
            
            # 4. 分析结果
            self.analyze_latency_results(test_results, symbol_count, strategy_count)

def setup_latency_measurement(self):
    """设置延迟测量点"""
    self.session.run("""
      // 在关键节点插入时间戳
      def measure_latency(stage_name, test_id, symbol, volume){
        current_time = now()
        insert into latency_monitor values(test_id, stage_name, current_time, null, null, symbol, volume)
        return current_time
      }
      
      // 计算阶段延迟
      def calculate_stage_latency(test_id, stage_name, end_time){
        start_record = select * from latency_monitor 
          where test_id = test_id and stage = stage_name and timestamp_out is null
          order by timestamp_in desc limit 1
        
        if(size(start_record) > 0){
          start_time = start_record.timestamp_in[0]
          latency_us = (end_time - start_time) / 1000  // 转换为微秒
          
          update latency_monitor set 
            timestamp_out = end_time,
            latency_us = latency_us
            where test_id = test_id and stage = stage_name and timestamp_out is null
          
          return latency_us
        }
        return null
      }
    """)
```

### 1.3 分段延迟分析
```python
def analyze_segment_latency(self):
    """分段延迟分析"""
    self.session.run("""
      def analyze_latency_breakdown(){
        // 按阶段统计延迟
        stage_stats = select stage, 
          avg(latency_us) as avg_latency,
          percentile(latency_us, 50) as p50_latency,
          percentile(latency_us, 95) as p95_latency,
          percentile(latency_us, 99) as p99_latency,
          max(latency_us) as max_latency,
          count(*) as sample_count
          from latency_monitor 
          where latency_us is not null
          group by stage
        
        return stage_stats
      }
      
      // 时间序列延迟分析
      def latency_time_series(){
        return select timestamp_in, stage, latency_us, symbol
          from latency_monitor 
          where latency_us is not null
          order by timestamp_in
      }
    """)
```

## 2. 吞吐量测试

### 2.1 多品种压力测试
```python
def throughput_stress_test(self):
    """吞吐量压力测试"""
    
    test_scenarios = [
        {"symbols": 1, "tick_rate": 1000, "duration": 300},    # 1品种，1000tick/s，5分钟
        {"symbols": 10, "tick_rate": 500, "duration": 300},    # 10品种，500tick/s，5分钟  
        {"symbols": 100, "tick_rate": 100, "duration": 300},   # 100品种，100tick/s，5分钟
        {"symbols": 500, "tick_rate": 50, "duration": 300},    # 500品种，50tick/s，5分钟
    ]
    
    for scenario in test_scenarios:
        print(f"Testing throughput: {scenario}")
        
        # 1. 生成测试数据
        test_data = self.generate_tick_data(
            symbol_count=scenario["symbols"],
            tick_rate=scenario["tick_rate"],
            duration=scenario["duration"]
        )
        
        # 2. 监控系统资源
        self.start_resource_monitoring()
        
        # 3. 执行压力测试
        start_time = time.time()
        self.inject_tick_data(test_data)
        end_time = time.time()
        
        # 4. 收集结果
        results = self.collect_throughput_results(start_time, end_time)
        self.analyze_throughput_results(results, scenario)

def monitor_system_resources(self):
    """系统资源监控"""
    self.session.run("""
      def collect_system_metrics(){
        // DolphinDB内存使用
        memory_info = getMemoryUsage()
        
        // 流表大小
        stream_table_sizes = dict(STRING, LONG)
        stream_table_sizes["tickStream"] = size(tickStream)
        stream_table_sizes["factor_stream"] = size(factor_stream)
        stream_table_sizes["orderStream"] = size(orderStream)
        
        // 引擎状态
        engine_stats = getStreamEngineStatus()
        
        return dict(["memory", "table_sizes", "engines"], 
                   [memory_info, stream_table_sizes, engine_stats])
      }
    """)
```

### 2.2 多策略并发测试
```python
def multi_strategy_concurrency_test(self):
    """多策略并发测试"""
    
    strategy_configs = [
        {"name": "bollinger", "symbols": ["IC2509", "IF2509"], "params": {"period": 20}},
        {"name": "ma_cross", "symbols": ["IH2509", "T2509"], "params": {"fast": 5, "slow": 20}},
        {"name": "momentum", "symbols": ["TF2509", "TS2509"], "params": {"lookback": 10}},
        {"name": "mean_revert", "symbols": ["AG2509", "AU2509"], "params": {"threshold": 2.0}},
        {"name": "arbitrage", "symbols": ["IC2509", "IF2509"], "params": {"spread_threshold": 0.5}}
    ]
    
    # 1. 部署多个策略
    for config in strategy_configs:
        self.deploy_strategy(config)
    
    # 2. 并发执行测试
    self.session.run("""
      // 并发策略执行监控
      def monitor_strategy_performance(){
        strategy_metrics = table(0:0, `strategy_name`processed_ticks`generated_signals`execution_time`memory_usage, 
                                [SYMBOL, LONG, LONG, DOUBLE, DOUBLE])
        
        for(strategy in strategy_list){
          metrics = getStrategyMetrics(strategy)
          strategy_metrics.append!(table(
            strategy as strategy_name,
            metrics["processed_ticks"] as processed_ticks,
            metrics["generated_signals"] as generated_signals, 
            metrics["execution_time"] as execution_time,
            metrics["memory_usage"] as memory_usage
          ))
        }
        
        return strategy_metrics
      }
    """)
```

## 3. 行情录制优化

### 3.1 全市场行情录制
```python
def market_data_recording_test(self):
    """全市场行情录制测试"""
    
    # 1. 设置录制配置
    recording_config = {
        "tick_data": True,
        "minute_data": True, 
        "daily_data": True,
        "compression": True,
        "partition_scheme": "date",
        "storage_path": "/data/market_data"
    }
    
    self.session.run(f"""
      // 创建分区表用于行情存储
      if(existsDatabase("dfs://market_data")){{
        dropDatabase("dfs://market_data")
      }}
      
      db = database("dfs://market_data", RANGE, 2024.01.01..2025.01.01)
      
      // Tick数据表
      tick_schema = table(1:0, `symbol`timestamp`price`volume`bid1`ask1`bid_vol1`ask_vol1, 
                         [SYMBOL, TIMESTAMP, DOUBLE, LONG, DOUBLE, DOUBLE, LONG, LONG])
      tick_table = createPartitionedTable(db, tick_schema, "tick_data", "timestamp")
      
      // 分钟数据表
      minute_schema = table(1:0, `symbol`timestamp`open`high`low`close`volume`amount, 
                           [SYMBOL, TIMESTAMP, DOUBLE, DOUBLE, DOUBLE, DOUBLE, LONG, DOUBLE])
      minute_table = createPartitionedTable(db, minute_schema, "minute_data", "timestamp")
    """)
    
    # 2. 内存使用优化
    self.optimize_memory_usage()
    
    # 3. 录制性能测试
    self.test_recording_performance()

def optimize_memory_usage(self):
    """内存使用优化"""
    self.session.run("""
      // 设置流表缓存策略
      setStreamTableCacheSize(100000)  // 设置流表缓存大小
      
      // 批量写入优化
      def batch_insert_optimization(){
        // 使用BatchTableWriter提高写入性能
        writer = batchTableWriter("dfs://market_data", "tick_data", false)
        
        // 设置批量大小
        writer.setBatchSize(10000)
        
        return writer
      }
      
      // 内存清理策略
      def memory_cleanup_strategy(){
        // 定期清理过期数据
        cutoff_time = now() - 24*60*60*1000  // 24小时前
        
        // 清理流表中的历史数据
        delete from tickStream where timestamp < cutoff_time
        delete from factor_stream where timestamp < cutoff_time
        
        // 强制垃圾回收
        gc()
      }
    """)
```

### 3.2 存储性能优化
```python
def storage_performance_optimization(self):
    """存储性能优化"""
    
    self.session.run("""
      // 压缩配置
      def setup_compression(){
        // 对历史数据启用压缩
        compressDir("/data/market_data/tick_data", "lz4")
        compressDir("/data/market_data/minute_data", "lz4")
      }
      
      // 分区优化
      def optimize_partitioning(){
        // 按日期和品种双重分区
        db_date = database("", RANGE, 2024.01.01..2025.01.01)
        db_symbol = database("", HASH, [SYMBOL, 100])
        db_composite = database("dfs://market_data_optimized", COMPO, [db_date, db_symbol])
        
        return db_composite
      }
      
      // 索引优化
      def create_indexes(){
        // 为常用查询字段创建索引
        createIndex("dfs://market_data", "tick_data", "symbol")
        createIndex("dfs://market_data", "tick_data", "timestamp")
      }
    """)
```

## 4. 故障排查系统

### 4.1 错误监控和诊断
```python
def error_monitoring_system(self):
    """错误监控和诊断系统"""
    
    self.session.run("""
      // 错误日志表
      share streamTable(1000:0, `timestamp`error_type`error_code`error_message`stack_trace`affected_symbol, 
        [TIMESTAMP, SYMBOL, INT, STRING, STRING, SYMBOL]) as error_log
      
      // 系统健康检查
      def system_health_check(){
        health_status = dict(STRING, ANY)
        
        // 检查DolphinDB连接
        health_status["ddb_connection"] = isConnected()
        
        // 检查流表状态
        health_status["stream_tables"] = check_stream_tables()
        
        // 检查引擎状态
        health_status["engines"] = check_engines_status()
        
        // 检查内存使用
        memory_usage = getMemoryUsage()
        health_status["memory_critical"] = memory_usage["used"] / memory_usage["total"] > 0.9
        
        // 检查磁盘空间
        disk_usage = getDiskUsage()
        health_status["disk_critical"] = disk_usage["used"] / disk_usage["total"] > 0.9
        
        return health_status
      }
      
      def check_stream_tables(){
        table_status = dict(STRING, BOOL)
        
        try{
          table_status["tickStream"] = isDefined("tickStream")
          table_status["factor_stream"] = isDefined("factor_stream") 
          table_status["orderStream"] = isDefined("orderStream")
        } catch(ex){
          table_status["error"] = ex.message
        }
        
        return table_status
      }
    """)
```

### 4.2 自动故障恢复
```python
def auto_recovery_system(self):
    """自动故障恢复系统"""
    
    recovery_procedures = {
        "connection_lost": self.recover_connection,
        "memory_overflow": self.recover_memory,
        "engine_stopped": self.recover_engines,
        "data_corruption": self.recover_data
    }
    
    def recover_connection(self):
        """连接恢复"""
        max_retries = 5
        retry_interval = 10  # 秒
        
        for attempt in range(max_retries):
            try:
                self.session.connect("localhost", 8848, "admin", "123456")
                print(f"Connection recovered on attempt {attempt + 1}")
                return True
            except Exception as e:
                print(f"Connection attempt {attempt + 1} failed: {e}")
                time.sleep(retry_interval)
        
        return False
    
    def recover_memory(self):
        """内存恢复"""
        self.session.run("""
          // 清理临时数据
          clearAllCache()
          
          // 重置流表大小
          truncate tickStream
          truncate factor_stream where timestamp < (now() - 60*60*1000)
          
          // 强制垃圾回收
          gc()
        """)
    
    def recover_engines(self):
        """引擎恢复"""
        self.session.run("""
          // 重启停止的引擎
          stopped_engines = getStoppedEngines()
          
          for(engine in stopped_engines){
            try{
              restartStreamEngine(engine)
            } catch(ex){
              print("Failed to restart engine: " + engine + ", error: " + ex.message)
            }
          }
        """)
```

## 5. 性能基准测试

### 5.1 基准测试套件
```python
def benchmark_test_suite(self):
    """性能基准测试套件"""
    
    benchmarks = [
        {
            "name": "single_symbol_low_freq",
            "symbols": 1,
            "tick_rate": 10,
            "strategies": 1,
            "duration": 300,
            "expected_latency_p99": 1000  # 微秒
        },
        {
            "name": "multi_symbol_medium_freq", 
            "symbols": 50,
            "tick_rate": 100,
            "strategies": 3,
            "duration": 300,
            "expected_latency_p99": 5000
        },
        {
            "name": "high_freq_stress_test",
            "symbols": 100,
            "tick_rate": 1000,
            "strategies": 5,
            "duration": 300,
            "expected_latency_p99": 10000
        }
    ]
    
    results = {}
    for benchmark in benchmarks:
        print(f"Running benchmark: {benchmark['name']}")
        result = self.run_benchmark(benchmark)
        results[benchmark['name']] = result
        
        # 验证性能指标
        if result['latency_p99'] > benchmark['expected_latency_p99']:
            print(f"WARNING: Latency exceeded expectation for {benchmark['name']}")
    
    return results
```

## 6. 实施计划

### 第1周：延迟测试框架
- 搭建延迟监控系统
- 实现端到端延迟测量
- 开发分段延迟分析

### 第2周：吞吐量和并发测试
- 多品种压力测试
- 多策略并发测试  
- 系统资源监控

### 第3周：行情录制优化
- 全市场数据录制测试
- 内存和存储优化
- 性能瓶颈分析

### 第4周：故障排查和基准测试
- 错误监控系统
- 自动恢复机制
- 性能基准测试套件

## 7. 预期性能指标

### 7.1 延迟指标
- 单品种P99延迟 < 1ms
- 10品种P99延迟 < 5ms  
- 100品种P99延迟 < 10ms

### 7.2 吞吐量指标
- 单策略支持1000+ tick/s
- 多策略支持500+ tick/s per strategy
- 内存使用 < 8GB (100品种场景)
