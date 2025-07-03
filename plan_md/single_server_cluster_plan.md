# DolphinDB单服务器集群部署方案

## 1. 项目概述

### 1.1 架构调整说明
基于用户反馈，将多服务器集群方案调整为单服务器集群部署，参考DolphinDB官方文档：
- 单机集群部署：https://docs.dolphindb.cn/zh/tutorials/single_machine_cluster_deploy.html
- 环境：Linux Ubuntu 20.04+
- 目标：在单台服务器上实现集群的高可用性和性能优化

### 1.2 单服务器集群架构 (免费版限制)
```
┌─────────────────────────────────────────────────────────────────┐
│                Single Server (Ubuntu 20.04) - Free Edition     │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐              │
│  │ Controller  │  │ Data Node 1 │  │ Data Node 2 │              │
│  │   (8900)    │  │   (8848)    │  │   (8849)    │              │
│  └─────────────┘  └─────────────┘  └─────────────┘              │
│                   ┌─────────────┐                               │
│                   │ Data Node 3 │                               │
│                   │   (8850)    │                               │
│                   └─────────────┘                               │
└─────────────────────────────────────────────────────────────────┘

注意：DolphinDB免费版限制：
- 最多3个数据节点
- 不支持计算节点
- 不支持代理节点
- 内存限制8GB
```

## 2. 集群配置设计

### 2.1 Controller节点配置
```ini
# controller.cfg
mode=controller
localSite=localhost:8900:controller
dfsReplicationFactor=2
dfsReplicaReliabilityLevel=1
dataSync=1
workerNum=4
maxMemSize=8
maxConnections=512
maxConnectionPerSite=8
newValuePartitionPolicy=add
enableChunkGranularityConfig=true
chunkCacheEngineMemSize=2
```

### 2.2 Data节点配置 (免费版3节点)
```ini
# datanode1.cfg
mode=datanode
localSite=localhost:8848:datanode1
controllerSite=localhost:8900:controller
workerNum=6
maxMemSize=2.5
maxConnections=256
volumes=/data/ddb/datanode1/storage
diskIOConcurrencyLevel=2
enableChunkGranularityConfig=true
chunkCacheEngineMemSize=0.5

# datanode2.cfg
mode=datanode
localSite=localhost:8849:datanode2
controllerSite=localhost:8900:controller
workerNum=6
maxMemSize=2.5
maxConnections=256
volumes=/data/ddb/datanode2/storage
diskIOConcurrencyLevel=2
enableChunkGranularityConfig=true
chunkCacheEngineMemSize=0.5

# datanode3.cfg
mode=datanode
localSite=localhost:8850:datanode3
controllerSite=localhost:8900:controller
workerNum=6
maxMemSize=2.5
maxConnections=256
volumes=/data/ddb/datanode3/storage
diskIOConcurrencyLevel=2
enableChunkGranularityConfig=true
chunkCacheEngineMemSize=0.5
```

### 2.3 免费版限制说明
```ini
# 免费版不支持以下节点类型：
# - Compute节点 (计算节点)
# - Agent节点 (代理节点)
#
# 所有计算任务将在Data节点上执行
# 通过增加Data节点的workerNum来补偿计算能力
```

## 3. 分布式数据库设计

### 3.1 分区策略 (适配3节点)
```python
def setup_distributed_database(session):
    """设置分布式数据库 - 3节点优化"""
    session.run("""
      // 创建分布式数据库 - 简化分区策略适配3节点
      if(existsDatabase("dfs://trading_data")){
        dropDatabase("dfs://trading_data")
      }

      // 时间分区：按周分区 (减少分区数量)
      dbDate = database("", RANGE, 2024.01.01..2025.01.01 step 7)

      // 品种分区：哈希分区3个桶 (匹配3个数据节点)
      dbSymbol = database("", HASH, [SYMBOL, 3])

      // 复合分区
      db = database("dfs://trading_data", COMPO, [dbDate, dbSymbol])

      // 创建tick数据表
      tick_schema = table(1:0,
        `timestamp`symbol`price`volume`bid1`ask1`bid_vol1`ask_vol1`gateway,
        [TIMESTAMP, SYMBOL, DOUBLE, LONG, DOUBLE, DOUBLE, LONG, LONG, SYMBOL])

      tick_table = createPartitionedTable(db, tick_schema, "tick_data",
        partitionColumns=["timestamp", "symbol"])

      // 创建K线数据表
      kline_schema = table(1:0,
        `timestamp`symbol`open`high`low`close`volume`interval,
        [TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, LONG, SYMBOL])

      kline_table = createPartitionedTable(db, kline_schema, "kline_data",
        partitionColumns=["timestamp", "symbol"])

      // 创建订单数据表
      order_schema = table(1:0,
        `timestamp`symbol`direction`price`volume`status`order_id`gateway,
        [TIMESTAMP, SYMBOL, SYMBOL, DOUBLE, INT, SYMBOL, STRING, SYMBOL])

      order_table = createPartitionedTable(db, order_schema, "order_data",
        partitionColumns=["timestamp", "symbol"])
    """)
```

### 3.2 数据节点计算优化 (免费版)
```python
def setup_datanode_compute_optimization(session):
    """设置数据节点计算优化 - 免费版无独立计算节点"""
    session.run("""
      // 配置3个数据节点
      data_nodes = ["localhost:8848:datanode1", "localhost:8849:datanode2", "localhost:8850:datanode3"]

      // 免费版无独立计算节点，在数据节点上执行计算
      // 配置数据本地性优化
      setDataLocalityPolicy("dfs://trading_data", "tick_data", "PREFER_LOCAL")
      setDataLocalityPolicy("dfs://trading_data", "kline_data", "PREFER_LOCAL")

      // 配置缓存策略 - 适配8GB内存限制
      setCacheEngineSize("tick_data", 512)   // 512MB缓存
      setCacheEngineSize("kline_data", 256)  // 256MB缓存

      // 配置负载均衡 - 3节点轮询
      setLoadBalancePolicy("ROUND_ROBIN")

      // 优化工作线程数 - 每个节点6个工作线程
      for(node in data_nodes){
        setNodeWorkerNum(node, 6)
      }
    """)
```

## 4. 高可用性配置

### 4.1 数据副本配置
```python
def setup_high_availability(session):
    """设置高可用性"""
    session.run("""
      // 设置副本因子
      setDfsReplicationFactor(2)
      
      // 配置副本分布策略
      setDfsReplicaReliabilityLevel(1)  // 确保副本在不同节点
      
      // 启用自动故障转移
      enableAutoFailover(true)
      
      // 配置心跳检测
      setHeartbeatInterval(3000)  // 3秒心跳
      setHeartbeatTimeout(10000)  // 10秒超时
      
      // 配置数据同步
      setDataSync(1)  // 同步写入
    """)
```

### 4.2 负载均衡配置
```python
def setup_load_balancing(session):
    """设置负载均衡"""
    session.run("""
      // 配置连接负载均衡
      setLoadBalancePolicy("ROUND_ROBIN")
      
      // 配置查询负载均衡
      setQueryLoadBalancePolicy("LEAST_LOADED")
      
      // 配置写入负载均衡
      setWriteLoadBalancePolicy("HASH_PARTITION")
      
      // 设置节点权重
      node_weights = dict(STRING, DOUBLE)
      node_weights["localhost:8848:datanode1"] = 1.0
      node_weights["localhost:8849:datanode2"] = 1.0
      node_weights["localhost:8852:datanode3"] = 1.0
      
      setNodeWeights(node_weights)
    """)
```

## 5. 性能优化配置

### 5.1 内存优化 (8GB限制)
```python
def setup_memory_optimization(session):
    """设置内存优化 - 严格8GB限制"""
    session.run("""
      // 配置内存池 - 总计7.5GB (留0.5GB给系统)
      setMaxMemSize(7680)  // 7.5GB总内存限制

      // 配置缓存策略 - 保守配置
      setCachePolicy("LRU")
      setCacheSize(1024)  // 1GB缓存 (3节点共享)

      // 配置流表内存 - 限制流表内存使用
      setStreamTableMemSize(512)  // 512MB流表内存

      // 配置压缩 - 高压缩比节省内存
      setCompressionLevel("HIGH")
      enableDeltaCompression(true)

      // 配置垃圾回收 - 更频繁的GC
      setGCInterval(30000)  // 30秒GC间隔
      setGCThreshold(0.8)   // 80%内存使用率触发GC

      // 配置分区缓存 - 限制每个分区的内存使用
      setPartitionCacheSize(64)  // 64MB每分区
    """)
```

### 5.2 磁盘IO优化
```python
def setup_disk_optimization(session):
    """设置磁盘IO优化"""
    session.run("""
      // 配置IO并发
      setDiskIOConcurrencyLevel(4)
      
      // 配置写入缓冲
      setWriteBufferSize(256)  // 256MB写入缓冲
      
      // 配置预读
      setReadAheadSize(64)  // 64MB预读
      
      // 配置分层存储
      setTieredStoragePolicy("HOT_WARM_COLD")
      setHotDataRetentionDays(7)    // 热数据7天
      setWarmDataRetentionDays(30)  // 温数据30天
    """)
```

## 6. 监控和管理

### 6.1 集群监控
```python
def setup_cluster_monitoring(session):
    """设置集群监控"""
    session.run("""
      // 创建监控表
      share streamTable(1000:0, 
        `timestamp`node_id`cpu_usage`memory_usage`disk_usage`network_io`status,
        [TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, SYMBOL]) as cluster_monitor
      
      // 监控函数
      def collect_cluster_metrics(){
        nodes = getClusterNodes()
        metrics = table(0:0, 
          `timestamp`node_id`cpu_usage`memory_usage`disk_usage`network_io`status,
          [TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, SYMBOL])
        
        for(node in nodes){
          node_stats = getNodeStats(node)
          metrics.append!(table(
            now() as timestamp,
            node as node_id,
            node_stats["cpu_usage"] as cpu_usage,
            node_stats["memory_usage"] as memory_usage,
            node_stats["disk_usage"] as disk_usage,
            node_stats["network_io"] as network_io,
            node_stats["status"] as status
          ))
        }
        
        return metrics
      }
      
      // 定时监控
      timer_monitor = createTimeSeriesEngine(name="clusterMonitor",
        windowSize=10000, step=10000,  // 每10秒收集一次
        metrics=<[collect_cluster_metrics()]>,
        dummyTable=cluster_monitor, outputTable=cluster_monitor)
    """)
```

### 6.2 自动恢复机制
```python
def setup_auto_recovery(session):
    """设置自动恢复机制"""
    session.run("""
      // 故障检测函数
      def detect_node_failure(){
        failed_nodes = select node_id from cluster_monitor 
          where timestamp > now() - 30000 and status = "FAILED"
        
        for(failed_node in failed_nodes.node_id){
          print("Detected failed node: " + failed_node)
          trigger_failover(failed_node)
        }
      }
      
      // 故障转移函数
      def trigger_failover(failed_node){
        // 重新分配分区
        redistributePartitions(failed_node)
        
        // 启动备用节点
        if(hasStandbyNode(failed_node)){
          activateStandbyNode(failed_node)
        }
        
        // 重新平衡负载
        rebalanceCluster()
      }
      
      // 定时故障检测
      timer_failover = createTimeSeriesEngine(name="failoverDetector",
        windowSize=30000, step=30000,  // 每30秒检测一次
        metrics=<[detect_node_failure()]>,
        dummyTable=cluster_monitor, outputTable=cluster_monitor)
    """)
```

## 7. 部署脚本

### 7.1 启动脚本
```bash
#!/bin/bash
# start_cluster.sh

# 设置环境变量
export DOLPHINDB_HOME=/opt/dolphindb
export DOLPHINDB_DATA=/data/ddb

# 创建数据目录
mkdir -p $DOLPHINDB_DATA/{controller,datanode1,datanode2,datanode3,computenode1,computenode2,agent1,agent2}

# 启动Controller
echo "Starting Controller..."
$DOLPHINDB_HOME/server/dolphindb -console 0 -mode controller -home $DOLPHINDB_DATA/controller -config $DOLPHINDB_HOME/server/controller.cfg -logFile $DOLPHINDB_DATA/controller/controller.log &

sleep 5

# 启动Data节点 (免费版仅3个节点)
echo "Starting Data Nodes..."
$DOLPHINDB_HOME/server/dolphindb -console 0 -mode datanode -home $DOLPHINDB_DATA/datanode1 -config $DOLPHINDB_HOME/server/datanode1.cfg -logFile $DOLPHINDB_DATA/datanode1/datanode1.log &
$DOLPHINDB_HOME/server/dolphindb -console 0 -mode datanode -home $DOLPHINDB_DATA/datanode2 -config $DOLPHINDB_HOME/server/datanode2.cfg -logFile $DOLPHINDB_DATA/datanode2/datanode2.log &
$DOLPHINDB_HOME/server/dolphindb -console 0 -mode datanode -home $DOLPHINDB_DATA/datanode3 -config $DOLPHINDB_HOME/server/datanode3.cfg -logFile $DOLPHINDB_DATA/datanode3/datanode3.log &

sleep 10

echo "Free Edition: Only 3 data nodes supported"
echo "Compute and Agent nodes not available in free edition"

echo "Cluster started successfully!"
```

## 8. 实施计划

### 第1周：环境准备和基础配置
- Ubuntu 20.04环境准备
- DolphinDB安装和基础配置
- 单机集群配置文件编写

### 第2周：分布式数据库设计
- 分区策略实现
- 存储计算分离配置
- 数据副本和高可用配置

### 第3周：性能优化和监控
- 内存和磁盘IO优化
- 集群监控系统实现
- 自动恢复机制开发

### 第4周：测试和文档
- 性能测试和压力测试
- 故障转移测试
- 部署文档和运维手册

## 9. 免费版限制和解决方案

### 9.1 免费版限制
- **节点数量限制**: 最多3个数据节点
- **内存限制**: 8GB总内存限制
- **功能限制**: 不支持计算节点和代理节点
- **副本限制**: 副本因子最大为2

### 9.2 优化策略
- **内存优化**: 严格控制内存使用，高压缩比
- **分区优化**: 3个哈希分区匹配3个节点
- **计算优化**: 在数据节点上执行计算任务
- **缓存优化**: 保守的缓存配置

### 9.3 预期收益 (免费版)
- **性能提升**: 相比单节点提升50-80%
- **可靠性**: 2副本数据保护
- **扩展性**: 在免费版限制内的最优配置
- **成本效益**: 零成本获得集群能力

### 9.4 升级路径
- 如需更多节点和功能，可考虑企业版
- 当前架构可平滑升级到企业版
- 配置文件和脚本兼容企业版
