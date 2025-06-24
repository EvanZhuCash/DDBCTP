# DolphinDB CTP 性能测试模块

本模块实现了DolphinDB CTP交易系统的单节点性能测试，包括延迟测试、行情录制优化和故障排查系统。

## 功能特性

### 1. 延迟测试框架 (`latency_test_framework.py`)
- **端到端延迟监控**: tick接收→DDB计算→信号生成→CTP提交→委托回报
- **分段延迟分析**: 精确测量每个处理阶段的延迟
- **多场景测试**: 支持1/10/100品种，1/3/5策略的组合测试
- **统计分析**: 提供平均值、P50、P95、P99延迟统计

### 2. 行情录制优化 (`market_data_recorder.py`)
- **全市场数据录制**: 支持tick、分钟、日线数据录制
- **内存优化**: 自动内存监控和清理机制
- **批量写入**: 高效的批量数据写入优化
- **压缩存储**: 支持数据压缩和分区存储

### 3. 故障排查系统 (`system_health_monitor.py`)
- **系统健康检查**: CPU、内存、磁盘使用率监控
- **DolphinDB状态监控**: 连接状态、流表、引擎状态检查
- **自动警报**: 支持邮件、Webhook、控制台通知
- **自动恢复**: 内存清理、连接重建、引擎重启

### 4. 综合测试运行器 (`performance_test_runner.py`)
- **一键测试**: 集成所有测试模块的统一入口
- **配置化测试**: 支持JSON配置文件自定义测试参数
- **压力测试**: 逐步增加负载找到系统极限
- **综合报告**: 生成详细的性能分析报告和优化建议

## 安装和使用

### 1. 安装依赖
```bash
pip install -r requirements.txt
```

### 2. 确保DolphinDB服务运行
```bash
# 确保DolphinDB服务在localhost:8848运行
# 用户名: admin, 密码: 123456
```

### 3. 运行测试

#### 运行完整性能测试
```bash
python performance_test_runner.py
```

#### 只运行延迟测试
```bash
python performance_test_runner.py --latency-only
```

#### 只运行录制测试
```bash
python performance_test_runner.py --recording-only
```

#### 只运行压力测试
```bash
python performance_test_runner.py --stress-only
```

#### 使用自定义配置
```bash
python performance_test_runner.py --config test_config.json
```

### 4. 单独运行模块

#### 延迟测试
```python
from latency_test_framework import LatencyTestFramework

framework = LatencyTestFramework()
results = framework.run_latency_test(
    symbol_counts=[1, 10, 50],
    strategy_counts=[1, 3],
    duration=30,
    tick_rate=50
)
framework.generate_latency_report("latency_results.json")
```

#### 行情录制测试
```python
from market_data_recorder import MarketDataRecorder

recorder = MarketDataRecorder()
symbols = [f"TEST{i:03d}" for i in range(100)]

recorder.start_recording(symbols)
recorder.inject_test_data(symbol_count=100, duration_minutes=10, tick_rate=20)
recorder.stop_recording()
recorder.generate_recording_report("recording_results.json")
```

#### 系统健康监控
```python
from system_health_monitor import SystemHealthMonitor

monitor = SystemHealthMonitor()
monitor.start_monitoring(interval=10.0)

# 运行一段时间后
monitor.generate_health_report("health_results.json")
monitor.stop_monitoring()
```

## 配置文件格式

创建 `test_config.json` 文件来自定义测试参数：

```json
{
  "run_latency_test": true,
  "run_recording_test": true,
  "run_stress_test": true,
  "latency_config": {
    "symbol_counts": [1, 10, 50, 100],
    "strategy_counts": [1, 3, 5],
    "duration": 60,
    "tick_rate": 100
  },
  "recording_config": {
    "symbol_count": 200,
    "duration_minutes": 15,
    "tick_rate": 50
  },
  "stress_config": {
    "max_symbols": 500,
    "max_strategies": 20,
    "test_duration": 600
  }
}
```

## 输出结果

测试完成后，会在 `results/` 目录下生成以下文件：

- `comprehensive_performance_report.json`: 综合性能报告
- `latency_test_report.json`: 延迟测试详细报告
- `recording_report.json`: 录制测试报告
- `health_report.json`: 系统健康报告

## 性能指标说明

### 延迟指标
- **平均延迟**: 所有测试样本的平均延迟时间
- **P50延迟**: 50%的请求延迟时间低于此值
- **P95延迟**: 95%的请求延迟时间低于此值
- **P99延迟**: 99%的请求延迟时间低于此值
- **最大延迟**: 测试期间的最大延迟时间
- **吞吐量**: 每秒处理的tick数量

### 系统资源指标
- **CPU使用率**: 系统CPU使用百分比
- **内存使用率**: 系统内存使用百分比
- **磁盘使用率**: 磁盘空间使用百分比
- **网络延迟**: DolphinDB连接延迟

### 录制性能指标
- **录制速率**: 每秒录制的tick数量
- **数据大小**: 录制数据的总大小
- **内存效率**: 内存使用优化效果

## 故障排查

### 常见问题

1. **DolphinDB连接失败**
   - 检查DolphinDB服务是否运行
   - 确认连接参数（host、port、用户名、密码）

2. **内存使用率过高**
   - 减少测试品种数量
   - 缩短测试持续时间
   - 检查系统其他进程的内存使用

3. **延迟过高**
   - 检查系统负载
   - 优化DolphinDB配置
   - 考虑硬件升级

4. **录制失败**
   - 检查磁盘空间
   - 确认数据库权限
   - 检查分区表配置

### 性能优化建议

1. **延迟优化**
   - 使用SSD存储
   - 增加内存容量
   - 优化网络配置
   - 调整DolphinDB参数

2. **吞吐量优化**
   - 使用批量处理
   - 优化流表大小
   - 调整引擎参数
   - 考虑多节点部署

3. **内存优化**
   - 定期清理历史数据
   - 使用数据压缩
   - 优化缓存策略
   - 监控内存泄漏

## 扩展开发

### 添加新的测试场景
1. 继承相应的测试类
2. 实现自定义测试逻辑
3. 在配置文件中添加新参数
4. 更新测试运行器

### 添加新的监控指标
1. 在 `SystemHealthMonitor` 中添加检查函数
2. 定义相应的警报规则
3. 实现自动恢复逻辑

### 添加新的通知渠道
1. 在 `AlertManager` 中实现通知方法
2. 添加配置参数
3. 测试通知功能

## 注意事项

1. **测试环境**: 建议在专用测试环境中运行，避免影响生产系统
2. **资源监控**: 测试过程中密切监控系统资源使用情况
3. **数据清理**: 测试完成后及时清理测试数据
4. **结果分析**: 结合业务场景分析测试结果，制定优化策略

## 技术支持

如有问题或建议，请联系开发团队或查看相关文档。
