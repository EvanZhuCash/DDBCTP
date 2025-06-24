# DolphinDB CTP 性能测试模块 - 项目结构

## 目录结构

```
performance_testing/
├── README.md                           # 项目说明文档
├── PROJECT_STRUCTURE.md               # 项目结构说明（本文件）
├── requirements.txt                    # Python依赖包列表
├── setup.py                           # 安装和设置脚本
├── quick_test.py                       # 快速验证测试
├── usage_examples.py                   # 使用示例
├── test_config_example.json            # 配置文件示例
│
├── latency_test_framework.py           # 延迟测试框架
├── market_data_recorder.py             # 行情录制优化
├── system_health_monitor.py            # 系统健康监控
├── performance_test_runner.py          # 综合测试运行器
│
├── config/                             # 配置文件目录
│   └── default_config.json            # 默认配置文件
│
├── results/                            # 测试结果目录
│   ├── comprehensive_performance_report.json
│   ├── latency_test_report.json
│   ├── recording_report.json
│   └── health_report.json
│
├── logs/                               # 日志文件目录
│   ├── performance_test.log
│   ├── system_health.log
│   └── error.log
│
└── data/                               # 测试数据目录
    ├── test_symbols.json
    └── historical_data/
```

## 核心模块说明

### 1. 延迟测试框架 (`latency_test_framework.py`)

**主要类:**
- `LatencyMeasurement`: 延迟测量点管理
- `LatencyTestFramework`: 延迟测试框架主类

**核心功能:**
- 端到端延迟监控
- 分段延迟分析
- 多场景测试支持
- 统计分析和报告生成

**测试阶段:**
1. `tick_receive`: 接收tick数据
2. `ddb_calculation`: DDB完成计算
3. `signal_generation`: 生成交易信号
4. `order_submission`: CTP提交申报
5. `order_response`: 收到委托回报

### 2. 行情录制优化 (`market_data_recorder.py`)

**主要类:**
- `MemoryMonitor`: 内存监控器
- `MarketDataRecorder`: 市场数据录制器

**核心功能:**
- 全市场数据录制（tick、分钟、日线）
- 内存使用监控和优化
- 批量写入优化
- 数据压缩和分区存储

**优化特性:**
- 自动内存清理
- 批量写入器
- 分区表存储
- 压缩算法支持

### 3. 系统健康监控 (`system_health_monitor.py`)

**主要类:**
- `HealthCheckResult`: 健康检查结果
- `AlertManager`: 警报管理器
- `SystemHealthMonitor`: 系统健康监控器
- `RecoveryManager`: 自动恢复管理器

**监控组件:**
- 系统资源（CPU、内存、磁盘）
- DolphinDB连接状态
- 流表状态
- 流引擎状态
- 性能指标

**警报功能:**
- 多种通知渠道（邮件、Webhook、控制台）
- 可配置的警报规则
- 警报抑制机制
- 自动恢复功能

### 4. 综合测试运行器 (`performance_test_runner.py`)

**主要类:**
- `PerformanceTestRunner`: 性能测试运行器

**核心功能:**
- 集成所有测试模块
- 配置化测试参数
- 压力测试和极限测试
- 综合报告生成
- 性能分析和优化建议

## 配置文件说明

### 默认配置 (`config/default_config.json`)

基本的系统配置，包括：
- DolphinDB连接参数
- 测试默认设置
- 监控阈值配置

### 详细配置 (`test_config_example.json`)

完整的测试配置示例，包括：
- 延迟测试配置
- 录制测试配置
- 压力测试配置
- 监控配置
- 通知配置

## 测试结果文件

### 综合性能报告 (`comprehensive_performance_report.json`)

包含所有测试模块的综合结果：
```json
{
  "test_session": {
    "start_time": "2024-01-01T10:00:00",
    "end_time": "2024-01-01T11:00:00",
    "duration_minutes": 60
  },
  "test_results": {
    "latency_test": {...},
    "recording_test": {...},
    "stress_test": {...}
  },
  "system_health": {...},
  "performance_summary": {...},
  "recommendations": [...]
}
```

### 延迟测试报告 (`latency_test_report.json`)

详细的延迟测试结果：
```json
{
  "test_timestamp": "2024-01-01T10:00:00",
  "test_results": [
    {
      "scenario": "1_symbols_1_strategies",
      "avg_latency_us": 245.6,
      "p99_latency_us": 892.3,
      "throughput_tps": 18.7
    }
  ],
  "summary": {...}
}
```

### 录制测试报告 (`recording_report.json`)

行情录制测试结果：
```json
{
  "recording_session": {
    "start_time": "2024-01-01T10:00:00",
    "duration_minutes": 30
  },
  "data_statistics": {
    "total_ticks": 50000,
    "data_size_mb": 125.6
  },
  "memory_info": {...},
  "storage_info": {...}
}
```

### 健康报告 (`health_report.json`)

系统健康状态报告：
```json
{
  "report_time": "2024-01-01T10:00:00",
  "overall_status": "HEALTHY",
  "component_status": [
    {
      "component": "cpu",
      "status": "HEALTHY",
      "message": "CPU使用率正常: 45.2%"
    }
  ],
  "recent_alerts": [...],
  "system_summary": {...}
}
```

## 使用流程

### 1. 初始安装
```bash
# 1. 安装依赖和初始化
python setup.py

# 2. 快速验证
python quick_test.py
```

### 2. 运行测试
```bash
# 完整测试
python performance_test_runner.py

# 单项测试
python latency_test_framework.py
python market_data_recorder.py
python system_health_monitor.py
```

### 3. 查看结果
```bash
# 查看测试结果
ls results/

# 查看日志
ls logs/
```

## 扩展开发

### 添加新的测试模块

1. 创建新的测试类
2. 实现标准接口方法
3. 在 `performance_test_runner.py` 中集成
4. 更新配置文件格式

### 添加新的监控指标

1. 在 `SystemHealthMonitor` 中添加检查方法
2. 定义相应的警报规则
3. 实现自动恢复逻辑
4. 更新报告格式

### 添加新的通知渠道

1. 在 `AlertManager` 中实现通知方法
2. 添加配置参数
3. 测试通知功能
4. 更新文档

## 性能优化建议

### 延迟优化
- 使用SSD存储
- 增加内存容量
- 优化网络配置
- 调整DolphinDB参数

### 吞吐量优化
- 使用批量处理
- 优化流表大小
- 调整引擎参数
- 考虑多节点部署

### 内存优化
- 定期清理历史数据
- 使用数据压缩
- 优化缓存策略
- 监控内存泄漏

## 故障排查

### 常见问题
1. DolphinDB连接失败
2. 内存使用率过高
3. 延迟过高
4. 录制失败

### 解决方案
- 检查服务状态
- 调整配置参数
- 优化系统资源
- 查看错误日志

## 注意事项

1. **测试环境**: 建议在专用测试环境中运行
2. **资源监控**: 测试过程中密切监控系统资源
3. **数据清理**: 测试完成后及时清理测试数据
4. **结果分析**: 结合业务场景分析测试结果

## 版本信息

- **当前版本**: 1.0
- **Python要求**: 3.7+
- **DolphinDB要求**: 2.0+
- **主要依赖**: pandas, psutil, requests

## 技术支持

如有问题或建议，请：
1. 查看 README.md 文档
2. 运行 quick_test.py 诊断
3. 查看 logs/ 目录下的错误日志
4. 联系开发团队
