# 综合交易框架 (Comprehensive Trading Framework)

基于DolphinDB的策略执行分离和复杂订单管理系统

## 项目概述

本项目是一个完整的量化交易框架，实现了策略执行分离架构和高级订单管理功能。系统基于DolphinDB流计算引擎，支持多策略、多时间间隔、横截面策略和复杂订单管理。

## 核心特性

### 1. 策略执行分离架构
- **因子计算层**: 独立的因子计算引擎，支持多种技术指标
- **策略层**: 纯信号生成，不涉及具体交易执行
- **执行层**: 根据信号和持仓状态执行交易决策
- **风险控制层**: 实时风险监控和订单拦截

### 2. 高级订单管理
- **订单跟踪**: 实时监控订单状态和价格偏离
- **撤单重发**: 智能撤单重发机制
- **部分成交处理**: 智能部分成交处理策略
- **限价单管理**: 分层限价单策略
- **订单队列管理**: 处理订单时序问题

### 3. 多策略支持
- **布林带策略**: 经典均值回归策略
- **移动平均交叉**: 趋势跟踪策略
- **横截面策略**: 多品种排序选股
- **策略工厂**: 动态策略创建和管理

### 4. 多时间间隔支持
- **Tick级别**: 实时tick数据处理
- **秒级聚合**: 10秒、30秒聚合
- **分钟级聚合**: 1分钟、5分钟、15分钟
- **小时级聚合**: 1小时、4小时

## 系统架构

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Market Data   │───▶│   DolphinDB     │───▶│   Strategy      │
│   (CTP/Other)   │    │   Streaming     │    │   Factory       │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                │                        │
                                ▼                        ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Order         │◀───│   Factor        │───▶│   Execution     │
│   Management    │    │   Calculation   │    │   Engine        │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                        │
         ▼                       ▼                        ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Risk          │    │   Position      │    │   Order         │
│   Management    │    │   Management    │    │   Execution     │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## 文件结构

```
comprehensive_trading_framework/
├── comprehensive_trading_system.py    # 主交易系统
├── config.py                         # 配置文件
├── strategy_factory.py               # 策略工厂
├── order_management.py               # 订单管理系统
├── README.md                         # 项目文档
├── requirements.txt                  # 依赖包
├── examples/                         # 示例代码
│   ├── basic_usage.py
│   ├── custom_strategy.py
│   └── backtest_example.py
└── docs/                            # 详细文档
    ├── architecture.md
    ├── api_reference.md
    └── deployment_guide.md
```

## 快速开始

### 1. 环境准备

```bash
# 安装依赖
pip install -r requirements.txt

# 启动DolphinDB服务器
# 确保DolphinDB服务器运行在localhost:8848
```

### 2. 配置设置

编辑 `config.py` 文件，设置您的交易账户信息：

```python
CTP_CONFIG = {
    "用户名": "your_username",
    "密码": "your_password",
    "经纪商代码": "9999",
    "交易服务器": "your_td_server",
    "行情服务器": "your_md_server"
}
```

### 3. 运行系统

```python
from comprehensive_trading_system import main

# 启动交易系统
main()
```

## 核心模块详解

### 1. ComprehensiveTradingSystem

主交易系统类，负责：
- DolphinDB连接管理
- 流表创建和管理
- 因子计算引擎初始化
- 策略执行引擎启动
- 订单管理系统集成

```python
# 初始化交易系统
trading_system = ComprehensiveTradingSystem()
trading_system.initialize_system()
```

### 2. StrategyFactory

策略工厂类，支持：
- 动态策略创建
- 策略配置管理
- 多策略组合
- 策略脚本生成

```python
# 创建策略工厂
factory = StrategyFactory()
strategy = factory.create_strategy("bollinger", "bb_strategy", config)
```

### 3. AdvancedOrderManager

高级订单管理器，提供：
- 订单状态跟踪
- 智能撤单重发
- 部分成交处理
- 风险控制检查

```python
# 初始化订单管理器
order_manager = AdvancedOrderManager(session, config)
order_manager.initialize_ddb_functions()
```

## DolphinDB流表结构

### 基础数据流表
- `tickStream`: Tick数据流
- `agg1min/5min/15min`: 聚合数据流
- `factor_stream`: 通用因子流
- `bollinger_stream`: 布林带专用流

### 交易相关流表
- `orderStream`: 订单流
- `trades`: 成交流
- `positions_orders`: 委托回报流
- `cancel_reorder_stream`: 撤单重发流

### 持仓和盈亏表
- `positionTable`: 持仓表
- `realized_positions`: 已实现盈亏
- `unrealized_positions`: 未实现盈亏

## 策略开发指南

### 1. 创建自定义策略

```python
class CustomStrategy(BaseStrategy):
    def get_factor_calculation(self) -> str:
        return """
        // 自定义因子计算逻辑
        custom_metrics = <[
            timestamp, price,
            // 添加您的因子计算
        ]>
        """
    
    def get_signal_logic(self) -> str:
        return """
        // 自定义信号逻辑
        def custom_signal_logic(params...){
            // 信号生成逻辑
        }
        """
    
    def get_execution_logic(self) -> str:
        return """
        // 自定义执行逻辑
        def custom_execution(params...){
            // 执行逻辑
        }
        """
```

### 2. 注册策略到工厂

```python
factory = StrategyFactory()
factory.strategy_classes["custom"] = CustomStrategy
strategy = factory.create_strategy("custom", "my_strategy", config)
```

## 配置说明

### 策略配置
```python
STRATEGY_CONFIG = {
    "bollinger": {
        "enabled": True,
        "timeframe": "1min",
        "period": 20,
        "std_dev": 2.0,
        "volume_per_order": 5
    }
}
```

### 订单管理配置
```python
ORDER_CONFIG = {
    "tracking": {
        "price_tolerance": 0.02,  # 价格容忍度
        "time_threshold": 30000,  # 时间阈值(毫秒)
        "max_retries": 3
    }
}
```

### 风险控制配置
```python
RISK_CONFIG = {
    "max_daily_loss": 10000,
    "max_position_per_symbol": 50,
    "max_order_frequency": 100
}
```

## 监控和日志

系统提供完整的监控和日志功能：

- **实时监控**: 订单状态、持仓情况、盈亏统计
- **性能监控**: 延迟统计、吞吐量监控
- **风险监控**: 实时风险指标监控
- **日志记录**: 分级日志记录，支持文件轮转

## 部署指南

### 1. 生产环境部署

```bash
# 1. 配置DolphinDB集群
# 2. 设置CTP生产环境连接
# 3. 配置监控和告警
# 4. 启动交易系统
python comprehensive_trading_system.py
```

### 2. 容器化部署

```dockerfile
FROM python:3.9
COPY . /app
WORKDIR /app
RUN pip install -r requirements.txt
CMD ["python", "comprehensive_trading_system.py"]
```

## 性能优化

- **流计算优化**: 使用DolphinDB原生流计算引擎
- **内存管理**: 合理设置流表大小和清理策略
- **并发处理**: 多线程订单处理和风险检查
- **网络优化**: 连接池和重连机制

## 风险提示

⚠️ **重要提示**: 
- 本系统仅供学习和研究使用
- 实盘交易前请充分测试
- 注意风险控制和资金管理
- 遵守相关法律法规

## 技术支持

如有问题，请参考：
1. 详细文档: `docs/` 目录
2. 示例代码: `examples/` 目录
3. API参考: `docs/api_reference.md`

## 版本历史

- v1.0.0: 初始版本，基础功能实现
- v1.1.0: 添加横截面策略支持
- v1.2.0: 增强订单管理功能
- v1.3.0: 性能优化和监控增强

## 许可证

本项目采用MIT许可证，详见LICENSE文件。
