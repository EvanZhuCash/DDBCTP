# IB Multi-Gateway Trading System

基于DolphinDB的多网关交易系统，支持Interactive Brokers和多个加密货币交易所。

## 功能特性

- **Interactive Brokers集成**: 使用ib_async库连接TWS/Gateway
- **多加密货币交易所**: 支持Binance、OKX、Bybit、Gate.io等
- **统一数据流**: 所有市场数据统一存储到DolphinDB
- **智能订单路由**: 自动选择最佳网关执行订单
- **实时监控**: 网关状态和性能监控
- **异步架构**: 高性能异步数据处理

## 系统架构

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   IB Gateway    │    │ Binance Gateway │    │   OKX Gateway   │
│   (ib_async)    │    │  (vnpy_binance) │    │  (vnpy_okx)     │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
                    ┌─────────────────┐
                    │  DolphinDB      │
                    │  Data Engine    │
                    │                 │
                    │ - Tick Stream   │
                    │ - Factor Calc   │
                    │ - Order Mgmt    │
                    │ - Risk Control  │
                    └─────────────────┘
```

## 安装要求

### 1. 系统要求
- Python 3.8+
- DolphinDB Server 3.00.2+
- Interactive Brokers TWS/Gateway (用于IB交易)

### 2. 安装依赖
```bash
pip install -r requirements.txt
```

### 3. DolphinDB设置
确保DolphinDB服务器运行在localhost:8848，用户名admin，密码123456

## 配置说明

### IB配置 (config/ib_config.py)
```python
IB_CONFIG = {
    'host': '127.0.0.1',
    'port': 7497,  # Paper trading
    'client_id': 1,
    'contracts': [
        {
            'symbol': 'AAPL',
            'type': 'STK',
            'exchange': 'SMART',
            'currency': 'USD'
        }
    ]
}
```

### 加密货币配置 (config/crypto_config.py)
```python
BINANCE_CONFIG = {
    'testnet': True,
    'api_key': 'your_api_key',
    'secret': 'your_secret',
    'symbols': ['BTCUSDT', 'ETHUSDT']
}
```

## 使用方法

### 1. 启动IB Gateway测试
```bash
cd ib_multi_gateway
python tests/test_ib_gateway.py
```

### 2. 使用多网关管理器
```python
from gateway_manager import MultiGatewayManager
from ib_gateway.ib_connector import IBGateway
from config.ib_config import IB_TEST_CONFIG

# 创建管理器
manager = MultiGatewayManager()
await manager.initialize()

# 注册IB网关
ib_gateway = IBGateway(IB_TEST_CONFIG)
await ib_gateway.connect()
manager.register_gateway('IB', ib_gateway)

# 下单
order_data = {
    'symbol': 'AAPL',
    'direction': 'BUY',
    'volume': 1,
    'order_type': 'MARKET'
}
await manager.route_order(order_data)
```

## 数据表结构

### IB市场数据表 (ib_tick_stream)
- timestamp: 时间戳
- symbol: 合约代码
- price: 最新价格
- volume: 成交量
- bid/ask: 买卖价
- bid_size/ask_size: 买卖量
- gateway: 网关标识

### 统一订单表 (unified_order_stream)
- timestamp: 时间戳
- symbol: 合约代码
- direction: 买卖方向
- price: 价格
- volume: 数量
- status: 订单状态
- gateway: 网关标识

## 测试说明

### IB测试前提条件
1. 安装TWS或IB Gateway
2. 启用API连接 (Configure > API > Settings)
3. 设置为Paper Trading模式
4. 确保端口7497开放

### 运行测试
```bash
# 测试IB连接和市场数据
python tests/test_ib_gateway.py

# 选择测试类型:
# 1. 完整测试 (连接+数据+订单)
# 2. 仅市场数据测试
# 3. 检查DolphinDB数据
```

## 开发计划

### 第1阶段: IB Gateway (当前)
- [x] IB连接器实现
- [x] 市场数据订阅
- [x] 基本订单执行
- [x] DolphinDB集成

### 第2阶段: 加密货币网关
- [ ] Binance网关集成
- [ ] OKX网关集成
- [ ] Bybit网关集成
- [ ] 统一数据接口

### 第3阶段: 高级功能
- [ ] 智能订单路由优化
- [ ] 风险管理模块
- [ ] 性能监控面板
- [ ] 策略回测框架

## 注意事项

1. **风险警告**: 本系统仅用于学习和测试，实盘交易需要充分测试
2. **API限制**: 注意各交易所的API调用频率限制
3. **数据存储**: DolphinDB表会占用内存，注意监控资源使用
4. **网络稳定性**: 确保网络连接稳定，避免数据丢失

## 故障排除

### 常见问题
1. **IB连接失败**: 检查TWS/Gateway是否运行，API是否启用
2. **DolphinDB连接失败**: 检查服务器状态和连接参数
3. **依赖安装失败**: 使用虚拟环境，确保Python版本兼容

### 日志查看
系统使用Python logging模块，可以通过调整日志级别查看详细信息：
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## 联系支持

如有问题或建议，请参考项目文档或联系开发团队。
