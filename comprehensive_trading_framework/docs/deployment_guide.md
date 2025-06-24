# 部署指南 (Deployment Guide)

## 系统要求

### 硬件要求
- **CPU**: 4核心以上，推荐8核心
- **内存**: 8GB以上，推荐16GB
- **存储**: SSD硬盘，至少100GB可用空间
- **网络**: 稳定的网络连接，延迟<10ms到交易所

### 软件要求
- **操作系统**: Linux (Ubuntu 20.04+) 或 Windows 10+
- **Python**: 3.8+
- **DolphinDB**: 2.00.10+
- **CTP接口**: vnpy-ctp 6.6.9+

## 安装步骤

### 1. DolphinDB安装配置

#### Linux环境
```bash
# 下载DolphinDB
wget https://www.dolphindb.cn/downloads/DolphinDB_Linux64_V2.00.10.zip
unzip DolphinDB_Linux64_V2.00.10.zip

# 配置DolphinDB
cd DolphinDB
cp dolphindb.cfg.sample dolphindb.cfg

# 编辑配置文件
vim dolphindb.cfg
```

#### 关键配置项
```ini
# dolphindb.cfg
localSite=localhost:8848:master
mode=single
maxMemSize=8
maxConnections=512
workerNum=4
localExecutors=3
maxBatchJobWorker=4
enableHTTPS=false
enableChunkGranularityConfig=true
```

#### 启动DolphinDB
```bash
# 启动服务器
./dolphindb -console 0

# 或后台启动
nohup ./dolphindb -console 0 > dolphindb.log 2>&1 &
```

### 2. Python环境配置

#### 创建虚拟环境
```bash
# 创建虚拟环境
python3 -m venv trading_env
source trading_env/bin/activate  # Linux
# 或 trading_env\Scripts\activate  # Windows

# 升级pip
pip install --upgrade pip
```

#### 安装依赖包
```bash
# 安装基础依赖
pip install -r requirements.txt

# 安装CTP接口 (如果需要)
pip install vnpy-ctp

# 安装TA-Lib (技术指标库)
# Linux:
sudo apt-get install libta-lib-dev
pip install TA-Lib

# Windows: 下载预编译包
pip install TA-Lib-0.4.24-cp39-cp39-win_amd64.whl
```

### 3. 系统配置

#### 配置文件设置
```python
# config.py
DDB_CONFIG = {
    "host": "localhost",
    "port": 8848,
    "username": "admin",
    "password": "123456"
}

CTP_CONFIG = {
    "用户名": "your_username",
    "密码": "your_password",
    "经纪商代码": "9999",
    "交易服务器": "tcp://your_td_server:port",
    "行情服务器": "tcp://your_md_server:port",
    "产品名称": "your_app_name",
    "授权编码": "your_auth_code"
}
```

#### 日志配置
```python
# 创建日志目录
mkdir -p logs

# 配置日志轮转
LOGGING_CONFIG = {
    "level": "INFO",
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    "file": "logs/trading_system.log",
    "max_size": 10485760,  # 10MB
    "backup_count": 5
}
```

## 生产环境部署

### 1. 系统架构

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Load Balancer │    │   Web Monitor   │    │   Alert System │
│   (Nginx)       │    │   (Flask/Django)│    │   (Email/SMS)   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                        │
         ▼                       ▼                        ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Trading       │◀──▶│   DolphinDB     │◀──▶│   Monitoring    │
│   Application   │    │   Cluster       │    │   System        │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                        │
         ▼                       ▼                        ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   CTP Gateway   │    │   Data Storage  │    │   Backup        │
│   (Primary)     │    │   (HDD/SSD)     │    │   System        │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │
         ▼
┌─────────────────┐
│   CTP Gateway   │
│   (Backup)      │
└─────────────────┘
```

### 2. 高可用配置

#### DolphinDB集群配置
```bash
# 控制节点配置
# controller.cfg
localSite=192.168.1.100:8900:controller
mode=controller
dfsReplicationFactor=2
dfsReplicaReliabilityLevel=1

# 数据节点配置
# datanode1.cfg
localSite=192.168.1.101:8901:datanode1
mode=datanode
controllerSite=192.168.1.100:8900:controller

# datanode2.cfg
localSite=192.168.1.102:8902:datanode2
mode=datanode
controllerSite=192.168.1.100:8900:controller
```

#### 应用程序高可用
```python
# 主备切换配置
HA_CONFIG = {
    "primary_ddb": "192.168.1.101:8901",
    "backup_ddb": "192.168.1.102:8902",
    "failover_timeout": 5,  # 秒
    "health_check_interval": 1  # 秒
}

class HAManager:
    def __init__(self, config):
        self.config = config
        self.current_connection = None
        
    def get_connection(self):
        # 实现主备切换逻辑
        pass
```

### 3. 容器化部署

#### Dockerfile
```dockerfile
FROM python:3.9-slim

# 安装系统依赖
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libta-lib-dev \
    && rm -rf /var/lib/apt/lists/*

# 设置工作目录
WORKDIR /app

# 复制依赖文件
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 复制应用代码
COPY . .

# 创建非root用户
RUN useradd -m -u 1000 trader
USER trader

# 暴露端口
EXPOSE 8080

# 启动命令
CMD ["python", "comprehensive_trading_system.py"]
```

#### Docker Compose
```yaml
# docker-compose.yml
version: '3.8'

services:
  dolphindb:
    image: ddb/dolphindb:latest
    ports:
      - "8848:8848"
    volumes:
      - ./data:/data
      - ./config/dolphindb.cfg:/opt/dolphindb/server/dolphindb.cfg
    environment:
      - DDB_LICENSE_FILE=/data/dolphindb.lic
    restart: unless-stopped

  trading-system:
    build: .
    depends_on:
      - dolphindb
    volumes:
      - ./config:/app/config
      - ./logs:/app/logs
    environment:
      - DDB_HOST=dolphindb
      - DDB_PORT=8848
    restart: unless-stopped

  monitoring:
    image: grafana/grafana:latest
    ports:
      - "3000:3000"
    volumes:
      - grafana-storage:/var/lib/grafana
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin123
    restart: unless-stopped

volumes:
  grafana-storage:
```

### 4. 监控和告警

#### 系统监控
```python
# monitoring.py
import psutil
import time
from datetime import datetime

class SystemMonitor:
    def __init__(self):
        self.metrics = {}
    
    def collect_metrics(self):
        """收集系统指标"""
        self.metrics.update({
            'timestamp': datetime.now(),
            'cpu_percent': psutil.cpu_percent(),
            'memory_percent': psutil.virtual_memory().percent,
            'disk_usage': psutil.disk_usage('/').percent,
            'network_io': psutil.net_io_counters()._asdict()
        })
    
    def check_alerts(self):
        """检查告警条件"""
        alerts = []
        
        if self.metrics.get('cpu_percent', 0) > 80:
            alerts.append("High CPU usage")
        
        if self.metrics.get('memory_percent', 0) > 85:
            alerts.append("High memory usage")
        
        return alerts
```

#### 交易监控
```python
# trading_monitor.py
class TradingMonitor:
    def __init__(self, session):
        self.session = session
    
    def get_trading_metrics(self):
        """获取交易指标"""
        return self.session.run("""
            metrics = dict(STRING, ANY)
            
            // 订单统计
            order_count = exec count(*) from orderStream
            metrics["order_count"] = order_count[0]
            
            // 成交统计
            trade_count = exec count(*) from trades
            metrics["trade_count"] = trade_count[0]
            
            // 持仓统计
            position_count = exec count(*) from positionTable
            metrics["position_count"] = position_count[0]
            
            // 盈亏统计
            realized_pnl = exec sum(pnl) from realized_positions
            metrics["realized_pnl"] = iif(size(realized_pnl) > 0, realized_pnl[0], 0.0)
            
            return metrics
        """)
```

### 5. 备份和恢复

#### 数据备份策略
```bash
#!/bin/bash
# backup.sh

# 设置变量
BACKUP_DIR="/backup/trading_system"
DATE=$(date +%Y%m%d_%H%M%S)
DDB_DATA_DIR="/opt/dolphindb/server/data"

# 创建备份目录
mkdir -p $BACKUP_DIR/$DATE

# 备份DolphinDB数据
echo "Backing up DolphinDB data..."
tar -czf $BACKUP_DIR/$DATE/ddb_data.tar.gz $DDB_DATA_DIR

# 备份配置文件
echo "Backing up configuration..."
cp -r config/ $BACKUP_DIR/$DATE/

# 备份日志文件
echo "Backing up logs..."
cp -r logs/ $BACKUP_DIR/$DATE/

# 清理旧备份 (保留7天)
find $BACKUP_DIR -type d -mtime +7 -exec rm -rf {} \;

echo "Backup completed: $BACKUP_DIR/$DATE"
```

#### 恢复流程
```bash
#!/bin/bash
# restore.sh

BACKUP_DATE=$1
BACKUP_DIR="/backup/trading_system"

if [ -z "$BACKUP_DATE" ]; then
    echo "Usage: $0 <backup_date>"
    exit 1
fi

echo "Restoring from backup: $BACKUP_DATE"

# 停止服务
systemctl stop trading-system
systemctl stop dolphindb

# 恢复数据
tar -xzf $BACKUP_DIR/$BACKUP_DATE/ddb_data.tar.gz -C /

# 恢复配置
cp -r $BACKUP_DIR/$BACKUP_DATE/config/* config/

# 启动服务
systemctl start dolphindb
sleep 10
systemctl start trading-system

echo "Restore completed"
```

### 6. 性能优化

#### DolphinDB优化
```ini
# 性能优化配置
maxMemSize=16                    # 增加内存限制
workerNum=8                      # 增加工作线程
localExecutors=6                 # 增加本地执行器
maxBatchJobWorker=8              # 增加批处理工作线程
enableChunkGranularityConfig=true # 启用块粒度配置
chunkCacheEngineMemSize=4        # 块缓存内存大小(GB)
```

#### 应用程序优化
```python
# 连接池配置
CONNECTION_POOL_CONFIG = {
    "max_connections": 10,
    "min_connections": 2,
    "connection_timeout": 30,
    "retry_attempts": 3
}

# 批处理配置
BATCH_CONFIG = {
    "batch_size": 1000,
    "batch_timeout": 100,  # ms
    "max_queue_size": 10000
}
```

### 7. 安全配置

#### 网络安全
```bash
# 防火墙配置
ufw allow 8848/tcp  # DolphinDB
ufw allow 22/tcp    # SSH
ufw deny 80/tcp     # 禁用HTTP
ufw enable
```

#### 应用安全
```python
# 加密配置
SECURITY_CONFIG = {
    "encrypt_passwords": True,
    "use_ssl": True,
    "ssl_cert_path": "/etc/ssl/certs/trading.crt",
    "ssl_key_path": "/etc/ssl/private/trading.key"
}
```

### 8. 运维脚本

#### 启动脚本
```bash
#!/bin/bash
# start_trading_system.sh

echo "Starting Trading System..."

# 检查DolphinDB状态
if ! pgrep -f dolphindb > /dev/null; then
    echo "Starting DolphinDB..."
    cd /opt/dolphindb/server
    nohup ./dolphindb -console 0 > /var/log/dolphindb.log 2>&1 &
    sleep 10
fi

# 启动交易系统
echo "Starting Trading Application..."
cd /opt/trading_system
source venv/bin/activate
nohup python comprehensive_trading_system.py > /var/log/trading_system.log 2>&1 &

echo "Trading System started successfully"
```

#### 健康检查脚本
```bash
#!/bin/bash
# health_check.sh

# 检查DolphinDB
if ! curl -s http://localhost:8848 > /dev/null; then
    echo "DolphinDB is down"
    exit 1
fi

# 检查交易应用
if ! pgrep -f comprehensive_trading_system.py > /dev/null; then
    echo "Trading application is down"
    exit 1
fi

echo "All services are healthy"
exit 0
```

## 故障排除

### 常见问题

1. **DolphinDB连接失败**
   - 检查防火墙设置
   - 验证配置文件
   - 查看DolphinDB日志

2. **CTP连接问题**
   - 验证账户信息
   - 检查网络连接
   - 确认交易时间

3. **内存不足**
   - 调整DolphinDB内存配置
   - 优化流表大小
   - 增加系统内存

4. **性能问题**
   - 检查CPU使用率
   - 优化查询语句
   - 调整线程配置

### 日志分析
```bash
# 查看系统日志
tail -f /var/log/trading_system.log

# 查看DolphinDB日志
tail -f /var/log/dolphindb.log

# 查看错误日志
grep ERROR /var/log/trading_system.log
```

这个部署指南提供了从开发环境到生产环境的完整部署流程，包括高可用配置、监控告警、备份恢复等关键运维内容。
