# DolphinDB CTP 前端监控系统

基于Flask + Vue.js + WebSocket的实时监控系统，为DolphinDB CTP交易系统提供策略实时交互、日志警报系统和实时监控面板。

## 功能特性

### 🎯 **策略实时交互**
- **策略控制矩阵**: 可视化策略-品种开关控制
- **实时状态同步**: WebSocket实时更新策略状态
- **批量操作**: 支持一键启用/停用所有策略
- **策略配置**: 动态调整策略参数和风险限额
- **性能监控**: 实时显示策略PnL、Sharpe比率等指标

### 📊 **实时监控面板**
- **系统资源监控**: CPU、内存、磁盘使用率实时图表
- **DolphinDB状态**: 连接状态、流表、引擎监控
- **多时间范围**: 支持1小时、6小时、24小时数据展示
- **健康检查**: 自动检测系统组件健康状态
- **警报设置**: 可配置的资源使用率警报阈值

### 📝 **日志警报系统**
- **实时日志流**: 类似终端的实时日志显示
- **智能过滤**: 按级别、组件、关键词过滤日志
- **统计分析**: 日志级别分布和趋势图表
- **警报规则**: 可配置的日志警报规则
- **多渠道通知**: 邮件、短信、Webhook通知支持

### 🔧 **技术特性**
- **响应式设计**: 支持桌面和移动设备
- **实时通信**: WebSocket双向通信
- **模块化架构**: 易于扩展和维护
- **美观界面**: 现代化UI设计
- **高性能**: 优化的前端性能

## 技术栈

### 后端
- **Flask**: Web框架
- **Flask-SocketIO**: WebSocket支持
- **DolphinDB Python API**: 数据库连接
- **psutil**: 系统监控
- **pandas**: 数据处理

### 前端
- **Bootstrap 5**: UI框架
- **Chart.js**: 图表库
- **Socket.IO**: WebSocket客户端
- **Font Awesome**: 图标库
- **原生JavaScript**: 交互逻辑

## 项目结构

```
frontend_monitoring/
├── app.py                          # Flask主应用
├── requirements.txt                # Python依赖
├── README.md                       # 项目说明
│
├── templates/                      # HTML模板
│   ├── base.html                  # 基础模板
│   ├── index.html                 # 总览页面
│   ├── strategy.html              # 策略控制页面
│   ├── monitoring.html            # 系统监控页面
│   └── logs.html                  # 日志警报页面
│
├── static/                         # 静态资源
│   ├── css/
│   │   └── custom.css             # 自定义样式
│   └── js/
│       └── common.js              # 通用JavaScript
│
└── screenshots/                    # 界面截图
    ├── dashboard.png
    ├── strategy.png
    ├── monitoring.png
    └── logs.png
```

## 安装和使用

### 1. 环境要求
- Python 3.7+
- DolphinDB 2.0+
- 现代浏览器（Chrome、Firefox、Safari、Edge）

### 2. 安装依赖
```bash
cd frontend_monitoring
pip install -r requirements.txt
```

### 3. 配置DolphinDB
确保DolphinDB服务运行在 `localhost:8848`，用户名 `admin`，密码 `123456`。

如需修改连接参数，请编辑 `app.py` 中的 `DolphinDBManager` 类：
```python
self.session.connect("your_host", your_port, "your_username", "your_password")
```

### 4. 启动应用
```bash
python app.py
```

应用将在 `http://localhost:5000` 启动。

### 5. 访问界面
- **总览**: http://localhost:5000/
- **策略控制**: http://localhost:5000/strategy
- **系统监控**: http://localhost:5000/monitoring
- **日志警报**: http://localhost:5000/logs

## 功能详解

### 策略控制面板

#### 策略控制矩阵
- 表格形式展示所有策略和品种的组合
- 切换按钮控制策略启用/停用状态
- 实时同步状态变化到所有客户端
- 支持批量操作

#### 策略配置
- 动态调整策略参数
- 设置风险限额和最大持仓
- JSON格式的灵活参数配置
- 实时生效无需重启

#### 操作日志
- 记录所有策略操作历史
- 时间戳和操作详情
- 自动滚动显示最新操作

### 系统监控面板

#### 实时指标卡片
- CPU使用率：实时显示和进度条
- 内存使用率：总量、已用、可用
- 磁盘使用率：空间使用情况
- DolphinDB状态：连接状态检查

#### 性能趋势图表
- 多时间范围选择（1小时/6小时/24小时）
- 平滑的线性图表
- 自动数据聚合和优化
- 响应式图表大小

#### 流表监控
- 实时显示流表状态
- 记录数量统计
- 存在性检查
- 最后更新时间

#### 警报设置
- 可配置的阈值设置
- 多种通知方式
- 实时警报触发
- 警报历史记录

### 日志警报系统

#### 实时日志流
- 终端风格的日志显示
- 颜色编码的日志级别
- 自动滚动和手动控制
- 时间戳和组件标识

#### 智能过滤
- 按日志级别过滤
- 按组件类型过滤
- 关键词搜索
- 实时过滤结果

#### 统计图表
- 日志级别分布饼图
- 日志趋势线性图
- 实时数据更新
- 可视化统计分析

#### 警报规则配置
- 灵活的规则定义
- 多种触发条件
- 多渠道通知配置
- 规则启用/禁用控制

## API接口

### 策略相关
- `GET /api/strategy/matrix` - 获取策略控制矩阵
- `POST /api/strategy/update` - 更新策略状态
- `GET /api/strategy/performance` - 获取策略性能数据

### 系统监控
- `GET /api/system/status` - 获取系统状态
- `GET /api/logs` - 获取日志数据

### WebSocket事件
- `/strategy` 命名空间：策略状态更新
- `/monitor` 命名空间：系统指标推送

## 自定义配置

### 修改连接参数
编辑 `app.py` 中的连接配置：
```python
class DolphinDBManager:
    def connect(self):
        self.session.connect("localhost", 8848, "admin", "123456")
```

### 调整更新频率
修改 `app.py` 中的监控间隔：
```python
def start_monitoring(self, interval: float = 30.0):  # 30秒间隔
```

### 自定义样式
编辑 `static/css/custom.css` 文件：
```css
:root {
    --primary-gradient: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    --success-color: #28a745;
    /* 其他颜色变量 */
}
```

### 添加新的监控指标
1. 在 `SystemMonitor` 类中添加新的检查方法
2. 在前端添加相应的显示组件
3. 更新WebSocket事件处理

## 部署建议

### 开发环境
```bash
python app.py  # 开发模式，自动重载
```

### 生产环境
使用Gunicorn + Nginx部署：
```bash
# 安装Gunicorn
pip install gunicorn

# 启动应用
gunicorn --worker-class eventlet -w 1 --bind 0.0.0.0:5000 app:app
```

### Docker部署
```dockerfile
FROM python:3.9-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .
EXPOSE 5000

CMD ["gunicorn", "--worker-class", "eventlet", "-w", "1", "--bind", "0.0.0.0:5000", "app:app"]
```

## 故障排查

### 常见问题

1. **DolphinDB连接失败**
   - 检查DolphinDB服务是否运行
   - 确认连接参数正确
   - 检查网络连接和防火墙

2. **WebSocket连接失败**
   - 检查浏览器控制台错误
   - 确认端口5000可访问
   - 检查代理服务器配置

3. **图表不显示**
   - 检查Chart.js库是否加载
   - 确认Canvas元素存在
   - 查看浏览器控制台错误

4. **实时数据不更新**
   - 检查WebSocket连接状态
   - 确认后端监控线程运行
   - 查看服务器日志

### 调试模式
启用Flask调试模式：
```python
if __name__ == '__main__':
    socketio.run(app, host='0.0.0.0', port=5000, debug=True)
```

### 日志记录
添加详细日志：
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## 扩展开发

### 添加新页面
1. 创建HTML模板文件
2. 在 `app.py` 中添加路由
3. 实现相应的API接口
4. 添加前端JavaScript逻辑

### 集成新的数据源
1. 创建新的管理器类
2. 实现数据获取方法
3. 添加WebSocket事件
4. 更新前端显示组件

### 自定义警报规则
1. 扩展 `AlertManager` 类
2. 添加新的触发条件
3. 实现通知渠道
4. 更新配置界面

## 性能优化

### 前端优化
- 使用CDN加载第三方库
- 压缩CSS和JavaScript文件
- 实现图片懒加载
- 优化图表渲染性能

### 后端优化
- 使用连接池管理数据库连接
- 实现数据缓存机制
- 优化WebSocket消息频率
- 使用异步处理提高并发

### 网络优化
- 启用Gzip压缩
- 使用HTTP/2协议
- 配置适当的缓存策略
- 优化WebSocket心跳机制

## 安全考虑

### 认证授权
- 实现用户登录系统
- 添加角色权限控制
- 使用JWT令牌认证
- 配置HTTPS加密

### 数据安全
- 验证所有输入数据
- 防止SQL注入攻击
- 限制API访问频率
- 记录操作审计日志

## 技术支持

如有问题或建议，请：
1. 查看本文档的故障排查部分
2. 检查浏览器控制台和服务器日志
3. 确认DolphinDB连接和数据
4. 联系开发团队获取支持

## 版本信息

- **当前版本**: 1.0.0
- **Python要求**: 3.7+
- **DolphinDB要求**: 2.0+
- **浏览器要求**: 现代浏览器（支持WebSocket和ES6）

## 更新日志

### v1.0.0 (2024-01-01)
- 初始版本发布
- 实现策略控制面板
- 实现系统监控面板
- 实现日志警报系统
- 支持WebSocket实时通信
- 响应式UI设计
