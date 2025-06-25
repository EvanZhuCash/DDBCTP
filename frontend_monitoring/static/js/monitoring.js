/**
 * 系统监控页面JavaScript模块
 * 处理系统指标监控、实时图表、DolphinDB状态等功能
 */

class MonitoringController {
    constructor() {
        this.socket = null;
        this.systemChart = null;
        this.systemMetrics = {};
        this.chartData = {
            cpu: [],
            memory: [],
            disk: [],
            timestamps: []
        };
        this.maxDataPoints = 50;
        this.timeRange = '1h'; // 1h, 6h, 24h
        
        this.init();
    }
    
    init() {
        this.initWebSocket();
        this.initSystemChart();
        this.loadSystemStatus();
        this.setupEventListeners();
        
        // 定期刷新数据
        setInterval(() => this.refreshData(), 5000);
    }
    
    initWebSocket() {
        this.socket = wsManager.connect('/monitor', {
            onConnect: () => {
                console.log('监控WebSocket连接成功');
                this.updateConnectionStatus(true);
            },
            onDisconnect: () => {
                console.log('监控WebSocket连接断开');
                this.updateConnectionStatus(false);
            },
            onError: (error) => {
                console.error('监控WebSocket错误:', error);
                this.updateConnectionStatus(false);
            },
            system_metrics_update: (data) => {
                this.handleSystemMetricsUpdate(data);
            },
            dolphindb_status_update: (data) => {
                this.handleDolphinDBStatusUpdate(data);
            },
            alert_triggered: (data) => {
                this.handleAlertTriggered(data);
            }
        });
    }
    
    initSystemChart() {
        const config = {
            type: 'line',
            data: {
                labels: [],
                datasets: [{
                    label: 'CPU使用率 (%)',
                    data: [],
                    borderColor: CONFIG.CHART_COLORS.PRIMARY,
                    backgroundColor: CONFIG.CHART_COLORS.PRIMARY + '20',
                    tension: 0.4,
                    fill: false
                }, {
                    label: '内存使用率 (%)',
                    data: [],
                    borderColor: CONFIG.CHART_COLORS.SUCCESS,
                    backgroundColor: CONFIG.CHART_COLORS.SUCCESS + '20',
                    tension: 0.4,
                    fill: false
                }, {
                    label: '磁盘使用率 (%)',
                    data: [],
                    borderColor: CONFIG.CHART_COLORS.WARNING,
                    backgroundColor: CONFIG.CHART_COLORS.WARNING + '20',
                    tension: 0.4,
                    fill: false
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                interaction: {
                    mode: 'index',
                    intersect: false,
                },
                scales: {
                    x: {
                        display: true,
                        title: {
                            display: true,
                            text: '时间'
                        }
                    },
                    y: {
                        display: true,
                        title: {
                            display: true,
                            text: '使用率 (%)'
                        },
                        min: 0,
                        max: 100
                    }
                },
                plugins: {
                    legend: {
                        position: 'top',
                    },
                    tooltip: {
                        callbacks: {
                            label: function(context) {
                                return context.dataset.label + ': ' + formatPercent(context.parsed.y);
                            }
                        }
                    }
                }
            }
        };
        
        this.systemChart = chartManager.createChart('system-performance-chart', config);
    }
    
    async loadSystemStatus() {
        try {
            const response = await apiManager.get('/system/status');
            if (response.success) {
                this.updateSystemCards(response.data);
                this.updateDolphinDBStatus(response.data.dolphindb);
                this.updateStreamTables(response.data.stream_tables);
            } else {
                showNotification('加载系统状态失败: ' + response.error, 'danger');
            }
        } catch (error) {
            console.error('加载系统状态错误:', error);
            showNotification('加载系统状态失败', 'danger');
        }
    }
    
    updateSystemCards(data) {
        // 更新CPU使用率
        const cpuUsage = data.cpu_usage || 0;
        document.getElementById('cpu-usage-display').textContent = formatPercent(cpuUsage);
        document.getElementById('cpu-progress').style.width = cpuUsage + '%';
        
        // 更新内存使用率
        const memoryUsage = data.memory_usage || 0;
        document.getElementById('memory-usage-display').textContent = formatPercent(memoryUsage.percent);
        document.getElementById('memory-progress').style.width = memoryUsage.percent + '%';
        document.getElementById('memory-used').textContent = formatFileSize(memoryUsage.used);
        document.getElementById('memory-total').textContent = formatFileSize(memoryUsage.total);
        
        // 更新磁盘使用率
        const diskUsage = data.disk_usage || 0;
        document.getElementById('disk-usage-display').textContent = formatPercent(diskUsage.percent);
        document.getElementById('disk-progress').style.width = diskUsage.percent + '%';
        document.getElementById('disk-used').textContent = formatFileSize(diskUsage.used);
        document.getElementById('disk-total').textContent = formatFileSize(diskUsage.total);
        
        // 更新网络状态
        if (data.network) {
            document.getElementById('network-sent').textContent = formatFileSize(data.network.bytes_sent);
            document.getElementById('network-recv').textContent = formatFileSize(data.network.bytes_recv);
        }
        
        // 更新图表数据
        this.updateChartData(data);
    }
    
    updateChartData(data) {
        const now = new Date();
        const timeLabel = now.toLocaleTimeString('zh-CN');
        
        // 添加新数据点
        this.chartData.timestamps.push(timeLabel);
        this.chartData.cpu.push(data.cpu_usage || 0);
        this.chartData.memory.push(data.memory_usage?.percent || 0);
        this.chartData.disk.push(data.disk_usage?.percent || 0);
        
        // 限制数据点数量
        if (this.chartData.timestamps.length > this.maxDataPoints) {
            this.chartData.timestamps.shift();
            this.chartData.cpu.shift();
            this.chartData.memory.shift();
            this.chartData.disk.shift();
        }
        
        // 更新图表
        if (this.systemChart) {
            this.systemChart.data.labels = this.chartData.timestamps;
            this.systemChart.data.datasets[0].data = this.chartData.cpu;
            this.systemChart.data.datasets[1].data = this.chartData.memory;
            this.systemChart.data.datasets[2].data = this.chartData.disk;
            this.systemChart.update('none');
        }
    }
    
    updateDolphinDBStatus(dolphindbData) {
        const statusElement = document.getElementById('dolphindb-status');
        const connectionElement = document.getElementById('dolphindb-connection');
        
        if (statusElement && connectionElement) {
            const isConnected = dolphindbData?.connected || false;
            const statusClass = isConnected ? 'status-online' : 'status-offline';
            const statusText = isConnected ? '在线' : '离线';
            
            statusElement.innerHTML = `
                <span class="status-indicator ${statusClass}"></span>
                ${statusText}
            `;
            
            connectionElement.textContent = isConnected ? '已连接' : '连接断开';
            
            // 更新详细信息
            if (dolphindbData && isConnected) {
                document.getElementById('dolphindb-version').textContent = dolphindbData.version || 'N/A';
                document.getElementById('dolphindb-uptime').textContent = dolphindbData.uptime || 'N/A';
                document.getElementById('dolphindb-sessions').textContent = dolphindbData.sessions || 0;
            }
        }
    }
    
    updateStreamTables(streamTables) {
        const container = document.getElementById('stream-tables-container');
        if (!container) return;
        
        if (!streamTables || streamTables.length === 0) {
            container.innerHTML = `
                <div class="text-center text-muted py-3">
                    <i class="fas fa-info-circle"></i>
                    暂无流表数据
                </div>
            `;
            return;
        }
        
        let html = `
            <div class="table-responsive">
                <table class="table table-sm">
                    <thead>
                        <tr>
                            <th>表名</th>
                            <th>状态</th>
                            <th>记录数</th>
                            <th>最后更新</th>
                        </tr>
                    </thead>
                    <tbody>
        `;
        
        streamTables.forEach(table => {
            const statusClass = table.exists ? 'status-online' : 'status-offline';
            const statusText = table.exists ? '正常' : '不存在';
            
            html += `
                <tr>
                    <td><code>${table.name}</code></td>
                    <td>
                        <span class="status-indicator ${statusClass}"></span>
                        ${statusText}
                    </td>
                    <td>${formatNumber(table.row_count || 0, 0)}</td>
                    <td>${getRelativeTime(table.last_update)}</td>
                </tr>
            `;
        });
        
        html += `
                    </tbody>
                </table>
            </div>
        `;
        
        container.innerHTML = html;
    }
    
    changeTimeRange(range) {
        this.timeRange = range;
        
        // 更新按钮状态
        document.querySelectorAll('.time-range-btn').forEach(btn => {
            btn.classList.remove('active');
        });
        document.querySelector(`[onclick="monitoringController.changeTimeRange('${range}')"]`).classList.add('active');
        
        // 重新加载数据
        this.loadHistoricalData();
    }
    
    async loadHistoricalData() {
        try {
            const response = await apiManager.get('/system/metrics/history', {
                time_range: this.timeRange
            });
            
            if (response.success && response.data.length > 0) {
                this.chartData.timestamps = response.data.map(item => formatTime(item.timestamp));
                this.chartData.cpu = response.data.map(item => item.cpu_usage);
                this.chartData.memory = response.data.map(item => item.memory_usage);
                this.chartData.disk = response.data.map(item => item.disk_usage);
                
                if (this.systemChart) {
                    this.systemChart.data.labels = this.chartData.timestamps;
                    this.systemChart.data.datasets[0].data = this.chartData.cpu;
                    this.systemChart.data.datasets[1].data = this.chartData.memory;
                    this.systemChart.data.datasets[2].data = this.chartData.disk;
                    this.systemChart.update();
                }
            }
        } catch (error) {
            console.error('加载历史数据错误:', error);
        }
    }
    
    handleSystemMetricsUpdate(data) {
        this.updateSystemCards(data);
    }
    
    handleDolphinDBStatusUpdate(data) {
        this.updateDolphinDBStatus(data);
    }
    
    handleAlertTriggered(data) {
        const alertType = data.level === 'critical' ? 'danger' : 'warning';
        showNotification(`系统警报: ${data.message}`, alertType, 10000);
        
        // 可以在这里添加更多的警报处理逻辑
        console.warn('系统警报触发:', data);
    }
    
    updateConnectionStatus(isConnected) {
        const statusElement = document.querySelector('.monitoring-connection-status');
        if (statusElement) {
            statusElement.className = `status-indicator ${isConnected ? 'status-online' : 'status-offline'}`;
        }
    }
    
    setupEventListeners() {
        // 全局函数绑定
        window.refreshPageData = () => this.refreshData();
        
        // 时间范围切换按钮
        document.querySelectorAll('.time-range-btn').forEach(btn => {
            btn.addEventListener('click', (e) => {
                const range = e.target.dataset.range;
                if (range) {
                    this.changeTimeRange(range);
                }
            });
        });
    }
    
    refreshData() {
        this.loadSystemStatus();
    }
}

// 页面加载完成后初始化
document.addEventListener('DOMContentLoaded', function() {
    window.monitoringController = new MonitoringController();
});
