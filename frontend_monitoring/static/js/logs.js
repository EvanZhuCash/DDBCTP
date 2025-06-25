/**
 * 日志警报页面JavaScript模块
 * 处理实时日志流、过滤、统计图表、警报规则等功能
 */

class LogsController {
    constructor() {
        this.socket = null;
        this.logs = [];
        this.filteredLogs = [];
        this.logStats = {
            total: 0,
            info: 0,
            warning: 0,
            error: 0
        };
        this.filters = {
            level: '',
            component: '',
            keyword: ''
        };
        this.maxLogs = 1000;
        this.autoScroll = true;
        this.statsChart = null;
        this.trendChart = null;
        
        this.init();
    }
    
    init() {
        this.initWebSocket();
        this.initCharts();
        this.loadLogs();
        this.setupEventListeners();
        
        // 定期刷新统计数据
        setInterval(() => this.updateStatistics(), 10000);
    }
    
    initWebSocket() {
        this.socket = wsManager.connect('/logs', {
            onConnect: () => {
                console.log('日志WebSocket连接成功');
                this.updateConnectionStatus(true);
            },
            onDisconnect: () => {
                console.log('日志WebSocket连接断开');
                this.updateConnectionStatus(false);
            },
            onError: (error) => {
                console.error('日志WebSocket错误:', error);
                this.updateConnectionStatus(false);
            },
            new_log: (data) => {
                this.handleNewLog(data);
            },
            log_stats_update: (data) => {
                this.handleStatsUpdate(data);
            },
            alert_rule_triggered: (data) => {
                this.handleAlertRuleTriggered(data);
            }
        });
    }
    
    initCharts() {
        // 日志级别分布饼图
        const statsConfig = {
            type: 'doughnut',
            data: {
                labels: ['信息', '警告', '错误'],
                datasets: [{
                    data: [0, 0, 0],
                    backgroundColor: [
                        CONFIG.CHART_COLORS.INFO,
                        CONFIG.CHART_COLORS.WARNING,
                        CONFIG.CHART_COLORS.DANGER
                    ],
                    borderWidth: 2,
                    borderColor: '#fff'
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        position: 'bottom'
                    },
                    tooltip: {
                        callbacks: {
                            label: function(context) {
                                const total = context.dataset.data.reduce((a, b) => a + b, 0);
                                const percentage = total > 0 ? ((context.parsed / total) * 100).toFixed(1) : 0;
                                return `${context.label}: ${context.parsed} (${percentage}%)`;
                            }
                        }
                    }
                }
            }
        };
        
        this.statsChart = chartManager.createChart('log-stats-chart', statsConfig);
        
        // 日志趋势线图
        const trendConfig = {
            type: 'line',
            data: {
                labels: [],
                datasets: [{
                    label: '日志数量',
                    data: [],
                    borderColor: CONFIG.CHART_COLORS.PRIMARY,
                    backgroundColor: CONFIG.CHART_COLORS.PRIMARY + '20',
                    tension: 0.4,
                    fill: true
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
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
                            text: '日志数量'
                        },
                        beginAtZero: true
                    }
                },
                plugins: {
                    legend: {
                        display: false
                    }
                }
            }
        };
        
        this.trendChart = chartManager.createChart('log-trend-chart', trendConfig);
    }
    
    async loadLogs() {
        try {
            const response = await apiManager.get('/logs/recent', {
                limit: 100
            });
            
            if (response.success) {
                this.logs = response.data;
                this.applyFilters();
                this.updateStatistics();
            } else {
                showNotification('加载日志失败: ' + response.error, 'danger');
            }
        } catch (error) {
            console.error('加载日志错误:', error);
            showNotification('加载日志失败', 'danger');
        }
    }
    
    handleNewLog(logData) {
        // 添加新日志到列表开头
        this.logs.unshift(logData);
        
        // 限制日志数量
        if (this.logs.length > this.maxLogs) {
            this.logs = this.logs.slice(0, this.maxLogs);
        }
        
        // 更新统计
        this.updateLogStats(logData);
        
        // 重新应用过滤器
        this.applyFilters();
        
        // 如果启用自动滚动，滚动到顶部
        if (this.autoScroll) {
            const container = document.getElementById('log-stream-container');
            if (container) {
                container.scrollTop = 0;
            }
        }
    }
    
    updateLogStats(logData) {
        this.logStats.total++;
        
        switch (logData.level.toLowerCase()) {
            case 'info':
                this.logStats.info++;
                break;
            case 'warning':
                this.logStats.warning++;
                break;
            case 'error':
                this.logStats.error++;
                break;
        }
        
        // 更新统计卡片
        document.getElementById('total-logs').textContent = this.logStats.total;
        document.getElementById('info-logs').textContent = this.logStats.info;
        document.getElementById('warning-logs').textContent = this.logStats.warning;
        document.getElementById('error-logs').textContent = this.logStats.error;
        
        // 更新饼图
        if (this.statsChart) {
            this.statsChart.data.datasets[0].data = [
                this.logStats.info,
                this.logStats.warning,
                this.logStats.error
            ];
            this.statsChart.update('none');
        }
    }
    
    applyFilters() {
        this.filteredLogs = this.logs.filter(log => {
            // 级别过滤
            if (this.filters.level && log.level.toLowerCase() !== this.filters.level.toLowerCase()) {
                return false;
            }
            
            // 组件过滤
            if (this.filters.component && log.component.toLowerCase() !== this.filters.component.toLowerCase()) {
                return false;
            }
            
            // 关键词过滤
            if (this.filters.keyword) {
                const keyword = this.filters.keyword.toLowerCase();
                return log.message.toLowerCase().includes(keyword) ||
                       log.component.toLowerCase().includes(keyword);
            }
            
            return true;
        });
        
        this.renderLogs();
    }
    
    renderLogs() {
        const container = document.getElementById('log-stream-container');
        if (!container) return;
        
        if (this.filteredLogs.length === 0) {
            container.innerHTML = `
                <div class="text-center text-muted py-4">
                    <i class="fas fa-info-circle fa-2x mb-2"></i>
                    <p>暂无日志数据</p>
                </div>
            `;
            return;
        }
        
        let html = '';
        this.filteredLogs.forEach(log => {
            const levelClass = log.level.toLowerCase();
            const timeStr = formatTime(log.timestamp);
            const relativeTime = getRelativeTime(log.timestamp);
            
            html += `
                <div class="log-entry ${levelClass}" data-level="${log.level}" data-component="${log.component}">
                    <div class="d-flex justify-content-between align-items-start">
                        <div class="flex-grow-1">
                            <div class="d-flex align-items-center mb-1">
                                <span class="badge bg-${getStatusColor(levelClass)} me-2">${log.level}</span>
                                <span class="text-muted small">${log.component}</span>
                                <span class="text-muted small ms-auto" title="${timeStr}">${relativeTime}</span>
                            </div>
                            <div class="log-message">${this.highlightKeyword(log.message)}</div>
                        </div>
                    </div>
                </div>
            `;
        });
        
        container.innerHTML = html;
    }
    
    highlightKeyword(message) {
        if (!this.filters.keyword) return message;
        
        const keyword = this.filters.keyword;
        const regex = new RegExp(`(${keyword})`, 'gi');
        return message.replace(regex, '<mark>$1</mark>');
    }
    
    applyLogFilter() {
        this.filters.level = document.getElementById('log-level-filter').value;
        this.filters.component = document.getElementById('component-filter').value;
        this.filters.keyword = document.getElementById('search-keyword').value;
        
        this.applyFilters();
        
        showNotification(`已应用过滤器，显示 ${this.filteredLogs.length} 条日志`, 'info');
    }
    
    clearLogFilter() {
        this.filters = { level: '', component: '', keyword: '' };
        
        document.getElementById('log-level-filter').value = '';
        document.getElementById('component-filter').value = '';
        document.getElementById('search-keyword').value = '';
        
        this.applyFilters();
        
        showNotification('已清除所有过滤器', 'info');
    }
    
    toggleAutoScroll() {
        this.autoScroll = !this.autoScroll;
        
        const button = document.getElementById('auto-scroll-btn');
        if (button) {
            button.innerHTML = this.autoScroll ? 
                '<i class="fas fa-pause"></i> 暂停滚动' : 
                '<i class="fas fa-play"></i> 自动滚动';
            button.className = `btn btn-sm ${this.autoScroll ? 'btn-warning' : 'btn-success'}`;
        }
        
        showNotification(`自动滚动已${this.autoScroll ? '启用' : '禁用'}`, 'info');
    }
    
    exportLogs() {
        const dataStr = JSON.stringify(this.filteredLogs, null, 2);
        const dataBlob = new Blob([dataStr], {type: 'application/json'});
        
        const link = document.createElement('a');
        link.href = URL.createObjectURL(dataBlob);
        link.download = `logs_${new Date().toISOString().slice(0, 10)}.json`;
        link.click();
        
        showNotification('日志已导出', 'success');
    }
    
    handleStatsUpdate(data) {
        this.logStats = data;
        
        // 更新统计卡片
        document.getElementById('total-logs').textContent = data.total;
        document.getElementById('info-logs').textContent = data.info;
        document.getElementById('warning-logs').textContent = data.warning;
        document.getElementById('error-logs').textContent = data.error;
        
        // 更新图表
        if (this.statsChart) {
            this.statsChart.data.datasets[0].data = [data.info, data.warning, data.error];
            this.statsChart.update('none');
        }
    }
    
    handleAlertRuleTriggered(data) {
        showNotification(`日志警报触发: ${data.rule_name} - ${data.message}`, 'warning', 10000);
        
        // 可以在这里添加更多的警报处理逻辑
        console.warn('日志警报触发:', data);
    }
    
    updateStatistics() {
        // 重新计算统计数据
        this.logStats = {
            total: this.logs.length,
            info: this.logs.filter(log => log.level.toLowerCase() === 'info').length,
            warning: this.logs.filter(log => log.level.toLowerCase() === 'warning').length,
            error: this.logs.filter(log => log.level.toLowerCase() === 'error').length
        };
        
        this.handleStatsUpdate(this.logStats);
    }
    
    updateConnectionStatus(isConnected) {
        const statusElement = document.querySelector('.logs-connection-status');
        if (statusElement) {
            statusElement.className = `status-indicator ${isConnected ? 'status-online' : 'status-offline'}`;
        }
    }
    
    setupEventListeners() {
        // 全局函数绑定
        window.applyLogFilter = () => this.applyLogFilter();
        window.clearLogFilter = () => this.clearLogFilter();
        window.toggleAutoScroll = () => this.toggleAutoScroll();
        window.exportLogs = () => this.exportLogs();
        window.refreshPageData = () => this.loadLogs();
        
        // 搜索框回车事件
        const searchInput = document.getElementById('search-keyword');
        if (searchInput) {
            searchInput.addEventListener('keypress', (e) => {
                if (e.key === 'Enter') {
                    this.applyLogFilter();
                }
            });
        }
        
        // 过滤器变化事件
        ['log-level-filter', 'component-filter'].forEach(id => {
            const element = document.getElementById(id);
            if (element) {
                element.addEventListener('change', () => this.applyLogFilter());
            }
        });
    }
}

// 页面加载完成后初始化
document.addEventListener('DOMContentLoaded', function() {
    window.logsController = new LogsController();
});
