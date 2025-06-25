/**
 * 总览页面JavaScript模块
 * 处理系统概览、关键指标、快速操作等功能
 */

class DashboardController {
    constructor() {
        this.socket = null;
        this.overviewChart = null;
        this.systemHealth = {};
        this.quickStats = {};
        this.refreshInterval = null;
        
        this.init();
    }
    
    init() {
        this.initWebSocket();
        this.initOverviewChart();
        this.loadDashboardData();
        this.setupEventListeners();
        
        // 定期刷新数据
        this.refreshInterval = setInterval(() => this.refreshData(), 15000);
    }
    
    initWebSocket() {
        this.socket = wsManager.connect('/dashboard', {
            onConnect: () => {
                console.log('总览WebSocket连接成功');
                this.updateConnectionStatus(true);
            },
            onDisconnect: () => {
                console.log('总览WebSocket连接断开');
                this.updateConnectionStatus(false);
            },
            onError: (error) => {
                console.error('总览WebSocket错误:', error);
                this.updateConnectionStatus(false);
            },
            dashboard_update: (data) => {
                this.handleDashboardUpdate(data);
            },
            system_alert: (data) => {
                this.handleSystemAlert(data);
            },
            quick_stats_update: (data) => {
                this.handleQuickStatsUpdate(data);
            }
        });
    }
    
    initOverviewChart() {
        const config = {
            type: 'line',
            data: {
                labels: [],
                datasets: [{
                    label: '系统负载',
                    data: [],
                    borderColor: CONFIG.CHART_COLORS.PRIMARY,
                    backgroundColor: CONFIG.CHART_COLORS.PRIMARY + '20',
                    tension: 0.4,
                    fill: true
                }, {
                    label: '活跃策略',
                    data: [],
                    borderColor: CONFIG.CHART_COLORS.SUCCESS,
                    backgroundColor: CONFIG.CHART_COLORS.SUCCESS + '20',
                    tension: 0.4,
                    yAxisID: 'y1'
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
                        type: 'linear',
                        display: true,
                        position: 'left',
                        title: {
                            display: true,
                            text: '系统负载 (%)'
                        },
                        min: 0,
                        max: 100
                    },
                    y1: {
                        type: 'linear',
                        display: true,
                        position: 'right',
                        title: {
                            display: true,
                            text: '活跃策略数'
                        },
                        grid: {
                            drawOnChartArea: false,
                        },
                        min: 0
                    }
                },
                plugins: {
                    legend: {
                        position: 'top',
                    },
                    tooltip: {
                        callbacks: {
                            label: function(context) {
                                let label = context.dataset.label || '';
                                if (label) {
                                    label += ': ';
                                }
                                if (context.datasetIndex === 0) {
                                    label += formatPercent(context.parsed.y);
                                } else {
                                    label += context.parsed.y;
                                }
                                return label;
                            }
                        }
                    }
                }
            }
        };
        
        this.overviewChart = chartManager.createChart('overview-chart', config);
    }
    
    async loadDashboardData() {
        try {
            const response = await apiManager.get('/dashboard/overview');
            if (response.success) {
                this.updateSystemCards(response.data.system);
                this.updateStrategyOverview(response.data.strategies);
                this.updateStreamTablesStatus(response.data.stream_tables);
                this.updateRecentActivity(response.data.recent_activity);
                this.updatePerformanceChart(response.data.performance);
            } else {
                showNotification('加载总览数据失败: ' + response.error, 'danger');
            }
        } catch (error) {
            console.error('加载总览数据错误:', error);
            showNotification('加载总览数据失败', 'danger');
        }
    }
    
    updateSystemCards(systemData) {
        // 更新DolphinDB状态
        const ddbStatus = document.getElementById('ddb-status');
        if (ddbStatus && systemData.dolphindb) {
            const isConnected = systemData.dolphindb.connected;
            const statusClass = isConnected ? 'text-success' : 'text-danger';
            const statusIcon = isConnected ? 'fa-check-circle' : 'fa-times-circle';
            const statusText = isConnected ? '在线' : '离线';
            
            ddbStatus.innerHTML = `
                <i class="fas ${statusIcon} ${statusClass}"></i>
                <span class="ms-1">${statusText}</span>
            `;
        }
        
        // 更新系统负载
        const systemLoad = document.getElementById('system-load');
        if (systemLoad && systemData.load) {
            const loadPercent = systemData.load.cpu || 0;
            const loadClass = loadPercent > 80 ? 'text-danger' : loadPercent > 60 ? 'text-warning' : 'text-success';
            
            systemLoad.innerHTML = `
                <span class="${loadClass}">${formatPercent(loadPercent)}</span>
            `;
        }
        
        // 更新内存使用
        const memoryUsage = document.getElementById('memory-usage');
        if (memoryUsage && systemData.memory) {
            const memPercent = systemData.memory.percent || 0;
            const memClass = memPercent > 80 ? 'text-danger' : memPercent > 60 ? 'text-warning' : 'text-success';
            
            memoryUsage.innerHTML = `
                <span class="${memClass}">${formatPercent(memPercent)}</span>
            `;
        }
        
        // 更新磁盘使用
        const diskUsage = document.getElementById('disk-usage');
        if (diskUsage && systemData.disk) {
            const diskPercent = systemData.disk.percent || 0;
            const diskClass = diskPercent > 80 ? 'text-danger' : diskPercent > 60 ? 'text-warning' : 'text-success';
            
            diskUsage.innerHTML = `
                <span class="${diskClass}">${formatPercent(diskPercent)}</span>
            `;
        }
    }
    
    updateStrategyOverview(strategiesData) {
        // 更新活跃策略数
        const activeStrategies = document.getElementById('active-strategies');
        if (activeStrategies && strategiesData) {
            activeStrategies.textContent = strategiesData.active_count || 0;
        }
        
        // 更新总PnL
        const totalPnl = document.getElementById('total-pnl');
        if (totalPnl && strategiesData) {
            const pnl = strategiesData.total_pnl || 0;
            const pnlClass = pnl >= 0 ? 'text-success' : 'text-danger';
            const pnlSign = pnl >= 0 ? '+' : '';
            
            totalPnl.innerHTML = `
                <span class="${pnlClass}">${pnlSign}${formatNumber(pnl)}</span>
            `;
        }
        
        // 更新今日交易
        const todayTrades = document.getElementById('today-trades');
        if (todayTrades && strategiesData) {
            todayTrades.textContent = strategiesData.today_trades || 0;
        }
    }
    
    updateStreamTablesStatus(streamTables) {
        const container = document.getElementById('stream-tables-status');
        if (!container) return;
        
        if (!streamTables || streamTables.length === 0) {
            container.innerHTML = `
                <div class="text-center text-muted py-2">
                    <small>暂无流表数据</small>
                </div>
            `;
            return;
        }
        
        let html = '<div class="row g-2">';
        
        streamTables.slice(0, 6).forEach(table => { // 只显示前6个
            const statusClass = table.exists ? 'success' : 'danger';
            const statusIcon = table.exists ? 'check-circle' : 'times-circle';
            
            html += `
                <div class="col-6">
                    <div class="d-flex align-items-center">
                        <i class="fas fa-${statusIcon} text-${statusClass} me-2"></i>
                        <small class="text-truncate" title="${table.name}">${table.name}</small>
                    </div>
                </div>
            `;
        });
        
        html += '</div>';
        
        if (streamTables.length > 6) {
            html += `<div class="text-center mt-2"><small class="text-muted">还有 ${streamTables.length - 6} 个流表...</small></div>`;
        }
        
        container.innerHTML = html;
    }
    
    updateRecentActivity(activities) {
        const container = document.getElementById('recent-activity');
        if (!container) return;
        
        if (!activities || activities.length === 0) {
            container.innerHTML = `
                <div class="text-center text-muted py-3">
                    <small>暂无最近活动</small>
                </div>
            `;
            return;
        }
        
        let html = '';
        activities.slice(0, 5).forEach(activity => { // 只显示最近5条
            const timeStr = getRelativeTime(activity.timestamp);
            const iconClass = this.getActivityIcon(activity.type);
            const colorClass = this.getActivityColor(activity.type);
            
            html += `
                <div class="d-flex align-items-start mb-2">
                    <i class="fas ${iconClass} ${colorClass} me-2 mt-1"></i>
                    <div class="flex-grow-1">
                        <div class="small">${activity.message}</div>
                        <div class="text-muted" style="font-size: 0.75rem;">${timeStr}</div>
                    </div>
                </div>
            `;
        });
        
        container.innerHTML = html;
    }
    
    updatePerformanceChart(performanceData) {
        if (!this.overviewChart || !performanceData || performanceData.length === 0) return;
        
        const labels = performanceData.map(item => formatTime(item.timestamp));
        const systemLoad = performanceData.map(item => item.system_load || 0);
        const activeStrategies = performanceData.map(item => item.active_strategies || 0);
        
        this.overviewChart.data.labels = labels;
        this.overviewChart.data.datasets[0].data = systemLoad;
        this.overviewChart.data.datasets[1].data = activeStrategies;
        this.overviewChart.update('none');
    }
    
    getActivityIcon(type) {
        const iconMap = {
            'strategy': 'fa-cogs',
            'system': 'fa-server',
            'alert': 'fa-exclamation-triangle',
            'trade': 'fa-exchange-alt',
            'error': 'fa-times-circle',
            'info': 'fa-info-circle'
        };
        return iconMap[type] || 'fa-circle';
    }
    
    getActivityColor(type) {
        const colorMap = {
            'strategy': 'text-primary',
            'system': 'text-info',
            'alert': 'text-warning',
            'trade': 'text-success',
            'error': 'text-danger',
            'info': 'text-muted'
        };
        return colorMap[type] || 'text-muted';
    }
    
    // 快速操作函数
    quickRestartStrategies() {
        if (!confirm('确定要重启所有策略吗？')) return;
        
        apiManager.post('/strategy/restart_all')
            .then(response => {
                if (response.success) {
                    showNotification('所有策略重启成功', 'success');
                    this.refreshData();
                } else {
                    showNotification('重启策略失败: ' + response.error, 'danger');
                }
            })
            .catch(error => {
                console.error('重启策略错误:', error);
                showNotification('重启策略失败', 'danger');
            });
    }
    
    quickSystemCheck() {
        showNotification('正在执行系统检查...', 'info');
        
        apiManager.post('/system/health_check')
            .then(response => {
                if (response.success) {
                    const issues = response.data.issues || [];
                    if (issues.length === 0) {
                        showNotification('系统检查完成，一切正常', 'success');
                    } else {
                        showNotification(`系统检查完成，发现 ${issues.length} 个问题`, 'warning');
                    }
                } else {
                    showNotification('系统检查失败: ' + response.error, 'danger');
                }
            })
            .catch(error => {
                console.error('系统检查错误:', error);
                showNotification('系统检查失败', 'danger');
            });
    }
    
    handleDashboardUpdate(data) {
        this.updateSystemCards(data.system);
        this.updateStrategyOverview(data.strategies);
        if (data.performance) {
            this.updatePerformanceChart(data.performance);
        }
    }
    
    handleSystemAlert(data) {
        const alertType = data.level === 'critical' ? 'danger' : 'warning';
        showNotification(`系统警报: ${data.message}`, alertType, 10000);
    }
    
    handleQuickStatsUpdate(data) {
        this.quickStats = data;
        // 可以在这里更新快速统计显示
    }
    
    updateConnectionStatus(isConnected) {
        const statusElement = document.querySelector('.dashboard-connection-status');
        if (statusElement) {
            statusElement.className = `status-indicator ${isConnected ? 'status-online' : 'status-offline'}`;
        }
    }
    
    setupEventListeners() {
        // 全局函数绑定
        window.quickRestartStrategies = () => this.quickRestartStrategies();
        window.quickSystemCheck = () => this.quickSystemCheck();
        window.refreshPageData = () => this.refreshData();
        
        // 快速导航按钮
        const quickNavButtons = document.querySelectorAll('.quick-nav-btn');
        quickNavButtons.forEach(btn => {
            btn.addEventListener('click', (e) => {
                const target = e.target.dataset.target;
                if (target) {
                    window.location.href = target;
                }
            });
        });
    }
    
    refreshData() {
        this.loadDashboardData();
    }
    
    destroy() {
        if (this.refreshInterval) {
            clearInterval(this.refreshInterval);
        }
        if (this.socket) {
            wsManager.disconnect('/dashboard');
        }
        if (this.overviewChart) {
            chartManager.destroyChart('overview-chart');
        }
    }
}

// 页面加载完成后初始化
document.addEventListener('DOMContentLoaded', function() {
    window.dashboardController = new DashboardController();
});

// 页面卸载时清理资源
window.addEventListener('beforeunload', function() {
    if (window.dashboardController) {
        window.dashboardController.destroy();
    }
});
