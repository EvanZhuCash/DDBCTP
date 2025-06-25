/**
 * 策略控制页面JavaScript模块
 * 处理策略矩阵、实时性能监控、WebSocket通信等功能
 */

class StrategyController {
    constructor() {
        this.socket = null;
        this.strategyMatrix = [];
        this.performanceChart = null;
        this.performanceData = [];
        this.isConnected = false;
        
        this.init();
    }
    
    init() {
        this.initWebSocket();
        this.initPerformanceChart();
        this.initSpreadChart();
        this.loadStrategyMatrix();
        this.setupEventListeners();

        // 定期刷新数据
        setInterval(() => this.refreshData(), 30000);

        // 更频繁地刷新tick数据
        setInterval(() => this.loadLiveTickData(), 1000);
    }
    
    initWebSocket() {
        this.socket = wsManager.connect('/strategy', {
            onConnect: () => {
                console.log('策略WebSocket连接成功');
                this.isConnected = true;
                this.updateConnectionStatus(true);
            },
            onDisconnect: () => {
                console.log('策略WebSocket连接断开');
                this.isConnected = false;
                this.updateConnectionStatus(false);
            },
            onError: (error) => {
                console.error('策略WebSocket错误:', error);
                this.updateConnectionStatus(false);
            },
            strategy_matrix_update: (data) => {
                this.handleStrategyMatrixUpdate(data);
            },
            strategy_performance_update: (data) => {
                this.handlePerformanceUpdate(data);
            },
            strategy_status_change: (data) => {
                this.handleStatusChange(data);
            }
        });
    }
    
    initPerformanceChart() {
        const config = {
            type: 'line',
            data: {
                labels: [],
                datasets: [{
                    label: '总PnL',
                    data: [],
                    borderColor: CONFIG.CHART_COLORS.PRIMARY,
                    backgroundColor: CONFIG.CHART_COLORS.PRIMARY + '20',
                    tension: 0.4,
                    fill: true
                }, {
                    label: '夏普比率',
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
                            text: 'PnL'
                        }
                    },
                    y1: {
                        type: 'linear',
                        display: true,
                        position: 'right',
                        title: {
                            display: true,
                            text: '夏普比率'
                        },
                        grid: {
                            drawOnChartArea: false,
                        },
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
                                if (context.parsed.y !== null) {
                                    if (context.datasetIndex === 0) {
                                        label += formatNumber(context.parsed.y);
                                    } else {
                                        label += formatNumber(context.parsed.y, 3);
                                    }
                                }
                                return label;
                            }
                        }
                    }
                }
            }
        };
        
        this.performanceChart = chartManager.createChart('strategy-performance-chart', config);
    }

    initSpreadChart() {
        const config = {
            type: 'line',
            data: {
                labels: [],
                datasets: [{
                    label: '价差',
                    data: [],
                    borderColor: CONFIG.CHART_COLORS.WARNING,
                    backgroundColor: CONFIG.CHART_COLORS.WARNING + '20',
                    tension: 0.4,
                    fill: true,
                    pointRadius: 2
                }, {
                    label: '价差百分比',
                    data: [],
                    borderColor: CONFIG.CHART_COLORS.ACCENT,
                    backgroundColor: CONFIG.CHART_COLORS.ACCENT + '20',
                    tension: 0.4,
                    yAxisID: 'y1',
                    pointRadius: 2
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
                            text: '价差'
                        }
                    },
                    y1: {
                        type: 'linear',
                        display: true,
                        position: 'right',
                        title: {
                            display: true,
                            text: '价差百分比 (%)'
                        },
                        grid: {
                            drawOnChartArea: false,
                        },
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
                                    label += formatNumber(context.parsed.y, 2);
                                } else {
                                    label += formatNumber(context.parsed.y, 3) + '%';
                                }
                                return label;
                            }
                        }
                    }
                }
            }
        };

        this.spreadChart = chartManager.createChart('spread-chart', config);
    }
    
    async loadStrategyMatrix() {
        try {
            const response = await apiManager.get('/strategy/matrix');
            if (response.success) {
                this.strategyMatrix = response.data;
                this.renderStrategyMatrix();
                this.updateStatistics();
            } else {
                showNotification('加载策略矩阵失败: ' + response.error, 'danger');
            }
        } catch (error) {
            console.error('加载策略矩阵错误:', error);
            showNotification('加载策略矩阵失败', 'danger');
        }
    }
    
    renderStrategyMatrix() {
        const container = document.getElementById('strategy-matrix-container');
        if (!container) return;
        
        if (this.strategyMatrix.length === 0) {
            container.innerHTML = `
                <div class="text-center text-muted py-4">
                    <i class="fas fa-info-circle fa-2x mb-2"></i>
                    <p>暂无策略数据</p>
                </div>
            `;
            return;
        }
        
        // 获取所有策略和品种
        const strategies = [...new Set(this.strategyMatrix.map(item => item.strategy_name))];
        const symbols = [...new Set(this.strategyMatrix.map(item => item.symbol))];
        
        let html = `
            <div class="table-responsive">
                <table class="table table-hover">
                    <thead>
                        <tr>
                            <th>策略\\品种</th>
                            ${symbols.map(symbol => `<th class="text-center">${symbol}</th>`).join('')}
                        </tr>
                    </thead>
                    <tbody>
        `;
        
        strategies.forEach(strategy => {
            html += `<tr><td class="fw-bold">${strategy}</td>`;
            symbols.forEach(symbol => {
                const item = this.strategyMatrix.find(m => 
                    m.strategy_name === strategy && m.symbol === symbol
                );
                
                if (item) {
                    const isEnabled = item.enabled;
                    const toggleClass = isEnabled ? 'enabled' : 'disabled';
                    const toggleText = isEnabled ? 'ON' : 'OFF';
                    
                    html += `
                        <td class="text-center">
                            <button class="strategy-toggle ${toggleClass}" 
                                    onclick="strategyController.toggleStrategy('${strategy}', '${symbol}')"
                                    data-strategy="${strategy}" 
                                    data-symbol="${symbol}">
                                ${toggleText}
                            </button>
                        </td>
                    `;
                } else {
                    html += '<td class="text-center text-muted">-</td>';
                }
            });
            html += '</tr>';
        });
        
        html += `
                    </tbody>
                </table>
            </div>
        `;
        
        container.innerHTML = html;
    }
    
    async toggleStrategy(strategyName, symbol) {
        try {
            const response = await apiManager.post('/strategy/toggle', {
                strategy_name: strategyName,
                symbol: symbol
            });
            
            if (response.success) {
                showNotification(`策略 ${strategyName}-${symbol} 状态已更新`, 'success');
                // 更新本地数据
                const item = this.strategyMatrix.find(m => 
                    m.strategy_name === strategyName && m.symbol === symbol
                );
                if (item) {
                    item.enabled = !item.enabled;
                    this.renderStrategyMatrix();
                    this.updateStatistics();
                }
            } else {
                showNotification('更新策略状态失败: ' + response.error, 'danger');
            }
        } catch (error) {
            console.error('切换策略状态错误:', error);
            showNotification('更新策略状态失败', 'danger');
        }
    }
    
    async enableAllStrategies() {
        if (!confirm('确定要启用所有策略吗？')) return;
        
        try {
            const response = await apiManager.post('/strategy/enable_all');
            if (response.success) {
                showNotification('所有策略已启用', 'success');
                this.loadStrategyMatrix();
            } else {
                showNotification('启用所有策略失败: ' + response.error, 'danger');
            }
        } catch (error) {
            console.error('启用所有策略错误:', error);
            showNotification('启用所有策略失败', 'danger');
        }
    }
    
    async disableAllStrategies() {
        if (!confirm('确定要停用所有策略吗？')) return;
        
        try {
            const response = await apiManager.post('/strategy/disable_all');
            if (response.success) {
                showNotification('所有策略已停用', 'success');
                this.loadStrategyMatrix();
            } else {
                showNotification('停用所有策略失败: ' + response.error, 'danger');
            }
        } catch (error) {
            console.error('停用所有策略错误:', error);
            showNotification('停用所有策略失败', 'danger');
        }
    }
    
    updateStatistics() {
        const totalStrategies = this.strategyMatrix.length;
        const activeStrategies = this.strategyMatrix.filter(item => item.enabled).length;
        const totalSymbols = [...new Set(this.strategyMatrix.map(item => item.symbol))].length;
        
        document.getElementById('total-strategies').textContent = totalStrategies;
        document.getElementById('active-strategies-count').textContent = activeStrategies;
        document.getElementById('total-symbols').textContent = totalSymbols;
        document.getElementById('active-subscriptions').textContent = activeStrategies;
    }
    
    handleStrategyMatrixUpdate(data) {
        this.strategyMatrix = data;
        this.renderStrategyMatrix();
        this.updateStatistics();
    }
    
    handlePerformanceUpdate(data) {
        if (this.performanceChart && data.length > 0) {
            const labels = data.map(item => formatTime(item.timestamp));
            const pnlData = data.map(item => item.total_pnl);
            const sharpeData = data.map(item => item.sharpe_ratio);
            
            this.performanceChart.data.labels = labels;
            this.performanceChart.data.datasets[0].data = pnlData;
            this.performanceChart.data.datasets[1].data = sharpeData;
            this.performanceChart.update('none');
        }
    }
    
    handleStatusChange(data) {
        showNotification(`策略状态变更: ${data.strategy_name}-${data.symbol} ${data.enabled ? '启用' : '停用'}`, 'info');
        this.loadStrategyMatrix();
    }
    
    updateConnectionStatus(isConnected) {
        const statusElement = document.querySelector('.strategy-connection-status');
        if (statusElement) {
            statusElement.className = `status-indicator ${isConnected ? 'status-online' : 'status-offline'}`;
        }
    }
    
    setupEventListeners() {
        // 全局函数绑定
        window.enableAllStrategies = () => this.enableAllStrategies();
        window.disableAllStrategies = () => this.disableAllStrategies();
        window.refreshStrategyMatrix = () => this.loadStrategyMatrix();
        window.refreshPageData = () => this.refreshData();
    }
    
    refreshData() {
        this.loadStrategyMatrix();
        this.loadPerformanceData();
        this.loadRecentSignals();
        this.loadCurrentPositions();
        this.loadLiveTickData();
        this.loadSpreadStats();
    }

    async loadLiveTickData() {
        try {
            const response = await apiManager.get('/market/live_ticks');
            if (response.success && response.data.length > 0) {
                this.displayLiveTickData(response.data);
            }
        } catch (error) {
            console.error('加载实时tick数据错误:', error);
        }
    }

    async loadSpreadStats() {
        try {
            const response = await apiManager.get('/market/spread_stats');
            if (response.success && response.data.length > 0) {
                this.updateSpreadChart(response.data);
            }
        } catch (error) {
            console.error('加载价差统计错误:', error);
        }
    }

    displayLiveTickData(ticks) {
        if (ticks.length === 0) return;

        // 更新最新价格显示
        const latestTick = ticks[0];
        document.getElementById('last-price').textContent = formatNumber(latestTick.last_price, 2);
        document.getElementById('bid-price').textContent = formatNumber(latestTick.bid_price, 2);
        document.getElementById('ask-price').textContent = formatNumber(latestTick.ask_price, 2);
        document.getElementById('bid-volume').textContent = latestTick.bid_volume;
        document.getElementById('ask-volume').textContent = latestTick.ask_volume;
        document.getElementById('spread-value').textContent = formatNumber(latestTick.spread, 2);

        const spreadPct = (latestTick.spread / latestTick.last_price * 100).toFixed(3);
        document.getElementById('spread-pct').textContent = spreadPct + '%';

        // 更新tick流显示
        this.updateTickStream(ticks);
    }

    updateTickStream(ticks) {
        const container = document.getElementById('tick-stream');
        if (!container) return;

        let html = '';
        ticks.slice(0, 15).forEach(tick => {
            const timeStr = new Date(tick.timestamp).toLocaleTimeString();
            const spreadPct = (tick.spread / tick.last_price * 100).toFixed(3);

            html += `
                <div class="tick-line mb-1">
                    <span class="text-muted">${timeStr}</span>
                    <strong class="text-primary">${formatNumber(tick.last_price, 2)}</strong>
                    <span class="text-success">${formatNumber(tick.bid_price, 2)}(${tick.bid_volume})</span>
                    <span class="text-danger">${formatNumber(tick.ask_price, 2)}(${tick.ask_volume})</span>
                    <span class="text-warning">±${formatNumber(tick.spread, 2)}(${spreadPct}%)</span>
                    <span class="text-info">Vol:${tick.volume}</span>
                </div>
            `;
        });

        container.innerHTML = html;

        // 自动滚动到顶部显示最新数据
        container.scrollTop = 0;
    }

    updateSpreadChart(spreadData) {
        if (!this.spreadChart || spreadData.length === 0) return;

        const labels = spreadData.map(item => formatTime(item.timestamp));
        const spreads = spreadData.map(item => item.spread);
        const spreadPcts = spreadData.map(item => item.spread_pct);

        this.spreadChart.data.labels = labels.slice(-20); // 只显示最近20个点
        this.spreadChart.data.datasets[0].data = spreads.slice(-20);
        this.spreadChart.data.datasets[1].data = spreadPcts.slice(-20);
        this.spreadChart.update('none');
    }

    async loadRecentSignals() {
        try {
            const response = await apiManager.get('/strategy/signals');
            if (response.success && response.data.length > 0) {
                this.displayRecentSignals(response.data);
            }
        } catch (error) {
            console.error('加载信号数据错误:', error);
        }
    }

    async loadCurrentPositions() {
        try {
            const response = await apiManager.get('/strategy/positions');
            if (response.success) {
                this.displayCurrentPositions(response.data);
            }
        } catch (error) {
            console.error('加载持仓数据错误:', error);
        }
    }

    displayRecentSignals(signals) {
        const container = document.getElementById('recent-signals-container');
        if (!container) return;

        if (signals.length === 0) {
            container.innerHTML = '<div class="text-muted text-center py-3">暂无信号数据</div>';
            return;
        }

        let html = '<div class="list-group list-group-flush">';
        signals.slice(0, 10).forEach(signal => {
            const signalClass = this.getSignalClass(signal.signal);
            const timeStr = getRelativeTime(signal.timestamp);

            html += `
                <div class="list-group-item d-flex justify-content-between align-items-center">
                    <div>
                        <span class="badge bg-${signalClass} me-2">${signal.signal}</span>
                        <strong>${signal.symbol}</strong>
                        <small class="text-muted ms-2">@ ${formatNumber(signal.price)}</small>
                    </div>
                    <small class="text-muted">${timeStr}</small>
                </div>
            `;
        });
        html += '</div>';

        container.innerHTML = html;
    }

    displayCurrentPositions(positions) {
        const container = document.getElementById('current-positions-container');
        if (!container) return;

        if (positions.length === 0) {
            container.innerHTML = '<div class="text-muted text-center py-3">暂无持仓</div>';
            return;
        }

        let html = '<div class="table-responsive"><table class="table table-sm">';
        html += '<thead><tr><th>品种</th><th>方向</th><th>数量</th><th>价格</th></tr></thead><tbody>';

        positions.forEach(position => {
            const directionClass = position.direction === 'LONG' ? 'text-success' : 'text-danger';
            const directionIcon = position.direction === 'LONG' ? 'fa-arrow-up' : 'fa-arrow-down';

            html += `
                <tr>
                    <td><strong>${position.symbol}</strong></td>
                    <td><i class="fas ${directionIcon} ${directionClass}"></i> ${position.direction}</td>
                    <td>${position.qty}</td>
                    <td>${formatNumber(position.price)}</td>
                </tr>
            `;
        });

        html += '</tbody></table></div>';
        container.innerHTML = html;
    }

    getSignalClass(signal) {
        switch (signal) {
            case 'OPEN_LONG':
            case 'CLOSE_SHORT':
                return 'success';
            case 'OPEN_SHORT':
            case 'CLOSE_LONG':
                return 'danger';
            case 'NO_TRADE':
                return 'secondary';
            default:
                return 'info';
        }
    }
    
    async loadPerformanceData() {
        try {
            const response = await apiManager.get('/strategy/performance');
            if (response.success) {
                this.handlePerformanceUpdate(response.data);
            }
        } catch (error) {
            console.error('加载性能数据错误:', error);
        }
    }
}

// 页面加载完成后初始化
document.addEventListener('DOMContentLoaded', function() {
    window.strategyController = new StrategyController();
});
