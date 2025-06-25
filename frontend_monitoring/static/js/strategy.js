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
        this.loadStrategyMatrix();
        this.setupEventListeners();
        
        // 定期刷新数据
        setInterval(() => this.refreshData(), 30000);
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
