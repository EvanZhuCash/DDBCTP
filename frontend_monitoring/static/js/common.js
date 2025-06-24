/**
 * DolphinDB CTP 前端监控系统通用JavaScript函数
 */

// 全局配置
const CONFIG = {
    API_BASE_URL: '/api',
    WEBSOCKET_NAMESPACE: {
        STRATEGY: '/strategy',
        MONITOR: '/monitor'
    },
    REFRESH_INTERVALS: {
        FAST: 5000,    // 5秒
        NORMAL: 30000, // 30秒
        SLOW: 60000    // 60秒
    },
    CHART_COLORS: {
        PRIMARY: 'rgba(102, 126, 234, 1)',
        SUCCESS: 'rgba(40, 167, 69, 1)',
        WARNING: 'rgba(255, 193, 7, 1)',
        DANGER: 'rgba(220, 53, 69, 1)',
        INFO: 'rgba(23, 162, 184, 1)'
    }
};

// 工具函数类
class Utils {
    /**
     * 格式化数字
     * @param {number} num 数字
     * @param {number} decimals 小数位数
     * @returns {string} 格式化后的字符串
     */
    static formatNumber(num, decimals = 2) {
        if (num === null || num === undefined || isNaN(num)) return 'N/A';
        return Number(num).toLocaleString('zh-CN', {
            minimumFractionDigits: decimals,
            maximumFractionDigits: decimals
        });
    }

    /**
     * 格式化百分比
     * @param {number} num 数字
     * @param {number} decimals 小数位数
     * @returns {string} 格式化后的百分比字符串
     */
    static formatPercent(num, decimals = 1) {
        if (num === null || num === undefined || isNaN(num)) return 'N/A';
        return Number(num).toFixed(decimals) + '%';
    }

    /**
     * 格式化文件大小
     * @param {number} bytes 字节数
     * @returns {string} 格式化后的文件大小
     */
    static formatFileSize(bytes) {
        if (bytes === 0) return '0 B';
        const k = 1024;
        const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
    }

    /**
     * 格式化时间
     * @param {string|Date} timestamp 时间戳
     * @returns {string} 格式化后的时间字符串
     */
    static formatTime(timestamp) {
        if (!timestamp) return 'N/A';
        const date = new Date(timestamp);
        return date.toLocaleString('zh-CN');
    }

    /**
     * 获取相对时间
     * @param {string|Date} timestamp 时间戳
     * @returns {string} 相对时间字符串
     */
    static getRelativeTime(timestamp) {
        if (!timestamp) return 'N/A';
        
        const now = new Date();
        const date = new Date(timestamp);
        const diffMs = now - date;
        const diffSecs = Math.floor(diffMs / 1000);
        const diffMins = Math.floor(diffSecs / 60);
        const diffHours = Math.floor(diffMins / 60);
        const diffDays = Math.floor(diffHours / 24);

        if (diffSecs < 60) return '刚刚';
        if (diffMins < 60) return `${diffMins}分钟前`;
        if (diffHours < 24) return `${diffHours}小时前`;
        if (diffDays < 7) return `${diffDays}天前`;
        
        return date.toLocaleDateString('zh-CN');
    }

    /**
     * 获取状态颜色
     * @param {string} status 状态
     * @returns {string} 颜色类名
     */
    static getStatusColor(status) {
        const statusMap = {
            'healthy': 'success',
            'online': 'success',
            'active': 'success',
            'enabled': 'success',
            'warning': 'warning',
            'error': 'danger',
            'critical': 'danger',
            'offline': 'danger',
            'disabled': 'secondary',
            'unknown': 'secondary'
        };
        
        return statusMap[status.toLowerCase()] || 'secondary';
    }

    /**
     * 获取状态图标
     * @param {string} status 状态
     * @returns {string} 图标类名
     */
    static getStatusIcon(status) {
        const iconMap = {
            'healthy': 'fas fa-check-circle',
            'online': 'fas fa-check-circle',
            'active': 'fas fa-check-circle',
            'enabled': 'fas fa-check-circle',
            'warning': 'fas fa-exclamation-triangle',
            'error': 'fas fa-times-circle',
            'critical': 'fas fa-times-circle',
            'offline': 'fas fa-times-circle',
            'disabled': 'fas fa-minus-circle',
            'unknown': 'fas fa-question-circle'
        };
        
        return iconMap[status.toLowerCase()] || 'fas fa-question-circle';
    }

    /**
     * 防抖函数
     * @param {Function} func 要防抖的函数
     * @param {number} wait 等待时间（毫秒）
     * @returns {Function} 防抖后的函数
     */
    static debounce(func, wait) {
        let timeout;
        return function executedFunction(...args) {
            const later = () => {
                clearTimeout(timeout);
                func(...args);
            };
            clearTimeout(timeout);
            timeout = setTimeout(later, wait);
        };
    }

    /**
     * 节流函数
     * @param {Function} func 要节流的函数
     * @param {number} limit 限制时间（毫秒）
     * @returns {Function} 节流后的函数
     */
    static throttle(func, limit) {
        let inThrottle;
        return function(...args) {
            if (!inThrottle) {
                func.apply(this, args);
                inThrottle = true;
                setTimeout(() => inThrottle = false, limit);
            }
        };
    }

    /**
     * 深拷贝对象
     * @param {any} obj 要拷贝的对象
     * @returns {any} 拷贝后的对象
     */
    static deepClone(obj) {
        if (obj === null || typeof obj !== 'object') return obj;
        if (obj instanceof Date) return new Date(obj.getTime());
        if (obj instanceof Array) return obj.map(item => Utils.deepClone(item));
        if (typeof obj === 'object') {
            const clonedObj = {};
            for (const key in obj) {
                if (obj.hasOwnProperty(key)) {
                    clonedObj[key] = Utils.deepClone(obj[key]);
                }
            }
            return clonedObj;
        }
    }

    /**
     * 生成随机ID
     * @param {number} length ID长度
     * @returns {string} 随机ID
     */
    static generateId(length = 8) {
        const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
        let result = '';
        for (let i = 0; i < length; i++) {
            result += chars.charAt(Math.floor(Math.random() * chars.length));
        }
        return result;
    }
}

// 通知管理器
class NotificationManager {
    constructor() {
        this.notifications = [];
        this.maxNotifications = 5;
    }

    /**
     * 显示通知
     * @param {string} message 消息内容
     * @param {string} type 通知类型 (success, warning, danger, info)
     * @param {number} duration 显示时长（毫秒）
     */
    show(message, type = 'info', duration = 3000) {
        const notification = {
            id: Utils.generateId(),
            message,
            type,
            timestamp: new Date()
        };

        this.notifications.unshift(notification);
        
        // 限制通知数量
        if (this.notifications.length > this.maxNotifications) {
            this.notifications = this.notifications.slice(0, this.maxNotifications);
        }

        this._renderNotification(notification, duration);
    }

    /**
     * 渲染通知
     * @private
     */
    _renderNotification(notification, duration) {
        const alertDiv = document.createElement('div');
        alertDiv.id = `notification-${notification.id}`;
        alertDiv.className = `alert alert-${notification.type} alert-dismissible fade show position-fixed`;
        alertDiv.style.cssText = `
            top: 20px; 
            right: 20px; 
            z-index: 9999; 
            min-width: 300px;
            max-width: 400px;
            box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
            border-radius: 8px;
            border: none;
        `;
        
        alertDiv.innerHTML = `
            <div class="d-flex align-items-center">
                <i class="${this._getNotificationIcon(notification.type)} me-2"></i>
                <div class="flex-grow-1">${notification.message}</div>
                <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
            </div>
        `;

        document.body.appendChild(alertDiv);

        // 自动消失
        setTimeout(() => {
            if (alertDiv.parentNode) {
                alertDiv.classList.remove('show');
                setTimeout(() => {
                    if (alertDiv.parentNode) {
                        alertDiv.parentNode.removeChild(alertDiv);
                    }
                }, 150);
            }
        }, duration);
    }

    /**
     * 获取通知图标
     * @private
     */
    _getNotificationIcon(type) {
        const iconMap = {
            'success': 'fas fa-check-circle',
            'warning': 'fas fa-exclamation-triangle',
            'danger': 'fas fa-times-circle',
            'info': 'fas fa-info-circle'
        };
        return iconMap[type] || 'fas fa-info-circle';
    }

    /**
     * 清除所有通知
     */
    clearAll() {
        this.notifications.forEach(notification => {
            const element = document.getElementById(`notification-${notification.id}`);
            if (element && element.parentNode) {
                element.parentNode.removeChild(element);
            }
        });
        this.notifications = [];
    }
}

// API管理器
class ApiManager {
    constructor() {
        this.baseUrl = CONFIG.API_BASE_URL;
    }

    /**
     * 发送GET请求
     * @param {string} endpoint 端点
     * @param {Object} params 查询参数
     * @returns {Promise} 请求Promise
     */
    async get(endpoint, params = {}) {
        const url = new URL(this.baseUrl + endpoint, window.location.origin);
        Object.keys(params).forEach(key => {
            if (params[key] !== null && params[key] !== undefined) {
                url.searchParams.append(key, params[key]);
            }
        });

        try {
            const response = await fetch(url);
            return await this._handleResponse(response);
        } catch (error) {
            throw new Error(`GET ${endpoint} 失败: ${error.message}`);
        }
    }

    /**
     * 发送POST请求
     * @param {string} endpoint 端点
     * @param {Object} data 请求数据
     * @returns {Promise} 请求Promise
     */
    async post(endpoint, data = {}) {
        try {
            const response = await fetch(this.baseUrl + endpoint, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(data)
            });
            return await this._handleResponse(response);
        } catch (error) {
            throw new Error(`POST ${endpoint} 失败: ${error.message}`);
        }
    }

    /**
     * 处理响应
     * @private
     */
    async _handleResponse(response) {
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }

        const contentType = response.headers.get('content-type');
        if (contentType && contentType.includes('application/json')) {
            return await response.json();
        } else {
            return await response.text();
        }
    }
}

// WebSocket管理器
class WebSocketManager {
    constructor() {
        this.connections = new Map();
    }

    /**
     * 连接WebSocket
     * @param {string} namespace 命名空间
     * @param {Object} handlers 事件处理器
     * @returns {Object} Socket.IO实例
     */
    connect(namespace, handlers = {}) {
        if (this.connections.has(namespace)) {
            return this.connections.get(namespace);
        }

        const socket = io(namespace);
        
        // 绑定默认事件
        socket.on('connect', () => {
            console.log(`WebSocket连接成功: ${namespace}`);
            if (handlers.onConnect) handlers.onConnect();
        });

        socket.on('disconnect', () => {
            console.log(`WebSocket连接断开: ${namespace}`);
            if (handlers.onDisconnect) handlers.onDisconnect();
        });

        socket.on('error', (error) => {
            console.error(`WebSocket错误 ${namespace}:`, error);
            if (handlers.onError) handlers.onError(error);
        });

        // 绑定自定义事件
        Object.keys(handlers).forEach(event => {
            if (!['onConnect', 'onDisconnect', 'onError'].includes(event)) {
                socket.on(event, handlers[event]);
            }
        });

        this.connections.set(namespace, socket);
        return socket;
    }

    /**
     * 断开WebSocket连接
     * @param {string} namespace 命名空间
     */
    disconnect(namespace) {
        const socket = this.connections.get(namespace);
        if (socket) {
            socket.disconnect();
            this.connections.delete(namespace);
        }
    }

    /**
     * 断开所有连接
     */
    disconnectAll() {
        this.connections.forEach((socket, namespace) => {
            socket.disconnect();
        });
        this.connections.clear();
    }

    /**
     * 获取连接
     * @param {string} namespace 命名空间
     * @returns {Object|null} Socket.IO实例
     */
    getConnection(namespace) {
        return this.connections.get(namespace) || null;
    }
}

// 图表管理器
class ChartManager {
    constructor() {
        this.charts = new Map();
        this.defaultOptions = {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'top'
                }
            }
        };
    }

    /**
     * 创建图表
     * @param {string} canvasId Canvas元素ID
     * @param {Object} config 图表配置
     * @returns {Object} Chart.js实例
     */
    createChart(canvasId, config) {
        const canvas = document.getElementById(canvasId);
        if (!canvas) {
            throw new Error(`Canvas元素未找到: ${canvasId}`);
        }

        const ctx = canvas.getContext('2d');
        const mergedConfig = {
            ...config,
            options: {
                ...this.defaultOptions,
                ...config.options
            }
        };

        const chart = new Chart(ctx, mergedConfig);
        this.charts.set(canvasId, chart);
        return chart;
    }

    /**
     * 获取图表
     * @param {string} canvasId Canvas元素ID
     * @returns {Object|null} Chart.js实例
     */
    getChart(canvasId) {
        return this.charts.get(canvasId) || null;
    }

    /**
     * 销毁图表
     * @param {string} canvasId Canvas元素ID
     */
    destroyChart(canvasId) {
        const chart = this.charts.get(canvasId);
        if (chart) {
            chart.destroy();
            this.charts.delete(canvasId);
        }
    }

    /**
     * 销毁所有图表
     */
    destroyAllCharts() {
        this.charts.forEach((chart, canvasId) => {
            chart.destroy();
        });
        this.charts.clear();
    }

    /**
     * 更新图表数据
     * @param {string} canvasId Canvas元素ID
     * @param {Object} newData 新数据
     * @param {string} updateMode 更新模式
     */
    updateChartData(canvasId, newData, updateMode = 'default') {
        const chart = this.getChart(canvasId);
        if (chart) {
            Object.assign(chart.data, newData);
            chart.update(updateMode);
        }
    }
}

// 全局实例
const notificationManager = new NotificationManager();
const apiManager = new ApiManager();
const wsManager = new WebSocketManager();
const chartManager = new ChartManager();

// 全局函数
window.showNotification = (message, type, duration) => {
    notificationManager.show(message, type, duration);
};

window.formatNumber = Utils.formatNumber;
window.formatPercent = Utils.formatPercent;
window.formatFileSize = Utils.formatFileSize;
window.formatTime = Utils.formatTime;
window.getRelativeTime = Utils.getRelativeTime;
window.getStatusColor = Utils.getStatusColor;
window.getStatusIcon = Utils.getStatusIcon;

// 页面加载完成后的初始化
document.addEventListener('DOMContentLoaded', function() {
    // 初始化工具提示
    const tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));
    tooltipTriggerList.map(function (tooltipTriggerEl) {
        return new bootstrap.Tooltip(tooltipTriggerEl);
    });

    // 初始化弹出框
    const popoverTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="popover"]'));
    popoverTriggerList.map(function (popoverTriggerEl) {
        return new bootstrap.Popover(popoverTriggerEl);
    });

    // 更新当前时间
    function updateCurrentTime() {
        const timeElement = document.getElementById('current-time');
        if (timeElement) {
            timeElement.textContent = new Date().toLocaleString('zh-CN');
        }
    }

    // 每秒更新时间
    setInterval(updateCurrentTime, 1000);
    updateCurrentTime();
});

// 页面卸载时清理资源
window.addEventListener('beforeunload', function() {
    wsManager.disconnectAll();
    chartManager.destroyAllCharts();
    notificationManager.clearAll();
});
