/**
 * 高级UI组件库
 * 提供模态框、加载状态、确认对话框、帮助系统等功能
 */

class UIComponents {
    constructor() {
        this.modals = new Map();
        this.loadingStates = new Map();
        this.confirmCallbacks = new Map();
        
        this.init();
    }
    
    init() {
        this.createLoadingOverlay();
        this.setupGlobalEventListeners();
    }
    
    /**
     * 创建全局加载遮罩
     */
    createLoadingOverlay() {
        const overlay = document.createElement('div');
        overlay.id = 'global-loading-overlay';
        overlay.className = 'loading-overlay d-none';
        overlay.innerHTML = `
            <div class="loading-content">
                <div class="loading-spinner"></div>
                <div class="h5 mb-0">加载中...</div>
                <div class="text-muted" id="loading-message">请稍候</div>
            </div>
        `;
        document.body.appendChild(overlay);
    }
    
    /**
     * 显示全局加载状态
     * @param {string} message 加载消息
     */
    showLoading(message = '加载中...') {
        const overlay = document.getElementById('global-loading-overlay');
        const messageEl = document.getElementById('loading-message');
        
        if (overlay && messageEl) {
            messageEl.textContent = message;
            overlay.classList.remove('d-none');
        }
    }
    
    /**
     * 隐藏全局加载状态
     */
    hideLoading() {
        const overlay = document.getElementById('global-loading-overlay');
        if (overlay) {
            overlay.classList.add('d-none');
        }
    }
    
    /**
     * 显示确认对话框
     * @param {Object} options 配置选项
     * @returns {Promise} 用户选择的Promise
     */
    showConfirm(options = {}) {
        const {
            title = '确认操作',
            message = '确定要执行此操作吗？',
            confirmText = '确认',
            cancelText = '取消',
            type = 'warning',
            icon = 'fas fa-question-circle'
        } = options;
        
        return new Promise((resolve) => {
            const modalId = 'confirm-modal-' + Date.now();
            const modal = this.createModal({
                id: modalId,
                title: title,
                body: `
                    <div class="text-center">
                        <i class="${icon} fa-3x text-${type} mb-3"></i>
                        <p class="h5">${message}</p>
                    </div>
                `,
                footer: `
                    <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">${cancelText}</button>
                    <button type="button" class="btn btn-${type}" id="${modalId}-confirm">${confirmText}</button>
                `,
                size: 'modal-dialog-centered'
            });
            
            // 绑定确认按钮事件
            document.getElementById(`${modalId}-confirm`).addEventListener('click', () => {
                modal.hide();
                resolve(true);
            });
            
            // 绑定取消事件
            modal._element.addEventListener('hidden.bs.modal', () => {
                if (!this.confirmCallbacks.has(modalId)) {
                    resolve(false);
                }
                this.removeModal(modalId);
            });
            
            this.confirmCallbacks.set(modalId, resolve);
            modal.show();
        });
    }
    
    /**
     * 显示信息对话框
     * @param {Object} options 配置选项
     */
    showAlert(options = {}) {
        const {
            title = '提示',
            message = '',
            type = 'info',
            icon = 'fas fa-info-circle',
            confirmText = '确定'
        } = options;
        
        const modalId = 'alert-modal-' + Date.now();
        const modal = this.createModal({
            id: modalId,
            title: title,
            body: `
                <div class="text-center">
                    <i class="${icon} fa-3x text-${type} mb-3"></i>
                    <p class="h5">${message}</p>
                </div>
            `,
            footer: `
                <button type="button" class="btn btn-${type}" data-bs-dismiss="modal">${confirmText}</button>
            `,
            size: 'modal-dialog-centered'
        });
        
        modal._element.addEventListener('hidden.bs.modal', () => {
            this.removeModal(modalId);
        });
        
        modal.show();
    }
    
    /**
     * 创建模态框
     * @param {Object} options 模态框配置
     * @returns {bootstrap.Modal} Bootstrap模态框实例
     */
    createModal(options = {}) {
        const {
            id,
            title = '模态框',
            body = '',
            footer = '',
            size = '',
            backdrop = true,
            keyboard = true
        } = options;
        
        const modalHtml = `
            <div class="modal fade" id="${id}" tabindex="-1" data-bs-backdrop="${backdrop}" data-bs-keyboard="${keyboard}">
                <div class="modal-dialog ${size}">
                    <div class="modal-content">
                        <div class="modal-header">
                            <h5 class="modal-title">${title}</h5>
                            <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
                        </div>
                        <div class="modal-body">${body}</div>
                        ${footer ? `<div class="modal-footer">${footer}</div>` : ''}
                    </div>
                </div>
            </div>
        `;
        
        // 添加到DOM
        document.body.insertAdjacentHTML('beforeend', modalHtml);
        
        // 创建Bootstrap模态框实例
        const modalElement = document.getElementById(id);
        const modal = new bootstrap.Modal(modalElement);
        
        this.modals.set(id, modal);
        return modal;
    }
    
    /**
     * 移除模态框
     * @param {string} modalId 模态框ID
     */
    removeModal(modalId) {
        const modal = this.modals.get(modalId);
        if (modal) {
            modal.dispose();
            this.modals.delete(modalId);
        }
        
        const modalElement = document.getElementById(modalId);
        if (modalElement) {
            modalElement.remove();
        }
        
        this.confirmCallbacks.delete(modalId);
    }
    
    /**
     * 显示帮助提示
     * @param {string} content 帮助内容
     * @param {HTMLElement} target 目标元素
     */
    showHelp(content, target) {
        if (!target) return;
        
        // 创建或更新tooltip
        let tooltip = bootstrap.Tooltip.getInstance(target);
        if (!tooltip) {
            tooltip = new bootstrap.Tooltip(target, {
                title: content,
                html: true,
                placement: 'auto',
                trigger: 'click',
                customClass: 'help-tooltip'
            });
        } else {
            tooltip.setContent({ '.tooltip-inner': content });
        }
        
        tooltip.show();
        
        // 3秒后自动隐藏
        setTimeout(() => {
            tooltip.hide();
        }, 3000);
    }
    
    /**
     * 创建进度条
     * @param {Object} options 进度条配置
     * @returns {Object} 进度条控制对象
     */
    createProgressBar(options = {}) {
        const {
            container,
            label = '进度',
            value = 0,
            max = 100,
            animated = true,
            striped = true,
            color = 'primary'
        } = options;
        
        if (!container) return null;
        
        const progressId = 'progress-' + Date.now();
        const progressHtml = `
            <div class="mb-2">
                <div class="d-flex justify-content-between">
                    <span>${label}</span>
                    <span id="${progressId}-text">${value}%</span>
                </div>
                <div class="progress">
                    <div class="progress-bar bg-${color} ${animated ? 'progress-bar-animated' : ''} ${striped ? 'progress-bar-striped' : ''}" 
                         id="${progressId}-bar" 
                         role="progressbar" 
                         style="width: ${value}%" 
                         aria-valuenow="${value}" 
                         aria-valuemin="0" 
                         aria-valuemax="${max}">
                    </div>
                </div>
            </div>
        `;
        
        container.insertAdjacentHTML('beforeend', progressHtml);
        
        const progressBar = document.getElementById(`${progressId}-bar`);
        const progressText = document.getElementById(`${progressId}-text`);
        
        return {
            update: (newValue) => {
                const percentage = Math.round((newValue / max) * 100);
                progressBar.style.width = percentage + '%';
                progressBar.setAttribute('aria-valuenow', newValue);
                progressText.textContent = percentage + '%';
            },
            remove: () => {
                const progressElement = progressBar.closest('.mb-2');
                if (progressElement) {
                    progressElement.remove();
                }
            }
        };
    }
    
    /**
     * 显示Toast通知
     * @param {Object} options Toast配置
     */
    showToast(options = {}) {
        const {
            title = '通知',
            message = '',
            type = 'info',
            duration = 5000,
            position = 'top-end'
        } = options;
        
        // 创建toast容器（如果不存在）
        let toastContainer = document.getElementById('toast-container');
        if (!toastContainer) {
            toastContainer = document.createElement('div');
            toastContainer.id = 'toast-container';
            toastContainer.className = `toast-container position-fixed ${position} p-3`;
            toastContainer.style.zIndex = '9999';
            document.body.appendChild(toastContainer);
        }
        
        const toastId = 'toast-' + Date.now();
        const toastHtml = `
            <div id="${toastId}" class="toast" role="alert" data-bs-delay="${duration}">
                <div class="toast-header">
                    <i class="fas fa-circle text-${type} me-2"></i>
                    <strong class="me-auto">${title}</strong>
                    <small class="text-muted">刚刚</small>
                    <button type="button" class="btn-close" data-bs-dismiss="toast"></button>
                </div>
                <div class="toast-body">${message}</div>
            </div>
        `;
        
        toastContainer.insertAdjacentHTML('beforeend', toastHtml);
        
        const toastElement = document.getElementById(toastId);
        const toast = new bootstrap.Toast(toastElement);
        
        toastElement.addEventListener('hidden.bs.toast', () => {
            toastElement.remove();
        });
        
        toast.show();
    }
    
    /**
     * 设置全局事件监听器
     */
    setupGlobalEventListeners() {
        // ESC键关闭模态框
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape') {
                // 关闭最顶层的模态框
                const modals = document.querySelectorAll('.modal.show');
                if (modals.length > 0) {
                    const topModal = modals[modals.length - 1];
                    const modalInstance = bootstrap.Modal.getInstance(topModal);
                    if (modalInstance) {
                        modalInstance.hide();
                    }
                }
            }
        });
        
        // 点击外部关闭下拉菜单
        document.addEventListener('click', (e) => {
            if (!e.target.closest('.dropdown')) {
                const dropdowns = document.querySelectorAll('.dropdown-menu.show');
                dropdowns.forEach(dropdown => {
                    const dropdownInstance = bootstrap.Dropdown.getInstance(dropdown.previousElementSibling);
                    if (dropdownInstance) {
                        dropdownInstance.hide();
                    }
                });
            }
        });
    }
}

// 创建全局UI组件实例
const uiComponents = new UIComponents();

// 全局函数
window.showConfirm = (options) => uiComponents.showConfirm(options);
window.showAlert = (options) => uiComponents.showAlert(options);
window.showLoading = (message) => uiComponents.showLoading(message);
window.hideLoading = () => uiComponents.hideLoading();
window.showHelp = (content, target) => uiComponents.showHelp(content, target);
window.showToast = (options) => uiComponents.showToast(options);
window.createProgressBar = (options) => uiComponents.createProgressBar(options);
