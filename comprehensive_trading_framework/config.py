#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Configuration file for Comprehensive Trading Framework
综合交易框架配置文件
"""

# DolphinDB Configuration
DDB_CONFIG = {
    "host": "localhost",
    "port": 8848,
    "username": "admin", 
    "password": "123456"
}

# CTP Configuration
CTP_CONFIG = {
    "用户名": "239344",
    "密码": "xyzhao@3026528",
    "经纪商代码": "9999", 
    "交易服务器": "180.168.146.187:10201",
    "行情服务器": "180.168.146.187:10211",
    "产品名称": "simnow_client_test",
    "授权编码": "0000000000000000"
}

# Trading Strategy Configuration
STRATEGY_CONFIG = {
    "bollinger": {
        "enabled": True,
        "timeframe": "1min",
        "period": 20,
        "std_dev": 2.0,
        "volume_per_order": 5,
        "max_position": 20
    },
    "ma_cross": {
        "enabled": False,
        "timeframe": "5min", 
        "fast_period": 5,
        "slow_period": 20,
        "volume_per_order": 3,
        "max_position": 15
    }
}

# Order Management Configuration
ORDER_CONFIG = {
    "tracking": {
        "price_tolerance": 0.02,  # 2% price deviation threshold
        "time_threshold": 30000,  # 30 seconds time threshold
        "max_retries": 3
    },
    "execution": {
        "order_delay_ms": 100,
        "max_queue_size": 1000,
        "batch_size": 10
    },
    "risk": {
        "max_daily_loss": 10000,
        "max_position_per_symbol": 50,
        "max_order_frequency": 100  # orders per minute
    }
}

# Cross-sectional Strategy Configuration
CROSS_SECTION_CONFIG = {
    "enabled": False,
    "top_n": 3,
    "bottom_n": 3,
    "rebalance_frequency": 300000,  # 5 minutes in milliseconds
    "universe": ["IC2509", "IF2509", "IH2509", "IM2509"]
}

# Timeframe Configuration
TIMEFRAME_CONFIG = {
    "tick": {"enabled": True},
    "10s": {"enabled": True, "window_size": 10000},
    "1min": {"enabled": True, "window_size": 60000},
    "5min": {"enabled": True, "window_size": 300000},
    "15min": {"enabled": True, "window_size": 900000},
    "1hour": {"enabled": False, "window_size": 3600000}
}

# Logging Configuration
LOGGING_CONFIG = {
    "level": "INFO",
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    "file": "comprehensive_trading.log",
    "max_size": 10485760,  # 10MB
    "backup_count": 5
}

# Monitoring Configuration
MONITORING_CONFIG = {
    "enabled": True,
    "update_interval": 5,  # seconds
    "metrics": [
        "order_count",
        "fill_rate", 
        "pnl",
        "position_count",
        "latency"
    ]
}

# Symbol Configuration
SYMBOL_CONFIG = {
    "IC2509": {
        "exchange": "CFFEX",
        "multiplier": 200,
        "tick_size": 0.2,
        "margin_rate": 0.12
    },
    "IF2509": {
        "exchange": "CFFEX", 
        "multiplier": 300,
        "tick_size": 0.2,
        "margin_rate": 0.12
    },
    "IH2509": {
        "exchange": "CFFEX",
        "multiplier": 300, 
        "tick_size": 0.2,
        "margin_rate": 0.12
    }
}

# Performance Configuration
PERFORMANCE_CONFIG = {
    "enable_profiling": False,
    "memory_monitoring": True,
    "latency_tracking": True,
    "throughput_monitoring": True
}

def get_config(section=None):
    """获取配置信息"""
    if section is None:
        return {
            "ddb": DDB_CONFIG,
            "ctp": CTP_CONFIG,
            "strategy": STRATEGY_CONFIG,
            "order": ORDER_CONFIG,
            "cross_section": CROSS_SECTION_CONFIG,
            "timeframe": TIMEFRAME_CONFIG,
            "logging": LOGGING_CONFIG,
            "monitoring": MONITORING_CONFIG,
            "symbol": SYMBOL_CONFIG,
            "performance": PERFORMANCE_CONFIG
        }
    
    config_map = {
        "ddb": DDB_CONFIG,
        "ctp": CTP_CONFIG,
        "strategy": STRATEGY_CONFIG,
        "order": ORDER_CONFIG,
        "cross_section": CROSS_SECTION_CONFIG,
        "timeframe": TIMEFRAME_CONFIG,
        "logging": LOGGING_CONFIG,
        "monitoring": MONITORING_CONFIG,
        "symbol": SYMBOL_CONFIG,
        "performance": PERFORMANCE_CONFIG
    }
    
    return config_map.get(section, {})

def validate_config():
    """验证配置有效性"""
    errors = []
    
    # 验证DDB配置
    if not DDB_CONFIG.get("host"):
        errors.append("DolphinDB host not configured")
    
    # 验证CTP配置
    required_ctp_fields = ["用户名", "密码", "经纪商代码", "交易服务器", "行情服务器"]
    for field in required_ctp_fields:
        if not CTP_CONFIG.get(field):
            errors.append(f"CTP {field} not configured")
    
    # 验证策略配置
    for strategy_name, config in STRATEGY_CONFIG.items():
        if config.get("enabled") and not config.get("timeframe"):
            errors.append(f"Strategy {strategy_name} timeframe not configured")
    
    return errors

if __name__ == "__main__":
    # 配置验证
    errors = validate_config()
    if errors:
        print("Configuration errors found:")
        for error in errors:
            print(f"  - {error}")
    else:
        print("Configuration validation passed")
        
    # 打印配置摘要
    print("\nConfiguration Summary:")
    print(f"DolphinDB: {DDB_CONFIG['host']}:{DDB_CONFIG['port']}")
    print(f"CTP User: {CTP_CONFIG['用户名']}")
    print(f"Enabled Strategies: {[k for k, v in STRATEGY_CONFIG.items() if v.get('enabled')]}")
    print(f"Monitored Symbols: {list(SYMBOL_CONFIG.keys())}")
