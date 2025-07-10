#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
验证CTP连接和合约发现
"""

import sys
import os
import json
import time
import logging
from pathlib import Path

# Add paths for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'vnpy_ctp'))

from market_data_recorder import ContractDiscovery

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def verify_ctp_connection():
    """验证CTP连接"""
    logger.info("=== 验证CTP连接 ===")
    
    # 加载CTP配置
    config_file = Path("ctp_setting.json")
    if not config_file.exists():
        logger.error("未找到CTP配置文件")
        return False
    
    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            ctp_setting = json.load(f)
        
        logger.info(f"CTP配置:")
        logger.info(f"  用户名: {ctp_setting['用户名']}")
        logger.info(f"  经纪商: {ctp_setting['经纪商代码']}")
        logger.info(f"  交易服务器: {ctp_setting['交易服务器']}")
        logger.info(f"  行情服务器: {ctp_setting['行情服务器']}")
        
        # 测试合约发现
        logger.info("开始测试CTP合约发现...")
        discovery = ContractDiscovery(ctp_setting)
        
        start_time = time.time()
        contracts = discovery.get_all_futures_contracts()
        end_time = time.time()
        
        logger.info(f"合约发现完成，耗时: {end_time - start_time:.2f}秒")
        logger.info(f"发现 {len(contracts)} 个期货合约")
        
        if contracts:
            logger.info("前20个合约:")
            for i, contract in enumerate(contracts[:20]):
                logger.info(f"  {i+1:2d}. {contract}")
            
            if len(contracts) > 20:
                logger.info(f"  ... 还有 {len(contracts) - 20} 个合约")
            
            return True
        else:
            logger.error("未发现任何合约")
            return False
            
    except Exception as e:
        logger.error(f"CTP连接验证失败: {e}")
        return False


if __name__ == "__main__":
    if verify_ctp_connection():
        logger.info("✅ CTP连接验证成功")
    else:
        logger.error("❌ CTP连接验证失败")
