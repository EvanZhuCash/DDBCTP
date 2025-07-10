#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
运行真实CTP数据录制测试
"""

import sys
import os
import json
import time
import logging
import dolphindb as ddb
from pathlib import Path

# Add paths for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'vnpy_ctp'))

from market_data_recorder import MarketDataRecorder

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def run_real_ctp_test():
    """运行真实CTP数据录制测试"""
    logger.info("=== 真实CTP数据录制测试 ===")
    
    # 加载CTP配置
    config_file = Path("ctp_setting.json")
    if not config_file.exists():
        logger.error("未找到CTP配置文件")
        return False
    
    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            ctp_setting = json.load(f)
        
        logger.info("创建市场数据录制器...")
        recorder = MarketDataRecorder(ctp_setting=ctp_setting)
        
        logger.info("启动录制（样本测试 - 前10个合约）...")
        recorder.start_recording(full_market=True)
        
        # 等待CTP连接和数据流入
        logger.info("等待CTP数据流入...")
        time.sleep(30)  # 等待30秒让数据流入
        
        # 检查数据
        logger.info("检查录制的数据...")
        session = ddb.session()
        session.connect('localhost', 8848, 'admin', '123456')
        
        tick_count = session.run('size(live_tick_stream)')
        logger.info(f"live_tick_stream 当前记录数: {tick_count}")
        
        if tick_count > 0:
            logger.info("显示最新5条数据:")
            recent_data = session.run('select top 5 * from live_tick_stream order by timestamp desc')
            logger.info(f"{recent_data}")
            
            # 按合约统计
            symbol_stats = session.run('''
                select symbol, count(*) as tick_count, 
                       min(price) as min_price, max(price) as max_price
                from live_tick_stream 
                group by symbol 
                order by tick_count desc
            ''')
            logger.info("按合约统计:")
            logger.info(f"{symbol_stats}")
        
        session.close()
        
        # 继续录制一段时间
        logger.info("继续录制30秒...")
        time.sleep(30)
        
        # 停止录制
        logger.info("停止录制...")
        recorder.stop_recording()
        
        # 最终检查
        logger.info("最终数据检查...")
        session = ddb.session()
        session.connect('localhost', 8848, 'admin', '123456')
        
        final_tick_count = session.run('size(live_tick_stream)')
        logger.info(f"最终 live_tick_stream 记录数: {final_tick_count}")
        
        # 检查月度数据库
        snapshot_count = session.run('exec count(*) from loadTable("dfs://market_data_recording_M", "snapshot")')
        logger.info(f"月度数据库 snapshot 记录数: {snapshot_count}")
        
        session.close()
        
        logger.info("✅ 真实CTP数据录制测试完成")
        return True
        
    except Exception as e:
        logger.error(f"录制测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def show_data_locations():
    """显示数据存储位置"""
    logger.info("\n=== 数据存储位置 ===")
    
    try:
        session = ddb.session()
        session.connect('localhost', 8848, 'admin', '123456')
        
        # 检查流表
        logger.info("1. 实时流表:")
        shared_tables = session.run('exec name from objs(true) where form=`TABLE and shared=true')
        for table in shared_tables:
            if table in ['live_tick_stream', 'live_minute_stream', 'recording_stats_stream']:
                size = session.run(f'size({table})')
                logger.info(f"   {table}: {size} 条记录")
        
        # 检查数据库
        logger.info("\n2. 分区数据库:")
        exists_m = session.run('existsDatabase("dfs://market_data_recording_M")')
        exists_y = session.run('existsDatabase("dfs://market_data_recording_Y")')
        
        logger.info(f"   月度数据库: {'存在' if exists_m else '不存在'}")
        logger.info(f"   年度数据库: {'存在' if exists_y else '不存在'}")
        
        if exists_m:
            snapshot_count = session.run('exec count(*) from loadTable("dfs://market_data_recording_M", "snapshot")')
            k_minute_count = session.run('exec count(*) from loadTable("dfs://market_data_recording_M", "k_minute")')
            logger.info(f"   月度数据库 snapshot: {snapshot_count} 条记录")
            logger.info(f"   月度数据库 k_minute: {k_minute_count} 条记录")
        
        session.close()
        
        logger.info("\n数据查询方式:")
        logger.info("1. 实时数据: select * from live_tick_stream")
        logger.info("2. 月度数据: select * from loadTable('dfs://market_data_recording_M', 'snapshot')")
        logger.info("3. 年度数据: select * from loadTable('dfs://market_data_recording_Y', 'snapshot')")
        
    except Exception as e:
        logger.error(f"显示数据位置失败: {e}")


def main():
    """主函数"""
    logger.info("真实CTP数据录制测试")
    logger.info("=" * 50)
    
    try:
        # 运行测试
        if run_real_ctp_test():
            logger.info("测试成功")
        else:
            logger.error("测试失败")
            return
        
        print("\n" + "="*50 + "\n")
        
        # 显示数据位置
        show_data_locations()
        
    except KeyboardInterrupt:
        logger.info("\n用户中断测试")
    except Exception as e:
        logger.error(f"测试过程中发生错误: {e}")


if __name__ == "__main__":
    main()
