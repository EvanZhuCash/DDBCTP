#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
启动真实CTP市场数据录制
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


def start_real_recording():
    """启动真实CTP数据录制"""
    logger.info("=== 启动真实CTP市场数据录制 ===")
    
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
        
        logger.info("启动录制（将自动发现所有期货合约）...")
        recorder.start_recording(full_market=True)
        
        # 录制60秒的真实数据
        logger.info("录制60秒真实CTP数据...")
        for i in range(60):
            time.sleep(1)
            if i % 10 == 0:
                logger.info(f"录制进度: {i}/60 秒")
        
        logger.info("停止录制...")
        recorder.stop_recording()
        
        # 生成报告
        report_file = f"real_ctp_recording_{time.strftime('%Y%m%d_%H%M%S')}.json"
        recorder.generate_recording_report(report_file)
        logger.info(f"录制报告已保存: {report_file}")
        
        return True
        
    except Exception as e:
        logger.error(f"录制失败: {e}")
        return False


def show_recorded_data():
    """显示录制的数据"""
    logger.info("=== 显示录制的真实数据 ===")
    
    try:
        session = ddb.session()
        session.connect('localhost', 8848, 'admin', '123456')
        
        # 检查流表数据
        logger.info("1. 实时流表数据:")
        tick_count = session.run('size(live_tick_stream)')
        minute_count = session.run('size(live_minute_stream)')
        stats_count = session.run('size(recording_stats_stream)')
        
        logger.info(f"   live_tick_stream: {tick_count} 条记录")
        logger.info(f"   live_minute_stream: {minute_count} 条记录")
        logger.info(f"   recording_stats_stream: {stats_count} 条记录")
        
        # 显示tick数据样本
        if tick_count > 0:
            logger.info("\n2. Tick数据样本 (最新5条):")
            tick_sample = session.run('select top 5 * from live_tick_stream order by timestamp desc')
            logger.info(f"{tick_sample}")
            
            # 按合约统计
            logger.info("\n3. 按合约统计:")
            symbol_stats = session.run('''
                select symbol, count(*) as tick_count, 
                       min(price) as min_price, max(price) as max_price,
                       sum(volume) as total_volume
                from live_tick_stream 
                group by symbol 
                order by tick_count desc 
                limit 10
            ''')
            logger.info(f"{symbol_stats}")
        
        # 检查月度数据库
        logger.info("\n4. 月度数据库:")
        snapshot_count = session.run('exec count(*) from loadTable("dfs://market_data_recording_M", "snapshot")')
        k_minute_count = session.run('exec count(*) from loadTable("dfs://market_data_recording_M", "k_minute")')
        
        logger.info(f"   snapshot表: {snapshot_count} 条记录")
        logger.info(f"   k_minute表: {k_minute_count} 条记录")
        
        if snapshot_count > 0:
            logger.info("\n5. 月度数据库样本:")
            snapshot_sample = session.run('''
                select top 5 * from loadTable("dfs://market_data_recording_M", "snapshot") 
                order by datetime desc
            ''')
            logger.info(f"{snapshot_sample}")
        
        session.close()
        
    except Exception as e:
        logger.error(f"显示数据失败: {e}")


def main():
    """主函数"""
    logger.info("真实CTP市场数据录制")
    logger.info("=" * 50)
    
    try:
        # 启动录制
        if start_real_recording():
            logger.info("✅ 录制成功完成")
        else:
            logger.error("❌ 录制失败")
            return
        
        print("\n" + "="*50 + "\n")
        
        # 显示录制的数据
        show_recorded_data()
        
        logger.info("\n" + "="*50)
        logger.info("真实数据已录制到:")
        logger.info("1. 实时流表: live_tick_stream, live_minute_stream")
        logger.info("2. 月度数据库: dfs://market_data_recording_M/snapshot")
        logger.info("3. 年度数据库: dfs://market_data_recording_Y/snapshot")
        logger.info("=" * 50)
        
    except KeyboardInterrupt:
        logger.info("\n用户中断录制")
    except Exception as e:
        logger.error(f"录制过程中发生错误: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
