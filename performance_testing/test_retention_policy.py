"""
测试3天数据保留策略
"""

import sys
import os
import time
import logging
from datetime import datetime, timedelta

# Add paths for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from market_data_recorder import MarketDataRecorder

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def test_retention_policy():
    """测试3天保留策略"""
    logger.info("开始测试3天数据保留策略...")
    
    # 清理环境
    logger.info("清理DolphinDB环境...")
    try:
        import subprocess
        subprocess.run([sys.executable, "cleanup_market_data.py"], check=True, cwd=os.path.dirname(__file__))
    except Exception as e:
        logger.warning(f"清理失败: {e}")
    
    # 创建录制器
    recorder = MarketDataRecorder()
    
    try:
        # 手动执行保留策略测试
        logger.info("手动执行3天保留策略...")
        recorder.execute_retention_policy()
        
        # 测试数据插入和查询
        logger.info("测试数据插入...")
        
        # 插入一些测试数据到月度数据库
        test_data_sql = """
            snapshot_table_m = loadTable("dfs://market_data_recording_M", "snapshot")
            
            // 插入今天的数据
            today_data = table(
                [now(), now()-1000, now()-2000] as datetime,
                [`IC2501, `rb2501, `au2501] as symbol,
                [100.0, 200.0, 300.0] as last_price,
                [1000, 2000, 3000] as volume,
                [99.5, 199.5, 299.5] as bid1,
                [100.5, 200.5, 300.5] as ask1,
                [500, 1000, 1500] as bid_vol1,
                [500, 1000, 1500] as ask_vol1,
                [100000.0, 400000.0, 900000.0] as turnover,
                [10000, 20000, 30000] as open_interest,
                [`CFFEX, `SHFE, `SHFE] as exchange
            )
            insert into snapshot_table_m values(today_data)
            
            // 插入4天前的数据（应该被清理）
            old_datetime = today() - 4 + 09:30:00.000
            old_data = table(
                [old_datetime, old_datetime+1000, old_datetime+2000] as datetime,
                [`IC2501, `rb2501, `au2501] as symbol,
                [95.0, 195.0, 295.0] as last_price,
                [800, 1600, 2400] as volume,
                [94.5, 194.5, 294.5] as bid1,
                [95.5, 195.5, 295.5] as ask1,
                [400, 800, 1200] as bid_vol1,
                [400, 800, 1200] as ask_vol1,
                [76000.0, 312000.0, 708000.0] as turnover,
                [8000, 16000, 24000] as open_interest,
                [`CFFEX, `SHFE, `SHFE] as exchange
            )
            insert into snapshot_table_m values(old_data)
        """
        
        recorder.session.run(test_data_sql)
        logger.info("测试数据插入完成")
        
        # 查询插入前的数据量
        before_count = recorder.session.run("""
            snapshot_table_m = loadTable("dfs://market_data_recording_M", "snapshot")
            select count(*) as total_count from snapshot_table_m
        """)
        logger.info(f"保留策略执行前数据量: {before_count.iloc[0]['total_count']}")
        
        # 查询4天前的数据
        old_data_count = recorder.session.run("""
            snapshot_table_m = loadTable("dfs://market_data_recording_M", "snapshot")
            select count(*) as old_count from snapshot_table_m where date(datetime) < today() - 3
        """)
        logger.info(f"4天前数据量: {old_data_count.iloc[0]['old_count']}")
        
        # 执行保留策略
        logger.info("执行3天保留策略...")
        recorder.execute_retention_policy()
        
        # 查询执行后的数据量
        after_count = recorder.session.run("""
            snapshot_table_m = loadTable("dfs://market_data_recording_M", "snapshot")
            select count(*) as total_count from snapshot_table_m
        """)
        logger.info(f"保留策略执行后数据量: {after_count.iloc[0]['total_count']}")
        
        # 验证4天前的数据是否被清理
        remaining_old_data = recorder.session.run("""
            snapshot_table_m = loadTable("dfs://market_data_recording_M", "snapshot")
            select count(*) as old_count from snapshot_table_m where date(datetime) < today() - 3
        """)
        logger.info(f"保留策略后4天前数据量: {remaining_old_data.iloc[0]['old_count']}")
        
        # 验证今天的数据是否保留
        today_data_count = recorder.session.run("""
            snapshot_table_m = loadTable("dfs://market_data_recording_M", "snapshot")
            select count(*) as today_count from snapshot_table_m where date(datetime) = today()
        """)
        logger.info(f"今天数据量: {today_data_count.iloc[0]['today_count']}")
        
        # 测试结果
        if remaining_old_data.iloc[0]['old_count'] == 0:
            logger.info("✅ 3天保留策略测试成功：旧数据已被清理")
        else:
            logger.warning("❌ 3天保留策略测试失败：旧数据未被完全清理")
        
        if today_data_count.iloc[0]['today_count'] > 0:
            logger.info("✅ 今天数据保留测试成功：新数据被保留")
        else:
            logger.warning("❌ 今天数据保留测试失败：新数据被误删")
        
        logger.info("3天保留策略测试完成")
        
    except Exception as e:
        logger.error(f"测试失败: {e}")
        raise
    
    finally:
        # 清理测试环境
        try:
            recorder.session.close()
        except:
            pass


def test_daily_volume_calculation():
    """测试日数据量计算"""
    logger.info("=== 日数据量计算 ===")
    
    # 交易参数
    trading_hours = 6.5  # 6-7小时平均
    contracts = 1728
    ticks_per_second = 3  # 现实CTP频率
    seconds_per_hour = 3600
    
    # 计算每日总tick数
    total_seconds = trading_hours * seconds_per_hour
    total_ticks_per_day = total_seconds * ticks_per_second
    
    logger.info(f"每日交易时间: {trading_hours}小时")
    logger.info(f"总合约数: {contracts}")
    logger.info(f"平均每秒tick数: {ticks_per_second}")
    logger.info(f"每日总秒数: {total_seconds:,.0f}")
    logger.info(f"每日总tick数: {total_ticks_per_day:,.0f}")
    
    # 计算数据大小
    bytes_per_record = 150  # 每条记录约150字节
    total_bytes_per_day = total_ticks_per_day * bytes_per_record
    mb_per_day = total_bytes_per_day / (1024 * 1024)
    gb_per_day = mb_per_day / 1024
    
    logger.info(f"每条记录字节数: {bytes_per_record}")
    logger.info(f"每日总MB: {mb_per_day:.1f} MB")
    logger.info(f"每日总GB: {gb_per_day:.3f} GB")
    
    # 计算3天保留的数据量
    days_retention = 3
    total_gb_3_days = gb_per_day * days_retention
    logger.info(f"3天保留总数据量: {total_gb_3_days:.3f} GB")
    
    # 计算流表大小（内存中）
    stream_hours = 2  # 流表保留2小时
    stream_ticks = stream_hours * seconds_per_hour * ticks_per_second
    stream_mb = (stream_ticks * bytes_per_record) / (1024 * 1024)
    logger.info(f"流表大小({stream_hours}小时保留): {stream_mb:.1f} MB")
    
    logger.info("\n=== 建议 ===")
    logger.info(f"1. 流表最大大小: {int(stream_ticks):,} 条记录 ({stream_mb:.1f} MB)")
    logger.info(f"2. 每日持久化存储: {gb_per_day:.3f} GB")
    logger.info(f"3. 3天总存储: {total_gb_3_days:.3f} GB")
    logger.info(f"4. 内存使用远低于8GB限制")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="3天数据保留策略测试")
    parser.add_argument("--test-retention", action="store_true", help="测试保留策略")
    parser.add_argument("--calc-volume", action="store_true", help="计算数据量")
    
    args = parser.parse_args()
    
    if args.test_retention:
        test_retention_policy()
    elif args.calc_volume:
        test_daily_volume_calculation()
    else:
        # 默认运行所有测试
        test_daily_volume_calculation()
        test_retention_policy()
