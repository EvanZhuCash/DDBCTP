"""
市场数据录制清理脚本
清理所有市场数据录制相关的数据库和流表
"""

import sys
import os
import logging
import dolphindb as ddb

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def cleanup_market_data_databases(host: str = "localhost", port: int = 8848, 
                                 username: str = "admin", password: str = "123456"):
    """清理所有市场数据录制数据库"""
    logger.info("开始清理市场数据录制数据库...")
    
    try:
        # 连接DolphinDB
        session = ddb.session()
        session.connect(host, port, username, password)
        logger.info(f"已连接到DolphinDB: {host}:{port}")
        
        # 定义要清理的数据库列表
        databases_to_clean = [
            "dfs://market_data_recording",      # 旧版数据库
            "dfs://market_data_recording_Y",    # 年度数据库
            "dfs://market_data_recording_M",    # 月度数据库
            "dfs://market_data_recording_D",    # 日度数据库（如果存在）
            "dfs://market_data_recording_H",    # 小时数据库（如果存在）
        ]
        
        # 清理数据库
        for db_name in databases_to_clean:
            try:
                logger.info(f"清理数据库: {db_name}")
                session.run(f'if(existsDatabase("{db_name}")) dropDatabase("{db_name}")')
                logger.info(f"数据库 {db_name} 清理完成")
            except Exception as e:
                logger.warning(f"清理数据库 {db_name} 失败: {e}")
        
        # 清理共享流表
        stream_tables = [
            "live_tick_stream",
            "live_minute_stream", 
            "recording_stats_stream",
            "market_data_stream",
            "aggregated_stream"
        ]
        
        logger.info("清理共享流表...")
        for table_name in stream_tables:
            try:
                session.run(f'try{{ undef("{table_name}", SHARED) }}catch(ex){{ print("Table {table_name} not found") }}')
                logger.info(f"流表 {table_name} 清理完成")
            except Exception as e:
                logger.warning(f"清理流表 {table_name} 失败: {e}")
        
        # 清理流引擎
        stream_engines = [
            "minuteAggEngine",
            "hourAggEngine", 
            "dailyAggEngine",
            "marketDataEngine"
        ]
        
        logger.info("清理流引擎...")
        for engine_name in stream_engines:
            try:
                session.run(f'try{{ dropStreamEngine("{engine_name}") }}catch(ex){{ print("Engine {engine_name} not found") }}')
                logger.info(f"流引擎 {engine_name} 清理完成")
            except Exception as e:
                logger.warning(f"清理流引擎 {engine_name} 失败: {e}")
        
        # 清理全局变量
        global_vars = [
            "memory_config",
            "tick_writer",
            "minute_writer",
            "snapshot_table_m",
            "k_minute_table_m",
            "snapshot_table_y",
            "k_minute_table_y"
        ]
        
        logger.info("清理全局变量...")
        for var_name in global_vars:
            try:
                session.run(f'try{{ undef("{var_name}") }}catch(ex){{ print("Variable {var_name} not found") }}')
                logger.info(f"全局变量 {var_name} 清理完成")
            except Exception as e:
                logger.warning(f"清理全局变量 {var_name} 失败: {e}")
        
        # 强制垃圾回收（如果支持）
        try:
            session.run('pnodeRun(clearAllCache)')
            logger.info("缓存清理完成")
        except Exception as e:
            logger.warning(f"缓存清理失败: {e}")
        
        # 关闭连接
        session.close()
        logger.info("市场数据录制数据库清理完成")
        
    except Exception as e:
        logger.error(f"清理过程中发生错误: {e}")
        raise


def get_database_info(host: str = "localhost", port: int = 8848, 
                     username: str = "admin", password: str = "123456"):
    """获取数据库信息"""
    try:
        session = ddb.session()
        session.connect(host, port, username, password)
        
        logger.info("获取数据库信息...")
        
        # 获取所有数据库
        try:
            databases = session.run('getDatabaseList()')
            logger.info("现有数据库:")
            for db in databases:
                if 'market_data' in str(db).lower():
                    logger.info(f"  - {db}")
        except Exception as e:
            logger.warning(f"获取数据库列表失败: {e}")
        
        # 获取共享对象
        try:
            shared_objects = session.run('objs(SHARED)')
            if not shared_objects.empty:
                logger.info("共享对象:")
                for obj in shared_objects['name']:
                    if 'stream' in str(obj).lower() or 'market' in str(obj).lower():
                        logger.info(f"  - {obj}")
        except Exception as e:
            logger.warning(f"获取共享对象失败: {e}")
        
        session.close()
        
    except Exception as e:
        logger.error(f"获取数据库信息失败: {e}")


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description="市场数据录制清理脚本")
    parser.add_argument("--host", default="localhost", help="DolphinDB主机地址")
    parser.add_argument("--port", type=int, default=8848, help="DolphinDB端口")
    parser.add_argument("--username", default="admin", help="用户名")
    parser.add_argument("--password", default="123456", help="密码")
    parser.add_argument("--info", action="store_true", help="只显示数据库信息，不执行清理")
    
    args = parser.parse_args()
    
    if args.info:
        get_database_info(args.host, args.port, args.username, args.password)
    else:
        cleanup_market_data_databases(args.host, args.port, args.username, args.password)


if __name__ == "__main__":
    main()
