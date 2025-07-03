"""
Cleanup script for IB Multi-Gateway Trading System
Cleans up DolphinDB tables and stream engines
"""

import dolphindb as ddb
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def cleanup_dolphindb(host='localhost', port=8848, user='admin', password='123456'):
    """清理DolphinDB表和流引擎"""
    
    try:
        # Connect to DolphinDB
        session = ddb.session()
        session.connect(host, port, user, password)
        logger.info("Connected to DolphinDB for cleanup")
        
        # List of tables to clean up
        tables_to_cleanup = [
            'ib_tick_stream',
            'ib_order_stream', 
            'ib_position_stream',
            'binance_tick_stream',
            'binance_order_stream',
            'unified_tick_stream',
            'unified_order_stream',
            'unified_position_stream',
            'gateway_status'
        ]
        
        # Drop stream tables
        for table_name in tables_to_cleanup:
            try:
                result = session.run(f"""
                    if(existsTable("{table_name}")){{
                        dropStreamTable(`{table_name})
                        print("Dropped table: {table_name}")
                    }} else {{
                        print("Table not found: {table_name}")
                    }}
                """)
                logger.info(f"Cleanup result for {table_name}: {result}")
                
            except Exception as e:
                logger.warning(f"Failed to cleanup table {table_name}: {e}")
        
        # Clean up any stream engines (if they exist)
        try:
            # Get list of all stream engines
            engines = session.run("getStreamEngineNames()")
            if engines:
                logger.info(f"Found stream engines: {engines}")
                
                for engine in engines:
                    try:
                        session.run(f"dropStreamEngine('{engine}')")
                        logger.info(f"Dropped stream engine: {engine}")
                    except Exception as e:
                        logger.warning(f"Failed to drop stream engine {engine}: {e}")
            else:
                logger.info("No stream engines found")
                
        except Exception as e:
            logger.warning(f"Failed to cleanup stream engines: {e}")
        
        # Clean up any shared variables related to the system
        try:
            shared_vars = session.run("getSharedVariables()")
            if shared_vars:
                ib_vars = [var for var in shared_vars if 'ib_' in var.lower() or 'gateway' in var.lower()]
                for var in ib_vars:
                    try:
                        session.run(f"undef('{var}')")
                        logger.info(f"Undefined shared variable: {var}")
                    except Exception as e:
                        logger.warning(f"Failed to undefine variable {var}: {e}")
                        
        except Exception as e:
            logger.warning(f"Failed to cleanup shared variables: {e}")
        
        # Force garbage collection
        try:
            session.run("gc()")
            logger.info("Forced garbage collection")
        except Exception as e:
            logger.warning(f"Failed to force garbage collection: {e}")
        
        session.close()
        logger.info("DolphinDB cleanup completed successfully")
        
    except Exception as e:
        logger.error(f"Failed to cleanup DolphinDB: {e}")
        raise


def cleanup_log_files():
    """清理日志文件"""
    import os
    import glob
    
    try:
        # Clean up log files in current directory
        log_files = glob.glob("*.log")
        for log_file in log_files:
            try:
                os.remove(log_file)
                logger.info(f"Removed log file: {log_file}")
            except Exception as e:
                logger.warning(f"Failed to remove log file {log_file}: {e}")
                
    except Exception as e:
        logger.warning(f"Failed to cleanup log files: {e}")


def main():
    """主清理函数"""
    logger.info("Starting IB Multi-Gateway system cleanup...")
    
    try:
        # Cleanup DolphinDB
        cleanup_dolphindb()
        
        # Cleanup log files
        cleanup_log_files()
        
        logger.info("System cleanup completed successfully")
        
    except Exception as e:
        logger.error(f"System cleanup failed: {e}")
        return False
    
    return True


if __name__ == "__main__":
    print("IB Multi-Gateway Trading System Cleanup")
    print("This will clean up all DolphinDB tables and stream engines.")
    
    confirm = input("Are you sure you want to proceed? (y/n): ").strip().lower()
    if confirm == 'y':
        success = main()
        if success:
            print("Cleanup completed successfully!")
        else:
            print("Cleanup failed. Check logs for details.")
    else:
        print("Cleanup cancelled.")
