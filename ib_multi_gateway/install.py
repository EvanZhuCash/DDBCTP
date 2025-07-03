"""
Installation script for IB Multi-Gateway Trading System
"""

import subprocess
import sys
import os
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def check_python_version():
    """检查Python版本"""
    version = sys.version_info
    if version.major < 3 or (version.major == 3 and version.minor < 8):
        logger.error("Python 3.8+ is required")
        return False
    
    logger.info(f"Python version: {version.major}.{version.minor}.{version.micro}")
    return True


def install_requirements():
    """安装依赖包"""
    try:
        logger.info("Installing required packages...")
        
        # Install core dependencies first
        core_packages = [
            'dolphindb>=1.30.22.5',
            'ib_insync>=0.9.86',
            'pandas>=2.0.0',
            'numpy>=1.24.0',
            'aiohttp>=3.8.0',
            'loguru>=0.7.0'
        ]
        
        for package in core_packages:
            logger.info(f"Installing {package}...")
            result = subprocess.run([
                sys.executable, '-m', 'pip', 'install', package
            ], capture_output=True, text=True)
            
            if result.returncode != 0:
                logger.error(f"Failed to install {package}: {result.stderr}")
                return False
        
        logger.info("Core packages installed successfully")
        
        # Install optional packages
        optional_packages = [
            'vnpy>=3.9.0',
            'pytest>=7.0.0',
            'pytest-asyncio>=0.21.0'
        ]
        
        for package in optional_packages:
            logger.info(f"Installing optional package {package}...")
            result = subprocess.run([
                sys.executable, '-m', 'pip', 'install', package
            ], capture_output=True, text=True)
            
            if result.returncode != 0:
                logger.warning(f"Failed to install optional package {package}: {result.stderr}")
        
        return True
        
    except Exception as e:
        logger.error(f"Failed to install requirements: {e}")
        return False


def check_dolphindb_connection():
    """检查DolphinDB连接"""
    try:
        import dolphindb as ddb
        
        session = ddb.session()
        session.connect("localhost", 8848, "admin", "123456")
        
        # Test basic query
        result = session.run("1+1")
        if result == 2:
            logger.info("DolphinDB connection successful")
            session.close()
            return True
        else:
            logger.error("DolphinDB connection test failed")
            session.close()
            return False
            
    except ImportError:
        logger.error("DolphinDB package not installed")
        return False
    except Exception as e:
        logger.error(f"DolphinDB connection failed: {e}")
        return False


def check_ib_insync():
    """检查ib_insync包"""
    try:
        import ib_insync
        logger.info(f"ib_insync version: {ib_insync.__version__}")
        return True
    except ImportError:
        logger.error("ib_insync package not installed")
        return False
    except Exception as e:
        logger.error(f"ib_insync check failed: {e}")
        return False


def create_test_config():
    """创建测试配置文件"""
    try:
        config_content = """
# Test Configuration
# Copy this to your actual configuration and modify as needed

IB_TEST_CONFIG = {
    'host': '127.0.0.1',
    'port': 7497,  # Paper trading port
    'client_id': 1,
    'ddb_host': 'localhost',
    'ddb_port': 8848,
    'ddb_user': 'admin',
    'ddb_password': '123456',
    'contracts': [
        {
            'symbol': 'AAPL',
            'type': 'STK',
            'exchange': 'SMART',
            'currency': 'USD'
        }
    ]
}
"""
        
        with open('test_config.py', 'w') as f:
            f.write(config_content)
        
        logger.info("Test configuration file created: test_config.py")
        return True
        
    except Exception as e:
        logger.error(f"Failed to create test config: {e}")
        return False


def main():
    """主安装函数"""
    logger.info("Starting IB Multi-Gateway Trading System installation...")
    
    # Check Python version
    if not check_python_version():
        return False
    
    # Install requirements
    if not install_requirements():
        logger.error("Failed to install requirements")
        return False
    
    # Check DolphinDB connection
    logger.info("Checking DolphinDB connection...")
    if not check_dolphindb_connection():
        logger.warning("DolphinDB connection failed. Please ensure DolphinDB server is running.")
    
    # Check ib_insync
    if not check_ib_insync():
        logger.error("ib_insync check failed")
        return False
    
    # Create test configuration
    create_test_config()
    
    logger.info("Installation completed successfully!")
    logger.info("\nNext steps:")
    logger.info("1. Ensure DolphinDB server is running on localhost:8848")
    logger.info("2. Install and configure TWS or IB Gateway for paper trading")
    logger.info("3. Run: python tests/test_ib_gateway.py")
    logger.info("4. Run: python main.py")
    
    return True


if __name__ == "__main__":
    success = main()
    if not success:
        sys.exit(1)
