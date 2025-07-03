"""
Interactive Brokers Configuration
"""

# IB Gateway Configuration
IB_CONFIG = {
    # IB Connection Settings
    'host': '127.0.0.1',
    'port': 7497,  # TWS Paper Trading: 7497, Live: 7496, Gateway Paper: 4002, Gateway Live: 4001
    'client_id': 1,
    
    # DolphinDB Connection Settings
    'ddb_host': 'localhost',
    'ddb_port': 8848,
    'ddb_user': 'admin',
    'ddb_password': '123456',
    
    # Contract Definitions
    'contracts': [
        {
            'symbol': 'AAPL',
            'type': 'STK',
            'exchange': 'SMART',
            'currency': 'USD'
        },
        {
            'symbol': 'MSFT',
            'type': 'STK',
            'exchange': 'SMART',
            'currency': 'USD'
        },
        {
            'symbol': 'GOOGL',
            'type': 'STK',
            'exchange': 'SMART',
            'currency': 'USD'
        },
        {
            'symbol': 'ES',
            'type': 'FUT',
            'exchange': 'CME',
            'currency': 'USD',
            'lastTradeDateOrContractMonth': '20241220'  # Update this for current contract
        },
        {
            'symbol': 'NQ',
            'type': 'FUT',
            'exchange': 'CME',
            'currency': 'USD',
            'lastTradeDateOrContractMonth': '20241220'  # Update this for current contract
        },
        {
            'symbol': 'YM',
            'type': 'FUT',
            'exchange': 'CBOT',
            'currency': 'USD',
            'lastTradeDateOrContractMonth': '20241220'  # Update this for current contract
        }
    ]
}

# Test Configuration for Paper Trading
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
        },
        {
            'symbol': 'SPY',
            'type': 'STK',
            'exchange': 'SMART',
            'currency': 'USD'
        }
    ]
}

# Live Configuration (use with caution)
IB_LIVE_CONFIG = {
    'host': '127.0.0.1',
    'port': 7496,  # Live trading port
    'client_id': 1,
    'ddb_host': 'localhost',
    'ddb_port': 8848,
    'ddb_user': 'admin',
    'ddb_password': '123456',
    'contracts': []  # Define contracts for live trading
}
