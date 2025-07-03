"""
Cryptocurrency Exchange Configurations
"""

# Binance Configuration
BINANCE_CONFIG = {
    'testnet': True,
    'api_key': 'your_testnet_api_key',
    'secret': 'your_testnet_secret',
    'base_url': 'https://testnet.binancefuture.com',
    'symbols': ['BTCUSDT', 'ETHUSDT', 'ADAUSDT', 'BNBUSDT', 'SOLUSDT'],
    'ddb_host': 'localhost',
    'ddb_port': 8848,
    'ddb_user': 'admin',
    'ddb_password': '123456'
}

# OKX Configuration
OKX_CONFIG = {
    'testnet': True,
    'api_key': 'your_testnet_api_key',
    'secret': 'your_testnet_secret',
    'passphrase': 'your_passphrase',
    'base_url': 'https://www.okx.com',
    'symbols': ['BTC-USDT-SWAP', 'ETH-USDT-SWAP', 'ADA-USDT-SWAP'],
    'ddb_host': 'localhost',
    'ddb_port': 8848,
    'ddb_user': 'admin',
    'ddb_password': '123456'
}

# Bybit Configuration
BYBIT_CONFIG = {
    'testnet': True,
    'api_key': 'your_testnet_api_key',
    'secret': 'your_testnet_secret',
    'base_url': 'https://api-testnet.bybit.com',
    'symbols': ['BTCUSDT', 'ETHUSDT', 'ADAUSDT'],
    'ddb_host': 'localhost',
    'ddb_port': 8848,
    'ddb_user': 'admin',
    'ddb_password': '123456'
}

# Gate.io Configuration
GATE_CONFIG = {
    'testnet': True,
    'api_key': 'your_testnet_api_key',
    'secret': 'your_testnet_secret',
    'base_url': 'https://api.gateio.ws',
    'symbols': ['BTC_USDT', 'ETH_USDT', 'ADA_USDT'],
    'ddb_host': 'localhost',
    'ddb_port': 8848,
    'ddb_user': 'admin',
    'ddb_password': '123456'
}

# Unified Crypto Configuration
CRYPTO_CONFIGS = {
    'binance': BINANCE_CONFIG,
    'okx': OKX_CONFIG,
    'bybit': BYBIT_CONFIG,
    'gate': GATE_CONFIG
}

# Common symbols across exchanges (normalized)
COMMON_SYMBOLS = {
    'BTC': ['BTCUSDT', 'BTC-USDT-SWAP', 'BTCUSDT', 'BTC_USDT'],
    'ETH': ['ETHUSDT', 'ETH-USDT-SWAP', 'ETHUSDT', 'ETH_USDT'],
    'ADA': ['ADAUSDT', 'ADA-USDT-SWAP', 'ADAUSDT', 'ADA_USDT']
}
