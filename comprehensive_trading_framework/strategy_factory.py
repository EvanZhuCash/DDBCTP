#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Strategy Factory for Comprehensive Trading Framework
策略工厂 - 支持动态策略创建和管理
"""

import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, List
from config import STRATEGY_CONFIG, TIMEFRAME_CONFIG

logger = logging.getLogger(__name__)

class BaseStrategy(ABC):
    """策略基类"""
    
    def __init__(self, name: str, config: Dict[str, Any]):
        self.name = name
        self.config = config
        self.enabled = config.get("enabled", False)
        
    @abstractmethod
    def get_factor_calculation(self) -> str:
        """返回因子计算的DolphinDB脚本"""
        pass
    
    @abstractmethod
    def get_signal_logic(self) -> str:
        """返回信号逻辑的DolphinDB脚本"""
        pass
    
    @abstractmethod
    def get_execution_logic(self) -> str:
        """返回执行逻辑的DolphinDB脚本"""
        pass

class BollingerBandStrategy(BaseStrategy):
    """布林带策略"""
    
    def get_factor_calculation(self) -> str:
        period = self.config.get("period", 20)
        std_dev = self.config.get("std_dev", 2.0)
        
        return f"""
            // 布林带因子计算
            use ta
            bollinger_metrics = <[
                timestamp, price,
                ta::bBands(price, timePeriod={period}, nbdevUp={std_dev}, nbdevDn={std_dev}, maType=0)[0] as `bb_upper,
                ta::bBands(price, timePeriod={period}, nbdevUp={std_dev}, nbdevDn={std_dev}, maType=0)[1] as `bb_middle,
                ta::bBands(price, timePeriod={period}, nbdevUp={std_dev}, nbdevDn={std_dev}, maType=0)[2] as `bb_lower,
                // 信号计算
                iif(price < ta::bBands(price, timePeriod={period}, nbdevUp={std_dev}, nbdevDn={std_dev}, maType=0)[2], 1, 0) as `long_signal,
                iif(price > ta::bBands(price, timePeriod={period}, nbdevUp={std_dev}, nbdevDn={std_dev}, maType=0)[0], 1, 0) as `short_signal,
                iif(abs(price - ta::bBands(price, timePeriod={period}, nbdevUp={std_dev}, nbdevDn={std_dev}, maType=0)[1]) < 0.01, 1, 0) as `close_signal
            ]>
            
            createReactiveStateEngine(name="bollinger_{self.name}",
                metrics=bollinger_metrics, dummyTable=agg1min, 
                outputTable=bollinger_stream, keyColumn="symbol", keepOrder=true)
        """
    
    def get_signal_logic(self) -> str:
        return """
            def bollinger_signal_logic(symbol, price, bb_upper, bb_middle, bb_lower, prev_price){
                long_condition = (price < bb_lower) && (prev_price >= bb_lower)
                short_condition = (price > bb_upper) && (prev_price <= bb_upper)
                close_long_condition = price >= bb_middle
                close_short_condition = price <= bb_middle
                
                signals = dict(STRING, INT)
                signals["OPEN_LONG"] = iif(long_condition, 1, 0)
                signals["OPEN_SHORT"] = iif(short_condition, 1, 0)
                signals["CLOSE_LONG"] = iif(close_long_condition, 1, 0)
                signals["CLOSE_SHORT"] = iif(close_short_condition, 1, 0)
                
                return signals
            }
        """
    
    def get_execution_logic(self) -> str:
        volume = self.config.get("volume_per_order", 5)
        
        return f"""
            def bollinger_execution(mutable posDict, symbol, long_signal, short_signal, close_signal, price, timestamp){{
                current_pos = posDict.get(symbol[0], "FLAT")
                
                if (current_pos == "FLAT"){{
                    if (long_signal[0] == 1){{
                        insert into orderStream values(rand(1..10000, 1)[0], symbol[0], timestamp[0], price[0], {volume}, "OPEN_LONG")
                        posDict[symbol[0]] = "LONG"
                    }} else if (short_signal[0] == 1){{
                        insert into orderStream values(rand(1..10000, 1)[0], symbol[0], timestamp[0], price[0], {volume}, "OPEN_SHORT")
                        posDict[symbol[0]] = "SHORT"
                    }}
                }} else if (current_pos == "LONG"){{
                    if (close_signal[0] == 1){{
                        pos_qty = exec qty from positionTable where symbol = symbol[0] and dir = "LONG"
                        if(size(pos_qty) > 0){{
                            insert into orderStream values(rand(1..10000, 1)[0], symbol[0], timestamp[0], price[0], pos_qty[0], "CLOSE_LONG")
                            posDict[symbol[0]] = "FLAT"
                        }}
                    }}
                }} else if (current_pos == "SHORT"){{
                    if (close_signal[0] == 1){{
                        pos_qty = exec qty from positionTable where symbol = symbol[0] and dir = "SHORT"
                        if(size(pos_qty) > 0){{
                            insert into orderStream values(rand(1..10000, 1)[0], symbol[0], timestamp[0], price[0], pos_qty[0], "CLOSE_SHORT")
                            posDict[symbol[0]] = "FLAT"
                        }}
                    }}
                }}
            }}
        """

class MovingAverageCrossStrategy(BaseStrategy):
    """移动平均交叉策略"""
    
    def get_factor_calculation(self) -> str:
        fast_period = self.config.get("fast_period", 5)
        slow_period = self.config.get("slow_period", 20)
        
        return f"""
            // 移动平均因子计算
            ma_metrics = <[
                timestamp, price,
                mavg(price, {fast_period}) as ma_fast,
                mavg(price, {slow_period}) as ma_slow,
                iif(mavg(price, {fast_period}) > mavg(price, {slow_period}), 1, 0) as `long_signal,
                iif(mavg(price, {fast_period}) < mavg(price, {slow_period}), 1, 0) as `short_signal,
                iif(abs(mavg(price, {fast_period}) - mavg(price, {slow_period})) < 0.01, 1, 0) as `close_signal
            ]>
            
            createReactiveStateEngine(name="ma_cross_{self.name}",
                metrics=ma_metrics, dummyTable=agg1min, 
                outputTable=ma_cross_stream, keyColumn="symbol", keepOrder=true)
        """
    
    def get_signal_logic(self) -> str:
        return """
            def ma_cross_signal_logic(symbol, price, ma_fast, ma_slow, prev_ma_fast, prev_ma_slow){
                long_condition = (ma_fast > ma_slow) && (prev_ma_fast <= prev_ma_slow)
                short_condition = (ma_fast < ma_slow) && (prev_ma_fast >= prev_ma_slow)
                
                signals = dict(STRING, INT)
                signals["OPEN_LONG"] = iif(long_condition, 1, 0)
                signals["OPEN_SHORT"] = iif(short_condition, 1, 0)
                signals["CLOSE_LONG"] = iif(short_condition, 1, 0)
                signals["CLOSE_SHORT"] = iif(long_condition, 1, 0)
                
                return signals
            }
        """
    
    def get_execution_logic(self) -> str:
        volume = self.config.get("volume_per_order", 3)
        
        return f"""
            def ma_cross_execution(mutable posDict, symbol, long_signal, short_signal, close_signal, price, timestamp){{
                current_pos = posDict.get(symbol[0], "FLAT")
                
                if (current_pos == "FLAT"){{
                    if (long_signal[0] == 1){{
                        insert into orderStream values(rand(1..10000, 1)[0], symbol[0], timestamp[0], price[0], {volume}, "OPEN_LONG")
                        posDict[symbol[0]] = "LONG"
                    }} else if (short_signal[0] == 1){{
                        insert into orderStream values(rand(1..10000, 1)[0], symbol[0], timestamp[0], price[0], {volume}, "OPEN_SHORT")
                        posDict[symbol[0]] = "SHORT"
                    }}
                }} else if (current_pos == "LONG"){{
                    if (close_signal[0] == 1){{
                        pos_qty = exec qty from positionTable where symbol = symbol[0] and dir = "LONG"
                        if(size(pos_qty) > 0){{
                            insert into orderStream values(rand(1..10000, 1)[0], symbol[0], timestamp[0], price[0], pos_qty[0], "CLOSE_LONG")
                            posDict[symbol[0]] = "FLAT"
                        }}
                    }}
                }} else if (current_pos == "SHORT"){{
                    if (close_signal[0] == 1){{
                        pos_qty = exec qty from positionTable where symbol = symbol[0] and dir = "SHORT"
                        if(size(pos_qty) > 0){{
                            insert into orderStream values(rand(1..10000, 1)[0], symbol[0], timestamp[0], price[0], pos_qty[0], "CLOSE_SHORT")
                            posDict[symbol[0]] = "FLAT"
                        }}
                    }}
                }}
            }}
        """

class StrategyFactory:
    """策略工厂类"""
    
    def __init__(self):
        self.strategies = {}
        self.strategy_classes = {
            "bollinger": BollingerBandStrategy,
            "ma_cross": MovingAverageCrossStrategy
        }
    
    def create_strategy(self, strategy_type: str, name: str, config: Dict[str, Any]) -> BaseStrategy:
        """创建策略实例"""
        if strategy_type not in self.strategy_classes:
            raise ValueError(f"Unknown strategy type: {strategy_type}")
        
        strategy_class = self.strategy_classes[strategy_type]
        strategy = strategy_class(name, config)
        
        self.strategies[name] = strategy
        logger.info(f"Created strategy: {name} ({strategy_type})")
        
        return strategy
    
    def get_strategy(self, name: str) -> BaseStrategy:
        """获取策略实例"""
        return self.strategies.get(name)
    
    def get_enabled_strategies(self) -> List[BaseStrategy]:
        """获取启用的策略"""
        return [strategy for strategy in self.strategies.values() if strategy.enabled]
    
    def load_strategies_from_config(self) -> List[BaseStrategy]:
        """从配置文件加载策略"""
        strategies = []
        
        for strategy_name, strategy_config in STRATEGY_CONFIG.items():
            if strategy_config.get("enabled", False):
                # 根据策略名称推断策略类型
                if "bollinger" in strategy_name.lower():
                    strategy_type = "bollinger"
                elif "ma" in strategy_name.lower():
                    strategy_type = "ma_cross"
                else:
                    logger.warning(f"Cannot determine strategy type for {strategy_name}")
                    continue
                
                try:
                    strategy = self.create_strategy(strategy_type, strategy_name, strategy_config)
                    strategies.append(strategy)
                except Exception as e:
                    logger.error(f"Failed to create strategy {strategy_name}: {e}")
        
        return strategies
    
    def generate_combined_script(self) -> str:
        """生成组合策略脚本"""
        enabled_strategies = self.get_enabled_strategies()
        
        if not enabled_strategies:
            logger.warning("No enabled strategies found")
            return ""
        
        script_parts = []
        
        # 添加因子计算部分
        script_parts.append("// === Factor Calculations ===")
        for strategy in enabled_strategies:
            script_parts.append(f"// {strategy.name} factor calculation")
            script_parts.append(strategy.get_factor_calculation())
            script_parts.append("")
        
        # 添加信号逻辑部分
        script_parts.append("// === Signal Logic ===")
        for strategy in enabled_strategies:
            script_parts.append(f"// {strategy.name} signal logic")
            script_parts.append(strategy.get_signal_logic())
            script_parts.append("")
        
        # 添加执行逻辑部分
        script_parts.append("// === Execution Logic ===")
        for strategy in enabled_strategies:
            script_parts.append(f"// {strategy.name} execution logic")
            script_parts.append(strategy.get_execution_logic())
            script_parts.append("")
        
        return "\n".join(script_parts)

def create_strategy_factory() -> StrategyFactory:
    """创建策略工厂实例"""
    factory = StrategyFactory()
    factory.load_strategies_from_config()
    return factory

if __name__ == "__main__":
    # 测试策略工厂
    factory = create_strategy_factory()
    
    print("Loaded strategies:")
    for strategy in factory.get_enabled_strategies():
        print(f"  - {strategy.name} ({strategy.__class__.__name__})")
    
    print("\nGenerated combined script:")
    print(factory.generate_combined_script())
