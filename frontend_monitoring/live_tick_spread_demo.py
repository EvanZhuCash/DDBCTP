#!/usr/bin/env python3
"""
实时Tick和价差演示 - 显示实时tick数据和bid/ask价差
"""

import dolphindb as ddb
import pandas as pd
import numpy as np
import time
import threading
from datetime import datetime
import random
import os

class LiveTickSpreadDemo:
    def __init__(self):
        self.session = ddb.session()
        self.session.connect("localhost", 8848, "admin", "123456")
        self.is_running = False
        self.current_price = 4500.0
        self.bid_price = 4499.5
        self.ask_price = 4500.5
        self.last_price = 4500.0
        self.volume = 0
        self.position = 0
        self.total_pnl = 0.0
        self.trade_count = 0
        self.position_price = 0.0
        self.tick_count = 0
        
    def setup_tick_tables(self):
        """设置tick数据表"""
        try:
            print("🔧 设置实时tick数据表...")
            
            # 创建详细的tick数据表
            self.session.run("""
            // 创建实时tick数据表
            share streamTable(1000:0, `symbol`timestamp`last_price`bid_price`ask_price`bid_volume`ask_volume`volume`turnover`spread, 
                [SYMBOL, TIMESTAMP, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, DOUBLE, DOUBLE]) as live_tick_stream
            
            // 创建价差统计表
            share streamTable(1000:0, `symbol`timestamp`spread`spread_pct`mid_price, 
                [SYMBOL, TIMESTAMP, DOUBLE, DOUBLE, DOUBLE]) as spread_stats
            
            // 创建tick统计表
            share streamTable(1000:0, `symbol`timestamp`tick_count`avg_spread`min_spread`max_spread`total_volume, 
                [SYMBOL, TIMESTAMP, INT, DOUBLE, DOUBLE, DOUBLE, INT]) as tick_summary
            """)
            
            print("✅ Tick数据表创建成功")
            
        except Exception as e:
            print(f"❌ 创建tick表失败: {e}")
    
    def generate_realistic_tick(self):
        """生成真实的tick数据"""
        # 基于布朗运动生成价格
        drift = random.uniform(-0.0005, 0.0005)
        volatility = 0.015
        dt = 1.0
        
        # 价格变动
        price_change = drift * self.current_price * dt + \
                      volatility * self.current_price * np.sqrt(dt) * random.gauss(0, 1)
        
        self.current_price += price_change
        self.current_price = max(4400, min(4600, self.current_price))
        self.current_price = round(self.current_price, 2)
        
        # 生成bid/ask价格
        spread_base = random.uniform(0.5, 2.0)  # 基础价差
        spread_volatility = random.uniform(0.1, 0.5)  # 价差波动
        
        half_spread = spread_base + spread_volatility * random.gauss(0, 1)
        half_spread = max(0.25, min(3.0, half_spread))  # 限制价差范围
        
        self.bid_price = round(self.current_price - half_spread, 2)
        self.ask_price = round(self.current_price + half_spread, 2)
        self.last_price = self.current_price
        
        # 生成成交量
        self.volume = random.randint(1, 50)
        
        # 生成bid/ask量
        bid_volume = random.randint(5, 100)
        ask_volume = random.randint(5, 100)
        
        # 计算价差
        spread = self.ask_price - self.bid_price
        spread_pct = (spread / self.current_price) * 100
        mid_price = (self.bid_price + self.ask_price) / 2
        
        # 计算成交额
        turnover = self.last_price * self.volume
        
        return {
            'last_price': self.last_price,
            'bid_price': self.bid_price,
            'ask_price': self.ask_price,
            'bid_volume': bid_volume,
            'ask_volume': ask_volume,
            'volume': self.volume,
            'turnover': turnover,
            'spread': spread,
            'spread_pct': spread_pct,
            'mid_price': mid_price
        }
    
    def insert_tick_data(self, tick_data):
        """插入tick数据到DolphinDB"""
        try:
            current_time = datetime.now()
            
            # 插入实时tick数据
            timestamp_str = current_time.strftime('%Y.%m.%dT%H:%M:%S.%f')[:-3]
            self.session.run(f"""
            t = table(`IC2311 as symbol, timestamp("{timestamp_str}") as timestamp,
                     {tick_data['last_price']} as last_price, {tick_data['bid_price']} as bid_price,
                     {tick_data['ask_price']} as ask_price, {tick_data['bid_volume']} as bid_volume,
                     {tick_data['ask_volume']} as ask_volume, {tick_data['volume']} as volume,
                     {tick_data['turnover']} as turnover, {tick_data['spread']} as spread)
            live_tick_stream.append!(t)
            """)
            
            # 插入价差统计
            self.session.run(f"""
            t2 = table(`IC2311 as symbol, timestamp("{timestamp_str}") as timestamp,
                       {tick_data['spread']} as spread, {tick_data['spread_pct']} as spread_pct,
                       {tick_data['mid_price']} as mid_price)
            spread_stats.append!(t2)
            """)
            
            # 每10个tick插入一次汇总统计
            if self.tick_count % 10 == 0:
                # 计算最近10个tick的统计
                recent_stats = self.session.run("""
                select
                    avg(s.spread) as avg_spread,
                    min(s.spread) as min_spread,
                    max(s.spread) as max_spread,
                    sum(t.volume) as total_volume
                from spread_stats s
                left join live_tick_stream t on s.symbol = t.symbol and s.timestamp = t.timestamp
                where s.symbol = `IC2311
                order by s.timestamp desc
                limit 10
                """)
                
                if recent_stats is not None and len(recent_stats) > 0:
                    stats = recent_stats.iloc[0]
                    self.session.run(f"""
                    t3 = table(`IC2311 as symbol, timestamp("{timestamp_str}") as timestamp,
                               {self.tick_count} as tick_count, {stats['avg_spread']} as avg_spread,
                               {stats['min_spread']} as min_spread, {stats['max_spread']} as max_spread,
                               {stats['total_volume']} as total_volume)
                    tick_summary.append!(t3)
                    """)
            
        except Exception as e:
            print(f"❌ 插入tick数据错误: {e}")
    
    def generate_signal_from_spread(self, tick_data):
        """基于价差生成交易信号"""
        signal = "NO_TRADE"
        
        # 基于价差的简单策略
        if tick_data['spread_pct'] < 0.02:  # 价差很小时
            if random.random() < 0.2:  # 20%概率
                if self.position == 0:
                    if tick_data['last_price'] < tick_data['mid_price']:
                        signal = "OPEN_LONG"
                    elif tick_data['last_price'] > tick_data['mid_price']:
                        signal = "OPEN_SHORT"
                elif self.position > 0 and tick_data['last_price'] > self.position_price * 1.005:
                    signal = "CLOSE_LONG"
                elif self.position < 0 and tick_data['last_price'] < self.position_price * 0.995:
                    signal = "CLOSE_SHORT"
        
        return signal
    
    def execute_trade(self, signal_type, price, timestamp):
        """执行交易"""
        try:
            qty = 1
            
            if signal_type in ['OPEN_LONG', 'CLOSE_SHORT']:
                if self.position <= 0:
                    if self.position < 0:  # 平空
                        pnl = abs(self.position) * (self.position_price - price)
                        self.total_pnl += pnl
                        self.trade_count += 1
                        
                    self.position = qty
                    self.position_price = price
                    
            elif signal_type in ['OPEN_SHORT', 'CLOSE_LONG']:
                if self.position >= 0:
                    if self.position > 0:  # 平多
                        pnl = self.position * (price - self.position_price)
                        self.total_pnl += pnl
                        self.trade_count += 1
                        
                    self.position = -qty
                    self.position_price = price
            
            # 插入交易记录
            order_timestamp_str = timestamp.strftime('%Y.%m.%dT%H:%M:%S.%f')[:-3]
            self.session.run(f"""
            t4 = table({self.trade_count + 1} as order_id, `IC2311 as symbol, timestamp("{order_timestamp_str}") as timestamp,
                       {price} as price, {qty} as qty, `{signal_type} as signal_type)
            orderStream.append!(t4)
            """)
            
            print(f"💰 交易: {signal_type} @ {price} | 持仓: {self.position} | PnL: {self.total_pnl:.2f}")
            
        except Exception as e:
            print(f"❌ 执行交易错误: {e}")
    
    def simulate_live_ticks(self):
        """模拟实时tick数据"""
        while self.is_running:
            try:
                self.tick_count += 1
                
                # 生成tick数据
                tick_data = self.generate_realistic_tick()
                current_time = datetime.now()
                
                # 插入数据到DolphinDB
                self.insert_tick_data(tick_data)
                
                # 生成交易信号
                signal = self.generate_signal_from_spread(tick_data)
                
                if signal != "NO_TRADE":
                    print(f"🚨 信号: {signal} @ {tick_data['last_price']}")
                    self.execute_trade(signal, tick_data['last_price'], current_time)
                    
                    # 插入信号数据
                    signal_timestamp_str = current_time.strftime('%Y.%m.%dT%H:%M:%S.%f')[:-3]
                    self.session.run(f"""
                    t5 = table(`IC2311 as symbol, timestamp("{signal_timestamp_str}") as timestamp,
                               {tick_data['last_price']} as close, "{signal}" as signal)
                    signalST.append!(t5)
                    """)
                
                # 显示tick信息
                print(f"📊 Tick #{self.tick_count}: IC2311 | "
                      f"Last: {tick_data['last_price']:.2f} | "
                      f"Bid: {tick_data['bid_price']:.2f}({tick_data['bid_volume']}) | "
                      f"Ask: {tick_data['ask_price']:.2f}({tick_data['ask_volume']}) | "
                      f"Spread: {tick_data['spread']:.2f}({tick_data['spread_pct']:.3f}%) | "
                      f"Vol: {tick_data['volume']}")
                
                time.sleep(0.5)  # 每0.5秒一个tick，更快的更新
                
            except Exception as e:
                print(f"❌ 模拟tick错误: {e}")
                time.sleep(1)
    
    def start_simulation(self, duration=30):
        """开始模拟"""
        print(f"🚀 开始{duration}秒实时Tick和价差演示...")
        
        self.setup_tick_tables()
        self.position_price = self.current_price
        
        self.is_running = True
        
        # 启动tick模拟线程
        tick_thread = threading.Thread(target=self.simulate_live_ticks)
        tick_thread.daemon = True
        tick_thread.start()
        
        print(f"📊 实时tick数据流运行中...")
        print(f"🌐 前端地址: http://localhost:5000")
        print(f"📈 策略页面: http://localhost:5000/strategy")
        print(f"⏱️ 运行时间: {duration}秒")
        print(f"📡 Tick频率: 每0.5秒")
        
        # 运行指定时间
        time.sleep(duration)
        
        self.stop_simulation()
    
    def stop_simulation(self):
        """停止模拟"""
        print("🛑 停止tick数据模拟...")
        self.is_running = False
        
        # 显示最终统计
        print("\n📊 Tick数据统计:")
        print(f"   总Tick数: {self.tick_count}")
        print(f"   总交易次数: {self.trade_count}")
        print(f"   总盈亏: {self.total_pnl:.2f}")
        print(f"   当前持仓: {self.position}")
        print(f"   最终价格: {self.current_price:.2f}")
        print(f"   最终价差: {self.ask_price - self.bid_price:.2f}")

def main():
    """主函数"""
    print("=" * 70)
    print("🎯 DolphinDB CTP 实时Tick和价差演示")
    print("=" * 70)
    
    demo = LiveTickSpreadDemo()
    
    try:
        # 运行30秒模拟
        demo.start_simulation(duration=30)
        
        print("\n✅ Tick演示完成！")
        print("💡 提示: 前端页面会显示实时数据，您可以:")
        print("   1. 查看实时tick数据和价差")
        print("   2. 监控bid/ask价格变化")
        print("   3. 观察价差统计和趋势")
        
    except KeyboardInterrupt:
        print("\n⚠️ 用户中断演示")
        demo.stop_simulation()
    except Exception as e:
        print(f"\n❌ 演示过程中发生错误: {e}")
        demo.stop_simulation()
    finally:
        print("\n🔚 程序结束")

if __name__ == "__main__":
    main()
