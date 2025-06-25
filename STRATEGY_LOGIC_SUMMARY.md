# Trading Strategy Logic Summary

## Overview
This document summarizes the current trading strategy logic implemented in `test_live_simulate.py` - a complete DolphinDB-CTP integrated trading system.

## System Architecture

### Core Components
1. **Live Market Data Reception** (CTP → DolphinDB)
2. **Order Generation Engine** (Tick-based strategy)
3. **Order-to-Trade Conversion** (Simulation layer)
4. **Position Management** (Real-time calculation)
5. **Risk Management** (PnL tracking)

## Strategy Logic Details

### 1. Order Generation Strategy
**Location**: `simulate_order()` function
**Trigger**: Every 20 ticks of market data

```dolphindb
def order_simulator(mutable orderStream, msg){
    time_ = msg.timestamp[0]
    sym_ = msg.symbol[0]
    price_ = msg.price[0]
    
    // Increment global tick counter
    globalTickCounter = globalTickCounter + 1
    orderid = count(orderStream)

    // Generate order every 20 ticks for testing
    if (mod(globalTickCounter, 20) == 0) {
        insert into orderStream values(orderid, sym_, time_, price_, 1, "OPEN_LONG")
        print("Generated OPEN_LONG order #" + string(orderid) + " for " + string(sym_) + " at price " + string(price_) + " (tick #" + string(globalTickCounter) + ")")
    }
}
```

**Strategy Rules**:
- **Frequency**: Every 20 market ticks
- **Direction**: OPEN_LONG only (simplified for testing)
- **Volume**: Fixed 1 lot per order
- **Price**: Current market price (last tick price)

### 2. Order-to-Trade Conversion
**Location**: `order_to_trade_converter()` function
**Purpose**: Simulate trade execution from orders

```dolphindb
def order_to_trade(mutable trades, msg){
    // Extract order details
    orderid = msg.orderID[0]
    sym = msg.symbol[0]
    timestamp = msg.timestamp[0]
    price = msg.orderPrice[0]
    volume = msg.orderVolume[0]
    direction = msg.direction[0]

    // Convert order direction to trade direction and offset
    if (direction == "OPEN_LONG") {
        dir = "LONG"
        offset = "OPEN"
    } else if (direction == "CLOSE_LONG") {
        dir = "SELL"
        offset = "CLOSE"
    }
    // ... similar for SHORT positions

    // Insert trade with execution delay simulation
    insert into trades values(sym, timestamp + 100, price, volume, dir, offset, "CFFEX")
}
```

**Conversion Rules**:
- **Execution Delay**: +100ms simulation
- **Fill Rate**: 100% (all orders filled)
- **Price**: Order price (no slippage simulation)
- **Exchange**: CFFEX (China Financial Futures Exchange)

### 3. Position Management
**Location**: `positions()` function
**Purpose**: Calculate and maintain position state from trades

```dolphindb
def updatePosition(mutable pos_table, mutable positionTable, mutable realized_positions, msg){
    // Extract trade details
    sym = msg.symbol[0]
    vol = msg.qty[0]
    price = msg.price[0]
    dir = msg.dir[0]
    offset = msg.offset[0]
    
    if (offset == "OPEN"){
        // Add to position queue
        pos_table.append!(table(sym as sym1, timestamp as timestamp1, price as price1, vol as qty1, dir as dir1))
    } else if (offset == "CLOSE"){
        // FIFO position closing logic
        // Calculate realized PnL
        // Update position queue
    }
    
    // Calculate current position summary
    queue = select * from pos_table where sym1 = sym and dir1 = dir order by timestamp1 asc
    if (size(queue) > 0) {
        totalVol = sum(queue.qty1)
        totalCost = sum(queue.qty1 * queue.price1)
        avgPrice = totalCost / totalVol
        insert into positionTable values(sym, totalVol, avgPrice, dir)
    }
}
```

**Position Rules**:
- **Method**: FIFO (First In, First Out)
- **Calculation**: Volume-weighted average price
- **Tracking**: Separate long/short positions
- **PnL**: Real-time unrealized and realized PnL

### 4. Risk Management
**Location**: Unrealized PnL calculation
**Purpose**: Real-time risk monitoring

```dolphindb
def unrealizedpnl(mutable unrealized_positions, pnl_calc, positionTable, msg){
    sym_ = msg.symbol[0]
    price_ = msg.price[0]
    time_ = msg.timestamp[0]
    
    posi_ = select * from positionTable where symbol=sym_
    if (size(posi_) > 0){
        dir_ = posi_.dir[0]
        cost_price_ = posi_.price[0]
        vol_ = posi_.qty[0]
        pnl = pnl_calc(price_, cost_price_, vol_, dir_)
        insert into unrealized_positions values(sym_, time_, price_, pnl, dir_)
    }
}
```

## Data Flow Architecture

```
CTP Market Data → tickStream
                     ↓
                 Order Generation (every 20 ticks)
                     ↓
                 orderStream
                     ↓
                 Order-to-Trade Conversion
                     ↓
                 trades
                     ↓
                 Position Calculation
                     ↓
                 positionTable + unrealized_positions
```

## Current Strategy Characteristics

### Strengths
- **Real-time Processing**: All components work with live market data
- **Complete Pipeline**: End-to-end order-to-position flow
- **Risk Monitoring**: Real-time PnL calculation
- **Scalable Architecture**: DolphinDB streaming engine

### Current Limitations
- **Simple Strategy**: Only OPEN_LONG orders every 20 ticks
- **No Market Conditions**: Doesn't consider market trends, volatility, etc.
- **Fixed Parameters**: No dynamic position sizing or timing
- **No Risk Limits**: No stop-loss, position limits, or risk controls

## Files Structure
- **Main System**: `test_live_simulate.py` - Complete trading system
- **Cleanup**: `cleanup.py` - Environment cleanup utility
- **Reference**: `proven_to_work/` - Previously working implementations

## Next Steps for Strategy Enhancement
1. **Market-based Signals**: Replace tick counting with technical indicators
2. **Dynamic Position Sizing**: Volume based on volatility or account size
3. **Risk Controls**: Stop-loss, position limits, drawdown controls
4. **Multiple Instruments**: Expand beyond single IC2509 contract
5. **Strategy Parameters**: Configurable strategy parameters
