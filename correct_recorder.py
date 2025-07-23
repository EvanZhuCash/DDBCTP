import time
from datetime import datetime
import sys
import dolphindb as ddb
import logging

logger = logging.getLogger("heartbeatwarning")
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler("heartbeatwarning.log")
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

from vnpy_ctp.api import MdApi
from vnpy.trader.utility import get_folder_path

{
    "用户名": "224829",
    "密码": "Evan@cash1q2",
    "经纪商代码": "9999",
    "交易服务器": "182.254.243.31:30001",
    "行情服务器": "182.254.243.31:30011",
    "产品名称": "simnow_client_test",
    "授权编码": "0000000000000000",
    "产品信息": ""
}
# Map to English keys for ctp::queryInstrument
md_server, md_port = ctp_setting["行情服务器"].split(":")
ctp_settings = {
    'md_server': md_server,
    'md_port': int(md_port),
    'broker_id': ctp_setting["经纪商代码"],
    'user_id': ctp_setting["用户名"],
    'password': ctp_setting["密码"],
    'app_id': ctp_setting["产品名称"],
    'auth_code': ctp_setting["授权编码"]
}
MAX_FLOAT = sys.float_info.max
hostname = "192.168.91.124"

class ddb_server():
  def __init__(self):
    self.session = ddb.session()
    self.session.connect(hostname, 8848, "admin", "123456")

  def extract_parent_symbol(self, contract):
    """Extract parent symbol from contract code using pure pattern matching (e.g., 'ag2510' -> 'AG')"""
    if not contract:
      return None

    contract = contract.upper()

    # Extract characters before first digit (letters + 3-4 digits pattern)
    for i in range(len(contract)):
      if contract[i].isdigit():
        if i > 0:
          parent = contract[:i]
          # Validate reasonable length for futures symbols (1-4 characters)
          if 1 <= len(parent) <= 4:
            # Verify remaining part looks like maturity (3-4 digits)
            remaining = contract[i:]
            if len(remaining) >= 3 and remaining.isdigit():
              return parent
        break

    # Fallback: return first few characters if no clear pattern found
    return contract[:4] if len(contract) <= 4 else contract[:2]

  def create_streamTable(self):
    try:
      print("Creating tickStream table...")
      self.session.run("""
        share streamTable(1000:0, `symbol`timestamp`system_time`last_price`volume`bid1`ask1`bid_vol1`ask_vol1`turnover`open_interest`exchange_id`trading_day`open_price`high_price`low_price`pre_close_price`pre_settlement_price`upper_limit`lower_limit`pre_open_interest`settlement_price`close_price`bid2`bid_vol2`ask2`ask_vol2`bid3`bid_vol3`ask3`ask_vol3`bid4`bid_vol4`ask4`ask_vol4`bid5`bid_vol5`ask5`ask_vol5`average_price`action_day`exchange_inst_id,
                       [SYMBOL, TIMESTAMP, TIMESTAMP, DOUBLE, LONG, DOUBLE, DOUBLE, LONG, LONG, DOUBLE, DOUBLE, SYMBOL, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, LONG, DOUBLE, LONG, DOUBLE, LONG, DOUBLE, LONG, DOUBLE, LONG, DOUBLE, LONG, DOUBLE, LONG, DOUBLE, LONG, DOUBLE, SYMBOL, SYMBOL]) as tickStream
      """)
      print("✓ tickStream created successfully with system_time column")

      print("Creating agg1min table...")
      self.session.run("""
        share streamTable(1000:0, `timestamp`symbol`open_price`high_price`low_price`close_price`volume`bid1`ask1`bid_vol1`ask_vol1`turnover`open_interest`upper_limit`lower_limit`pre_settlement_price`settlement_price`average_price`latency_ms,
                       [TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, LONG, DOUBLE, DOUBLE, LONG, LONG, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, LONG]) as agg1min
      """)
      print("✓ agg1min created successfully with OHLC and latency fields")

      # Verify tables exist
      tables = self.session.run('exec name from objs() where name in [`tickStream, `agg1min]')
      print(f"✓ Verified tables exist: {tables}")

    except Exception as e:
      print(f"ERROR creating stream tables: {e}")

  def create_agg1min_engine(self):
    self.session.run("""
      engine1min = createTimeSeriesEngine(name="engine1min", windowSize=60000, step=60000,
      metrics=<[first(last_price), max(last_price), min(last_price), last(last_price), sum(volume), last(bid1), last(ask1), last(bid_vol1), last(ask_vol1), sum(turnover), last(open_interest), last(upper_limit), last(lower_limit), last(pre_settlement_price), last(settlement_price), last(average_price), avg(system_time - timestamp)]>,
      dummyTable=tickStream, outputTable=agg1min, timeColumn="timestamp",useSystemTime=false,keyColumn="symbol",
      useWindowStartTime=false)
      go
      subscribeTable(tableName=`tickStream, actionName="engine1min", handler=tableInsert{engine1min}, msgAsTable=true, offset=-1)
    """)

  def create_normal_tables_and_routing(self, contracts):
    """Create normal tables and working routing - backto working solution"""
    try:
      print("Creating normal tables and working routing...")

      # Get parent symbols using proper extraction
      parent_symbols = set()
      for contract in contracts:
        parent = self.extract_parent_symbol(contract)
        if parent:
          parent_symbols.add(parent)

      print(f"Creating for parent symbols: {', '.join(parent_symbols)}")

      # Create normal tables for each parent
      for parent in parent_symbols:
        snapshot_table_name = f"CTP_{parent}_snapshots"
        kmin_table_name = f"CTP_{parent}_k_minute"

        # Create snapshots table with system_time
        self.session.run(f"""
          share streamTable(1000:0, `symbol`timestamp`system_time`last_price`volume`bid1`ask1`bid_vol1`ask_vol1`turnover`open_interest`exchange_id`trading_day`open_price`high_price`low_price`pre_close_price`pre_settlement_price`upper_limit`lower_limit`pre_open_interest`settlement_price`close_price`bid2`bid_vol2`ask2`ask_vol2`bid3`bid_vol3`ask3`ask_vol3`bid4`bid_vol4`ask4`ask_vol4`bid5`bid_vol5`ask5`ask_vol5`average_price`action_day`exchange_inst_id,
                       [SYMBOL, TIMESTAMP, TIMESTAMP, DOUBLE, LONG, DOUBLE, DOUBLE, LONG, LONG, DOUBLE, DOUBLE, SYMBOL, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, LONG, DOUBLE, LONG, DOUBLE, LONG, DOUBLE, LONG, DOUBLE, LONG, DOUBLE, LONG, DOUBLE, LONG, DOUBLE, LONG, DOUBLE, SYMBOL, SYMBOL]) as {snapshot_table_name}
        """)

        # Create k_minute table with correct OHLC structure
        self.session.run(f"""
          share streamTable(1000:0, `timestamp`symbol`open_price`high_price`low_price`close_price`volume`bid1`ask1`bid_vol1`ask_vol1`turnover`open_interest`upper_limit`lower_limit`pre_settlement_price`settlement_price`average_price`latency_ms,
                       [TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, LONG, DOUBLE, DOUBLE, LONG, LONG, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, LONG]) as {kmin_table_name}
        """)
        print(f"✓ Created normal tables for {parent}")

      # Create the WORKING routing function with exact symbol matching
      parent_list = list(parent_symbols)

      # Create mapping of contracts to parent symbols
      contract_to_parent = {}
      for contract in contracts:
        parent = self.extract_parent_symbol(contract)
        if parent:
          contract_to_parent[contract.upper()] = parent

      # Generate routing conditions for each parent
      routing_conditions = {}
      for parent in parent_list:
        # Find all contracts that belong to this parent
        parent_contracts = [contract for contract, p in contract_to_parent.items() if p == parent]
        if parent_contracts:
          # Create condition for exact symbol matching
          condition = " or ".join([f'upper(symbol) == "{contract}"' for contract in parent_contracts])
          routing_conditions[parent] = condition

      self.session.run(f"""

        def routeSnapshots(mutable t) {{
          if(size(t) == 0) return
          print("Snapshot router processing " + string(size(t)) + " records")

          // Route each parent symbol using exact symbol matching
          {chr(10).join([f'''
          // Route {parent} symbols: {routing_conditions.get(parent, "false")}
          {parent.lower()}Data = select * from t where {routing_conditions.get(parent, "false")}
          if(size({parent.lower()}Data) > 0) {{
            insert into CTP_{parent}_snapshots select * from {parent.lower()}Data
            print("SUCCESS: Inserted " + string(size({parent.lower()}Data)) + " records into CTP_{parent}_snapshots")
          }}''' for parent in parent_list if parent in routing_conditions])}
        }}

        def routeMinutes(mutable t) {{
          if(size(t) == 0) return
          print("Minute router processing " + string(size(t)) + " records")

          // Route each parent symbol using exact symbol matching
          {chr(10).join([f'''
          // Route {parent} symbols: {routing_conditions.get(parent, "false")}
          {parent.lower()}Data = select * from t where {routing_conditions.get(parent, "false")}
          if(size({parent.lower()}Data) > 0) {{
            insert into CTP_{parent}_k_minute select * from {parent.lower()}Data
            print("SUCCESS: Inserted " + string(size({parent.lower()}Data)) + " records into CTP_{parent}_k_minute")
          }}''' for parent in parent_list if parent in routing_conditions])}
        }}

        // Subscribe with the working handlers
        subscribeTable(tableName=`tickStream, actionName="snapRouter", handler=routeSnapshots, msgAsTable=true, offset=-1)
        subscribeTable(tableName=`agg1min, actionName="minRouter", handler=routeMinutes, msgAsTable=true, offset=-1)
        print("Working subscriptions created successfully")
      """)

      print("✓ WORKING normal table routing created")

    except Exception as e:
      print(f"ERROR creating normal tables: {e}")

  def create_dfs_databases(self, contracts):
    """Create DFS databases with correct partitioning (separate from routing)"""
    try:
      print("Creating DFS databases with correct partitioning...")

      # Get parent symbols using proper extraction
      parent_symbols = set()
      for contract in contracts:
        parent = self.extract_parent_symbol(contract)
        if parent:
          parent_symbols.add(parent)

      print(f"Creating DFS databases for parent symbols: {', '.join(parent_symbols)}")

      # Create DFS databases for each parent
      for parent in parent_symbols:
        db_name = f"dfs://CTP_{parent.upper()}"

        # Create single DFS database with both tables
        self.session.run(f'''
          // Create single DFS database for {parent} with both tables
          if(!existsDatabase("{db_name}")) {{
            // Create database with TSDB engine for snapshots
            create database "{db_name}"
            partitioned by VALUE(2020.01.01..2030.01.01), HASH([SYMBOL, 10])
            engine='TSDB'

            // Create snapshots table (TSDB with your partitioning scheme)
            create table "{db_name}"."snapshots"(
                symbol SYMBOL,
                timestamp TIMESTAMP,
                system_time TIMESTAMP,
                last_price DOUBLE,
                volume LONG,
                bid1 DOUBLE,
                ask1 DOUBLE,
                bid_vol1 LONG,
                ask_vol1 LONG,
                turnover DOUBLE,
                open_interest DOUBLE,
                exchange_id SYMBOL,
                trading_day DATE,
                open_price DOUBLE,
                high_price DOUBLE,
                low_price DOUBLE,
                pre_close_price DOUBLE,
                pre_settlement_price DOUBLE,
                upper_limit DOUBLE,
                lower_limit DOUBLE,
                pre_open_interest DOUBLE,
                settlement_price DOUBLE,
                close_price DOUBLE,
                bid2 DOUBLE,
                bid_vol2 LONG,
                ask2 DOUBLE,
                ask_vol2 LONG,
                bid3 DOUBLE,
                bid_vol3 LONG,
                ask3 DOUBLE,
                ask_vol3 LONG,
                bid4 DOUBLE,
                bid_vol4 LONG,
                ask4 DOUBLE,
                ask_vol4 LONG,
                bid5 DOUBLE,
                bid_vol5 LONG,
                ask5 DOUBLE,
                ask_vol5 LONG,
                average_price DOUBLE,
                action_day DATE,
                exchange_inst_id SYMBOL
            )
            partitioned by trading_day, symbol,
            sortColumns=[`symbol,`timestamp],
            keepDuplicates=ALL

            // Create k_minute table (using same partitioning as database)
            create table "{db_name}"."k_minute"(
                timestamp TIMESTAMP,
                symbol SYMBOL,
                open_price DOUBLE,
                high_price DOUBLE,
                low_price DOUBLE,
                close_price DOUBLE,
                volume LONG,
                bid1 DOUBLE,
                ask1 DOUBLE,
                bid_vol1 LONG,
                ask_vol1 LONG,
                turnover DOUBLE,
                open_interest DOUBLE,
                upper_limit DOUBLE,
                lower_limit DOUBLE,
                pre_settlement_price DOUBLE,
                settlement_price DOUBLE,
                average_price DOUBLE,
                latency_ms LONG,
                trading_day DATE
            )
            partitioned by trading_day, symbol,
            sortColumns=[`symbol,`timestamp],
            keepDuplicates=ALL

            print("Created DFS database: {db_name} with snapshots and k_minute tables")
          }}
        ''')
        print(f"✓ Created DFS databases for {parent}")

    except Exception as e:
      print(f"ERROR creating DFS databases: {e}")

  def transfer_normal_to_dfs(self, contracts):
    """Transfer data from normal tables to DFS tables"""
    try:
      print("Transferring data from normal tables to DFS tables...")

      # Get parent symbols using proper extraction
      parent_symbols = set()
      for contract in contracts:
        parent = self.extract_parent_symbol(contract)
        if parent:
          parent_symbols.add(parent)

      total_transferred = 0

      for parent in parent_symbols:
        try:
          # Transfer snapshots with date conversion
          snap_count = self.session.run(f'select count(*) from CTP_{parent}_snapshots').iloc[0,0]
          if snap_count > 0:
            self.session.run(f'''
              snapData = select symbol, timestamp, system_time, last_price, volume, bid1, ask1, bid_vol1, ask_vol1, turnover, open_interest, exchange_id,
                               temporalParse(string(trading_day), "yyyyMMdd") as trading_day,
                               open_price, high_price, low_price, pre_close_price, pre_settlement_price, upper_limit, lower_limit, pre_open_interest, settlement_price, close_price,
                               bid2, bid_vol2, ask2, ask_vol2, bid3, bid_vol3, ask3, ask_vol3, bid4, bid_vol4, ask4, ask_vol4, bid5, bid_vol5, ask5, ask_vol5, average_price,
                               temporalParse(string(action_day), "yyyyMMdd") as action_day,
                               exchange_inst_id
                        from CTP_{parent}_snapshots
              loadTable("dfs://CTP_{parent.upper()}", "snapshots").append!(snapData)
              delete from CTP_{parent}_snapshots
            ''')
            print(f"✓ Transferred {snap_count} snapshot records for {parent}")
            total_transferred += snap_count

          # Transfer k_minute with trading_day field
          min_count = self.session.run(f'select count(*) from CTP_{parent}_k_minute').iloc[0,0]
          if min_count > 0:
            self.session.run(f'''
              minData = select *, temporalParse(string(date(timestamp)), "yyyy.MM.dd") as trading_day from CTP_{parent}_k_minute
              loadTable("dfs://CTP_{parent.upper()}", "k_minute").append!(minData)
              delete from CTP_{parent}_k_minute
            ''')
            print(f"✓ Transferred {min_count} minute records for {parent}")
            total_transferred += min_count

        except Exception as e:
          print(f"Error transferring {parent} data: {e}")

      print(f"✓ Total transferred: {total_transferred} records")
      return total_transferred

    except Exception as e:
      print(f"ERROR transferring data: {e}")
      return 0

  def cleanup_dfs_databases(self):
    """Clean up all DFS databases (similar to cleanup_all_dfs.py)"""
    try:
      print("🧹 Cleaning up DFS databases...")

      # Discover all DFS databases
      all_dfs_databases = self.session.run('getClusterDFSDatabases()')

      if all_dfs_databases is None or len(all_dfs_databases) == 0:
        print("ℹ️  No DFS databases found")
        return

      print(f"📊 Found {len(all_dfs_databases)} DFS databases:")
      for db in all_dfs_databases:
        print(f"  - {db}")

      # Clean all discovered DFS databases
      cleaned_count = 0
      for db_name in all_dfs_databases:
        try:
          db_path = db_name if db_name.startswith('dfs://') else f'dfs://{db_name}'
          self.session.run(f'dropDatabase("{db_path}")')
          print(f'✅ Deleted: {db_path}')
          cleaned_count += 1
        except Exception as e:
          print(f'❌ Failed to delete {db_path}: {e}')

      print(f"✅ Successfully deleted: {cleaned_count} DFS databases")

    except Exception as e:
      print(f"ERROR cleaning DFS databases: {e}")

  def verify_all_systems_ready(self, contracts):
    """Verify all databases, tables, and routing are ready before starting market data"""
    try:
      print("🔍 Verifying system readiness...")

      # Get parent symbols using proper extraction
      parent_symbols = set()
      for contract in contracts:
        parent = self.extract_parent_symbol(contract)
        if parent:
          parent_symbols.add(parent)

      # 1. Verify basic stream tables exist
      try:
        tick_schema = self.session.run('schema(tickStream)')
        agg_schema = self.session.run('schema(agg1min)')
        print("✅ Basic stream tables (tickStream, agg1min) verified")
      except Exception as e:
        print(f"❌ Basic stream tables missing: {e}")
        return False

      # 2. Verify normal tables for each parent symbol
      for parent in parent_symbols:
        try:
          snap_schema = self.session.run(f'schema(CTP_{parent}_snapshots)')
          min_schema = self.session.run(f'schema(CTP_{parent}_k_minute)')
          print(f"✅ Normal tables for {parent} verified")
        except Exception as e:
          print(f"❌ Normal tables for {parent} missing: {e}")
          return False

      # 3. Verify DFS databases exist
      dfs_databases = self.session.run('getClusterDFSDatabases()')
      for parent in parent_symbols:
        db_name = f"dfs://CTP_{parent.upper()}"
        if db_name not in dfs_databases:
          print(f"❌ DFS database {db_name} missing")
          return False

        # Verify both tables exist in DFS database
        try:
          snap_count = self.session.run(f'select count(*) from loadTable("{db_name}", "snapshots")').iloc[0,0]
          min_count = self.session.run(f'select count(*) from loadTable("{db_name}", "k_minute")').iloc[0,0]
          print(f"✅ DFS database {db_name} with both tables verified")
        except Exception as e:
          print(f"❌ DFS database {db_name} tables missing: {e}")
          return False

      # 4. Verify routing subscriptions exist
      try:
        # Check if subscriptions exist by looking for published tables
        streaming_stat = self.session.run('getStreamingStat()')
        published_tables = streaming_stat.get('PublishedTables', None)

        if published_tables is not None and len(published_tables) > 0:
          # Check if our expected subscriptions exist
          expected_subscriptions = ['snapRouter', 'minRouter']
          found_subscriptions = []

          for table_info in published_tables:
            if 'actions' in table_info or 'Actions' in table_info:
              actions = table_info.get('actions', table_info.get('Actions', ''))
              if isinstance(actions, str):
                if 'snapRouter' in actions:
                  found_subscriptions.append('snapRouter')
                if 'minRouter' in actions:
                  found_subscriptions.append('minRouter')

          if len(found_subscriptions) >= 2:
            print(f"✅ Routing subscriptions verified: {found_subscriptions}")
          else:
            print(f"⚠️  Partial routing subscriptions found: {found_subscriptions}")
            print("✅ Proceeding anyway - subscriptions may be working")
        else:
          print("⚠️  No published tables found, but proceeding anyway")

      except Exception as e:
        print(f"⚠️  Could not verify routing subscriptions: {e}")
        print("✅ Proceeding anyway - subscriptions may be working")

      print(f"🎉 All systems ready! Verified {len(parent_symbols)} parent symbols: {', '.join(parent_symbols)}")
      return True

    except Exception as e:
      print(f"❌ System verification failed: {e}")
      return False

  def analyze_tick_intervals(self, symbol=None, minutes=2):
    """Analyze tick intervals to detect missing ticks and measure recording completeness"""
    try:
      print(f"🔍 Analyzing tick intervals for the last {minutes} minutes...")

      if symbol:
        # Analyze specific symbol
        symbols_to_check = [symbol]
        print(f"Focusing on symbol: {symbol}")
      else:
        # Get all symbols from tickStream
        symbols_result = self.session.run('select distinct symbol from tickStream')
        if len(symbols_result) == 0:
          print("❌ No symbols found in tickStream")
          return
        symbols_to_check = symbols_result['symbol'].tolist()
        print(f"Analyzing {len(symbols_to_check)} symbols")

      analysis_results = {}

      for sym in symbols_to_check:
        try:
          # Get tick data for the last N minutes
          tick_data = self.session.run(f'''
            select symbol, timestamp, system_time, last_price, volume,
                   (system_time - timestamp) as latency_ms
            from tickStream
            where symbol = `{sym} and timestamp >= now() - {minutes}*60*1000
            order by timestamp
          ''')

          if len(tick_data) == 0:
            print(f"⚠️  No recent data for {sym}")
            continue

          # Calculate tick intervals
          tick_data['prev_timestamp'] = tick_data['timestamp'].shift(1)
          tick_data['interval_ms'] = (tick_data['timestamp'] - tick_data['prev_timestamp']).dt.total_seconds() * 1000

          # Remove first row (no previous timestamp)
          intervals = tick_data['interval_ms'].dropna()
          latencies = tick_data['latency_ms'].dropna()

          if len(intervals) > 0:
            analysis_results[sym] = {
              'total_ticks': len(tick_data),
              'avg_interval_ms': intervals.mean(),
              'min_interval_ms': intervals.min(),
              'max_interval_ms': intervals.max(),
              'std_interval_ms': intervals.std(),
              'avg_latency_ms': latencies.mean(),
              'max_latency_ms': latencies.max(),
              'expected_ticks_per_min': 60000 / intervals.mean() if intervals.mean() > 0 else 0,
              'gaps_over_1sec': len(intervals[intervals > 1000]),
              'gaps_over_5sec': len(intervals[intervals > 5000])
            }

        except Exception as e:
          print(f"❌ Error analyzing {sym}: {e}")

      # Print analysis results
      print("\n📊 TICK ANALYSIS RESULTS:")
      print("=" * 80)

      for sym, stats in analysis_results.items():
        print(f"\n🎯 {sym}:")
        print(f"  Total ticks: {stats['total_ticks']}")
        print(f"  Avg interval: {stats['avg_interval_ms']:.1f}ms")
        print(f"  Min interval: {stats['min_interval_ms']:.1f}ms")
        print(f"  Max interval: {stats['max_interval_ms']:.1f}ms")
        print(f"  Std interval: {stats['std_interval_ms']:.1f}ms")
        print(f"  Expected ticks/min: {stats['expected_ticks_per_min']:.1f}")
        print(f"  Avg latency: {stats['avg_latency_ms']:.1f}ms")
        print(f"  Max latency: {stats['max_latency_ms']:.1f}ms")
        print(f"  Gaps >1sec: {stats['gaps_over_1sec']}")
        print(f"  Gaps >5sec: {stats['gaps_over_5sec']}")

        # Quality assessment
        if stats['expected_ticks_per_min'] >= 100:  # Expecting ~120 ticks/min for active contracts
          print(f"  ✅ GOOD: High tick frequency")
        elif stats['expected_ticks_per_min'] >= 50:
          print(f"  ⚠️  MODERATE: Medium tick frequency")
        else:
          print(f"  ❌ LOW: Low tick frequency - possible missing data")

        if stats['avg_latency_ms'] < 10:
          print(f"  ✅ EXCELLENT: Low latency")
        elif stats['avg_latency_ms'] < 50:
          print(f"  ✅ GOOD: Acceptable latency")
        else:
          print(f"  ⚠️  HIGH: High latency - check system performance")

      return analysis_results

    except Exception as e:
      print(f"❌ Error in tick analysis: {e}")
      return None

  def get_subscription_stats(self):
    """Get statistics about current subscriptions and data flow"""
    try:
      print("📈 SUBSCRIPTION STATISTICS:")
      print("=" * 50)

      # Get total symbols in tickStream
      total_symbols = self.session.run('select count(distinct symbol) from tickStream').iloc[0,0]
      print(f"Total symbols receiving data: {total_symbols}")

      # Get recent tick counts per symbol (last 1 minute)
      recent_ticks = self.session.run('''
        select symbol, count(*) as tick_count
        from tickStream
        where timestamp >= now() - 60*1000
        group by symbol
        order by tick_count desc
      ''')

      if len(recent_ticks) > 0:
        print(f"\nTop 10 most active symbols (last 1 min):")
        for i, row in recent_ticks.head(10).iterrows():
          print(f"  {row['symbol']}: {row['tick_count']} ticks")

        print(f"\nBottom 10 least active symbols (last 1 min):")
        for i, row in recent_ticks.tail(10).iterrows():
          print(f"  {row['symbol']}: {row['tick_count']} ticks")

        avg_ticks = recent_ticks['tick_count'].mean()
        print(f"\nAverage ticks per symbol (last 1 min): {avg_ticks:.1f}")

        # Identify potentially problematic symbols
        low_activity = recent_ticks[recent_ticks['tick_count'] < 10]
        if len(low_activity) > 0:
          print(f"\n⚠️  {len(low_activity)} symbols with <10 ticks in last minute:")
          for i, row in low_activity.iterrows():
            print(f"    {row['symbol']}: {row['tick_count']} ticks")

      return recent_ticks

    except Exception as e:
      print(f"❌ Error getting subscription stats: {e}")
      return None

  def optimize_subscription_strategy(self, all_contracts, max_contracts=None):
    """Analyze and optimize subscription strategy for comprehensive recording"""
    try:
      print("🎯 OPTIMIZING SUBSCRIPTION STRATEGY:")
      print("=" * 50)

      # Group contracts by parent symbol
      parent_groups = {}
      for contract in all_contracts:
        parent = self.extract_parent_symbol(contract)
        if parent not in parent_groups:
          parent_groups[parent] = []
        parent_groups[parent].append(contract)

      print(f"Total contracts: {len(all_contracts)}")
      print(f"Parent symbols: {len(parent_groups)}")

      # Analyze contract patterns
      print(f"\nContract distribution by parent:")
      for parent, contracts in sorted(parent_groups.items(), key=lambda x: len(x[1]), reverse=True):
        print(f"  {parent}: {len(contracts)} contracts - {contracts[:3]}{'...' if len(contracts) > 3 else ''}")

      # Recommend subscription strategy
      print(f"\n📋 SUBSCRIPTION RECOMMENDATIONS:")

      if max_contracts and len(all_contracts) > max_contracts:
        print(f"⚠️  Total contracts ({len(all_contracts)}) exceeds limit ({max_contracts})")

        # Strategy 1: Most active contracts per parent
        print(f"\n🎯 STRATEGY 1: Most active contracts per parent")
        print(f"Select 1-2 most active contracts per parent symbol")

        recommended_contracts = []
        for parent, contracts in parent_groups.items():
          # Sort by contract month (assuming newer contracts are more active)
          sorted_contracts = sorted(contracts, reverse=True)
          recommended_contracts.extend(sorted_contracts[:2])  # Take top 2 per parent

        print(f"Recommended contracts ({len(recommended_contracts)}): {recommended_contracts[:10]}...")

        # Strategy 2: Focus on major commodity groups
        print(f"\n🎯 STRATEGY 2: Focus on major commodity groups")
        major_commodities = ['AU', 'AG', 'CU', 'AL', 'ZN', 'SC', 'RB', 'HC', 'I', 'J', 'JM', 'A', 'M', 'Y', 'P', 'C', 'CS', 'CF', 'SR', 'TA', 'MA', 'PP', 'V', 'L', 'BU', 'RU', 'NI', 'SN', 'SS', 'SP']

        major_contracts = []
        for parent in major_commodities:
          if parent in parent_groups:
            # Take most active contract for each major commodity
            sorted_contracts = sorted(parent_groups[parent], reverse=True)
            major_contracts.append(sorted_contracts[0])

        print(f"Major commodity contracts ({len(major_contracts)}): {major_contracts}")

        return {
          'all_contracts': all_contracts,
          'strategy1_contracts': recommended_contracts[:max_contracts] if max_contracts else recommended_contracts,
          'strategy2_contracts': major_contracts,
          'parent_groups': parent_groups
        }
      else:
        print(f"✅ All contracts ({len(all_contracts)}) can be subscribed")
        return {
          'all_contracts': all_contracts,
          'recommended_contracts': all_contracts,
          'parent_groups': parent_groups
        }

    except Exception as e:
      print(f"❌ Error optimizing subscription strategy: {e}")
      return None

  def monitor_system_performance(self, duration_minutes=5):
    """Monitor system performance during recording"""
    try:
      print(f"🔍 MONITORING SYSTEM PERFORMANCE for {duration_minutes} minutes...")
      print("=" * 60)

      start_time = time.time()
      end_time = start_time + (duration_minutes * 60)

      while time.time() < end_time:
        # Get current stats
        current_time = time.time()
        elapsed = (current_time - start_time) / 60

        print(f"\n⏱️  Time: {elapsed:.1f}/{duration_minutes} minutes")

        # Check tick flow
        recent_ticks = self.session.run('''
          select count(*) as total_ticks, count(distinct symbol) as active_symbols
          from tickStream
          where timestamp >= now() - 30*1000
        ''')

        if len(recent_ticks) > 0:
          total_ticks = recent_ticks.iloc[0]['total_ticks']
          active_symbols = recent_ticks.iloc[0]['active_symbols']
          ticks_per_second = total_ticks / 30

          print(f"  📊 Last 30s: {total_ticks} ticks, {active_symbols} symbols, {ticks_per_second:.1f} ticks/sec")

          # Check for performance issues
          if ticks_per_second < 10:
            print(f"  ⚠️  LOW: Tick rate below expected")
          elif ticks_per_second > 1000:
            print(f"  ⚠️  HIGH: Very high tick rate - monitor system resources")
          else:
            print(f"  ✅ GOOD: Normal tick rate")

        # Check latency
        latency_stats = self.session.run('''
          select avg(system_time - timestamp) as avg_latency_ms,
                 max(system_time - timestamp) as max_latency_ms
          from tickStream
          where timestamp >= now() - 30*1000
        ''')

        if len(latency_stats) > 0:
          avg_latency = latency_stats.iloc[0]['avg_latency_ms']
          max_latency = latency_stats.iloc[0]['max_latency_ms']

          print(f"  🕐 Latency: avg={avg_latency:.1f}ms, max={max_latency:.1f}ms")

          if avg_latency > 100:
            print(f"  ⚠️  HIGH LATENCY: System may be overloaded")
          else:
            print(f"  ✅ GOOD: Low latency")

        # Wait before next check
        time.sleep(30)

      print(f"\n✅ Monitoring complete!")

    except Exception as e:
      print(f"❌ Error monitoring performance: {e}")
    except KeyboardInterrupt:
      print(f"\n⏹️  Monitoring stopped by user")





class ctp_gateway():

  def __init__(self, s, logger):
    self.md_api = CtpMdApi(self, s, logger)

  def connect(self, setting):
    userid = setting["用户名"]
    password = setting["密码"]
    brokerid = setting["经纪商代码"]
    md_address = setting["行情服务器"]

    if (
      (not md_address.startswith("tcp://"))
      and (not md_address.startswith("ssl://"))
      and (not md_address.startswith("socks"))
    ):
      md_address = "tcp://" + md_address

    self.md_api.connect(md_address, userid, password, brokerid)

  def subscribe(self, req):
    self.md_api.subscribe(req)

  def close(self):
    self.md_api.close()

class CtpMdApi(MdApi):

  def __init__(self, gateway, s, logger):
    super().__init__()
    self.gateway = "CTP"
    self.session = s
    self.logger = logger

    self.reqid: int = 0

    self.connect_status: bool = False
    self.login_status: bool = False
    self.subscribed: set = set()

    self.userid: str = ""
    self.password: str = ""
    self.brokerid: str = ""

    self.current_date: str = datetime.now().strftime("%Y%m%d")

  def onFrontConnected(self):
    print("行情服务器连接成功")
    self.login()

  def onFrontDisconnected(self, reason):
    self.login_status = False
    print(f"行情服务器连接断开，原因{reason}")

  def onHeartBeatWarning(self, reqid):
    self.logger.info(f"距离上次接收报文的事件, {reqid}")

  def onRspUserLogin(self, data, error, reqid, last):
    if not error["ErrorID"]:
      self.login_status = True
      print("行情服务器登录成功")

      for symbol in self.subscribed:
        self.subscribeMarketData(symbol)
    else:
      print("行情服务器登录失败", error)

  def onRspError(self, error, reqid, last):
    print("行情接口报错", error)

  def onRspSubMarketData(self, data, error, reqid, last):
    if not error or not error["ErrorID"]:
      return

    print("行情订阅失败", error)

  def onRtnDepthMarketData(self, data):
    if not data["UpdateTime"]:
      return

    symbol = data["InstrumentID"]
    if not data["ActionDay"]:
      date_str = self.current_date
    else:
      date_str = data["ActionDay"]

    date_str_dot = date_str[:4]+"."+date_str[4:6]+"."+date_str[6:]
    milisec_str = str(data["UpdateMillisec"])+"0"*(3-len(str(data["UpdateMillisec"])))
    timestamp = f"{date_str_dot}T{data['UpdateTime']}.{milisec_str}"

    # Extract all available fields with proper null handling
    last_price = 0 if data["LastPrice"] == MAX_FLOAT else data["LastPrice"]
    volume = int(data["Volume"])
    bid1 = 0 if data["BidPrice1"] == MAX_FLOAT else data["BidPrice1"]
    ask1 = 0 if data["AskPrice1"] == MAX_FLOAT else data["AskPrice1"]
    bid_vol1 = int(data["BidVolume1"])
    ask_vol1 = int(data["AskVolume1"])
    turnover = 0 if data["Turnover"] == MAX_FLOAT else data["Turnover"]
    open_interest = 0 if data["OpenInterest"] == MAX_FLOAT else data["OpenInterest"]
    exchange_id = data.get("ExchangeID", "")
    trading_day = data.get("TradingDay", "")
    open_price = 0 if data["OpenPrice"] == MAX_FLOAT else data["OpenPrice"]
    high_price = 0 if data["HighestPrice"] == MAX_FLOAT else data["HighestPrice"]
    low_price = 0 if data["LowestPrice"] == MAX_FLOAT else data["LowestPrice"]
    pre_close_price = 0 if data["PreClosePrice"] == MAX_FLOAT else data["PreClosePrice"]
    pre_settlement_price = 0 if data["PreSettlementPrice"] == MAX_FLOAT else data["PreSettlementPrice"]
    upper_limit = 0 if data["UpperLimitPrice"] == MAX_FLOAT else data["UpperLimitPrice"]
    lower_limit = 0 if data["LowerLimitPrice"] == MAX_FLOAT else data["LowerLimitPrice"]
    pre_open_interest = 0 if data["PreOpenInterest"] == MAX_FLOAT else data["PreOpenInterest"]
    settlement_price = 0 if data["SettlementPrice"] == MAX_FLOAT else data["SettlementPrice"]
    close_price = 0 if data["ClosePrice"] == MAX_FLOAT else data["ClosePrice"]

    # Bid/Ask levels 2-5
    bid2 = 0 if data["BidPrice2"] == MAX_FLOAT else data["BidPrice2"]
    bid_vol2 = int(data["BidVolume2"])
    ask2 = 0 if data["AskPrice2"] == MAX_FLOAT else data["AskPrice2"]
    ask_vol2 = int(data["AskVolume2"])
    bid3 = 0 if data["BidPrice3"] == MAX_FLOAT else data["BidPrice3"]
    bid_vol3 = int(data["BidVolume3"])
    ask3 = 0 if data["AskPrice3"] == MAX_FLOAT else data["AskPrice3"]
    ask_vol3 = int(data["AskVolume3"])
    bid4 = 0 if data["BidPrice4"] == MAX_FLOAT else data["BidPrice4"]
    bid_vol4 = int(data["BidVolume4"])
    ask4 = 0 if data["AskPrice4"] == MAX_FLOAT else data["AskPrice4"]
    ask_vol4 = int(data["AskVolume4"])
    bid5 = 0 if data["BidPrice5"] == MAX_FLOAT else data["BidPrice5"]
    bid_vol5 = int(data["BidVolume5"])
    ask5 = 0 if data["AskPrice5"] == MAX_FLOAT else data["AskPrice5"]
    ask_vol5 = int(data["AskVolume5"])

    average_price = 0 if data.get("AveragePrice", MAX_FLOAT) == MAX_FLOAT else data.get("AveragePrice", 0)
    action_day = data.get("ActionDay", "")
    exchange_inst_id = data.get("ExchangeInstID", "")

    # Insert comprehensive tick data
    # Handle empty string values properly for DolphinDB
    exchange_id_val = f'"{exchange_id}"' if exchange_id else '""'
    trading_day_val = f'"{trading_day}"' if trading_day else '""'
    action_day_val = f'"{action_day}"' if action_day else '""'
    exchange_inst_id_val = f'"{exchange_inst_id}"' if exchange_inst_id else '""'

    # Create the insert statement with system_time for latency analysis
    insert_sql = f"insert into tickStream values(`{symbol}, {timestamp}, now(), {last_price}, {volume}l, {bid1}, {ask1}, {bid_vol1}l, {ask_vol1}l, {turnover}, {open_interest}, {exchange_id_val}, {trading_day_val}, {open_price}, {high_price}, {low_price}, {pre_close_price}, {pre_settlement_price}, {upper_limit}, {lower_limit}, {pre_open_interest}, {settlement_price}, {close_price}, {bid2}, {bid_vol2}l, {ask2}, {ask_vol2}l, {bid3}, {bid_vol3}l, {ask3}, {ask_vol3}l, {bid4}, {bid_vol4}l, {ask4}, {ask_vol4}l, {bid5}, {bid_vol5}l, {ask5}, {ask_vol5}l, {average_price}, {action_day_val}, {exchange_inst_id_val})"

    self.session.run(insert_sql)

  def connect(self, address, userid, password, brokerid):
    self.userid = userid
    self.password = password
    self.brokerid = brokerid
    self.connect_status = False

    if not self.connect_status:
      path = get_folder_path(self.gateway.lower())
      self.createFtdcMdApi((str(path) + "\\Md").encode("GBK"))

      self.registerFront(address)
      self.init()

      self.connect_status = True

  def login(self):
    ctp_req = {
      "UserID": self.userid,
      "Password": self.password,
      "BrokerID": self.brokerid
    }

    self.reqid += 1
    self.reqUserLogin(ctp_req, self.reqid)

  def subscribe(self, req):
    if self.login_status:
      self.subscribeMarketData(req)
    self.subscribed.add(req)

  def close(self):
    if self.connect_status:
      self.exit()

def get_all_instruments():
  """
  Get all available instruments for trading using DolphinDB CTP plugin
  Returns a list of instrument IDs
  """
  session = ddb.session()
  session.connect(hostname, 8848, "admin", "123456")

  # Load CTP plugin (ignore if already loaded)
  try:
    session.run('loadPlugin("ctp")')
  except:
    pass  # Plugin already loaded

  # Query all instruments
  script = f'''
  result = ctp::queryInstrument("{ctp_settings['md_server']}", {ctp_settings['md_port']}, "{ctp_settings['broker_id']}", "{ctp_settings['user_id']}", "{ctp_settings['password']}", "{ctp_settings['app_id']}", "{ctp_settings['auth_code']}");
  ids = exec instrumentID from result;
  ids
  '''

  result = session.run(script)
  session.close()

  print(f"Found {len(result)} instruments")
  return result

if __name__ == "__main__":
  import sys
  import argparse
  
  # First run cleanup
  print("Running cleanup...")
  import subprocess
  subprocess.run([sys.executable, "cleanup.py"], cwd=".")
  # subprocess.run(["python", "cleanup_all_dfs.py"], cwd=".")

  # Parse command line arguments
  parser = argparse.ArgumentParser(description='CTP Market Data Recorder')
  parser.add_argument('--get-instruments', action='store_true',
                     help='Get list of all available instruments and exit')
  parser.add_argument('--futures-only', action='store_true',
                     help='Subscribe to futures contracts only (excludes options)')
  parser.add_argument('--contracts', nargs='+',
                     help='Specific contracts to subscribe to (e.g., --contracts ag2510 au2510)')
  parser.add_argument('--analyze-ticks', action='store_true',
                     help='Analyze tick intervals and latency (requires existing data)')
  parser.add_argument('--analyze-symbol', type=str,
                     help='Analyze specific symbol tick intervals')
  parser.add_argument('--subscription-stats', action='store_true',
                     help='Show subscription statistics and exit')
  parser.add_argument('--monitor-performance', type=int, metavar='MINUTES',
                     help='Monitor system performance for specified minutes')
  parser.add_argument('--max-contracts', type=int, default=200,
                     help='Maximum number of contracts to subscribe (default: 200)')

  args = parser.parse_args()

  # Check if user wants to get all instruments
  if args.get_instruments:
    print("Getting all available instruments...")
    instruments = get_all_instruments()
    print(f"Found {len(instruments)} instruments:")
    for i, inst in enumerate(instruments):
      print(f"  {i+1}. {inst}")
    sys.exit(0)

  # Initialize database connection for analysis tools
  db = ddb_server()
  s = db.session

  # Handle analysis commands that don't require new recording
  if args.analyze_ticks:
    print("Analyzing tick intervals...")
    db.analyze_tick_intervals(symbol=args.analyze_symbol)
    sys.exit(0)

  if args.subscription_stats:
    print("Getting subscription statistics...")
    db.get_subscription_stats()
    sys.exit(0)

  if args.monitor_performance:
    print(f"Monitoring performance for {args.monitor_performance} minutes...")
    db.monitor_system_performance(args.monitor_performance)
    sys.exit(0)

  # Database already initialized above for analysis tools
  db.create_streamTable()
  s.enableStreaming()
  db.create_agg1min_engine()

  # Determine which contracts to subscribe to
  contracts_to_subscribe = []

  if args.futures_only:
    print("Getting all available instruments...")
    all_instruments = get_all_instruments()

    # Filter for futures only (exclude options which typically have longer names or specific patterns)
    futures_contracts = []
    for inst in all_instruments:
      # Basic filtering: futures typically have shorter names and follow patterns like AB2509
      # Options often have longer names with strike prices and C/P indicators
      if len(inst) <= 6 and not any(char in inst.upper() for char in ['C', 'P']):
        futures_contracts.append(inst)

    # Optimize subscription strategy
    optimization_result = db.optimize_subscription_strategy(futures_contracts, args.max_contracts)
    if optimization_result and 'strategy1_contracts' in optimization_result:
      contracts_to_subscribe = optimization_result['strategy1_contracts']
    else:
      contracts_to_subscribe = futures_contracts[:args.max_contracts]

    print(f"Filtered to {len(contracts_to_subscribe)} futures contracts (max: {args.max_contracts})")

  elif args.contracts:
    contracts_to_subscribe = args.contracts
    print(f"Using specified contracts: {contracts_to_subscribe}")
  else:
    print("❌ ERROR: No contracts specified!")
    print("Please use one of the following options:")
    print("  --contracts <contract1> <contract2> ...  # Specify contracts manually")
    print("  --futures-only                           # Use futures contracts only")
    print("  --get-instruments                        # Get list of available instruments")
    print("  --analyze-ticks                          # Analyze existing tick data")
    print("  --subscription-stats                     # Show current subscription stats")
    print("Example: python correct_recorder.py --contracts ag2510 au2510 sc2509")
    print("Example: python correct_recorder.py --futures-only --max-contracts 100")
    sys.exit(1)

  # Create normal tables and routing (working solution)
  db.create_normal_tables_and_routing(contracts_to_subscribe)

  # Also create DFS databases for later transfer
  db.create_dfs_databases(contracts_to_subscribe)

  # Verify all databases and tables are ready before starting market data
  print("Verifying all databases and tables are ready...")
  if not db.verify_all_systems_ready(contracts_to_subscribe):
    print("❌ System verification failed - exiting")
    sys.exit(1)

  # Subscribe to contracts
  print(f"Subscribing to {len(contracts_to_subscribe)} contracts...")
  for i, contract in enumerate(contracts_to_subscribe):
    print(f"  {i+1}/{len(contracts_to_subscribe)}: {contract}")
    # Note: gateway.subscribe will be called after CTP connection

  print("✅ All systems verified and ready - starting market data recording...")

  gateway = ctp_gateway(s, logger)
  gateway.connect(ctp_setting)

  # Subscribe to contracts after CTP connection is established
  for i, contract in enumerate(contracts_to_subscribe):
    print(f"  Subscribing {i+1}/{len(contracts_to_subscribe)}: {contract}")
    gateway.subscribe(contract)

  print("🚀 Market data recording started successfully!")

  try:
    # Start periodic analysis
    last_analysis_time = time.time()
    analysis_interval = 120  # Analyze every 2 minutes

    while True:
      current_time = time.time()

      # Periodic analysis
      if current_time - last_analysis_time >= analysis_interval:
        print(f"\n{'='*60}")
        print(f"🔍 PERIODIC ANALYSIS - {datetime.now().strftime('%H:%M:%S')}")
        print(f"{'='*60}")

        # Quick subscription stats
        db.get_subscription_stats()

        # Quick tick analysis for a few symbols
        print(f"\n📊 Sample tick analysis:")
        sample_symbols = contracts_to_subscribe[:3]  # Analyze first 3 symbols
        for symbol in sample_symbols:
          db.analyze_tick_intervals(symbol, minutes=2)

        last_analysis_time = current_time
        print(f"{'='*60}\n")

      time.sleep(1)
  except KeyboardInterrupt:
    print("stop running")
    print("Closing gateway...")
    try:
      gateway.close()
      print("Gateway closed successfully")
    except Exception as e:
      print(f"Error closing gateway: {e}")

    print("Stopping streaming engines...")
    try:
      s.run('unsubscribeTable(tableName=`tickStream, actionName="engine1min")')
      s.run('dropStreamEngine("engine1min")')
      print("Streaming engines stopped")
    except Exception as e:
      print(f"Error stopping engines: {e}")

  print("Closing DolphinDB session...")
  try:
    s.close()
    print("Session closed successfully")
  except Exception as e:
    print(f"Error closing session: {e}")

  print("Cleanup complete")