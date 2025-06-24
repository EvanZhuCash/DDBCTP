import dolphindb as ddb
import pandas as pd
import time

# Set pandas display options for better output
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth', None)

def cleanup_ddb(host="localhost", port=8848, username="admin", password="123456"):
    """
    Comprehensive DolphinDB cleanup script that combines all cleanup functionality:
    - Inspects current streaming state
    - Unsubscribes from all active subscriptions
    - Drops all stream engines (dynamically discovered)
    - Undefines all shared tables and variables (dynamically discovered)
    - Verifies cleanup completion

    Args:
        host (str): DolphinDB server host (default: "localhost")
        port (int): DolphinDB server port (default: 8848)
        username (str): Username (default: "admin")
        password (str): Password (default: "123456")
    """
    s = ddb.session()
    try:
        print(f"Connecting to DolphinDB at {host}:{port}...")
        s.connect(host, port, username, password)
        print("Connected to DolphinDB.")

        # 1. Inspect the current streaming state
        print("\n--- Current Streaming Publisher Tables ---")
        try:
            pub_tables = s.run("getStreamingStat().pubTables")
            print(pub_tables)
        except Exception as e:
            print(f"Could not get publisher tables: {e}")
            pub_tables = pd.DataFrame()

        print("\n--- Current Streaming Publisher Connections ---")
        try:
            pub_conns = s.run("getStreamingStat().pubConns")
            print(pub_conns)
        except Exception as e:
            print(f"Could not get publisher connections: {e}")

        # 2. Skip subscription topics check (function not available in this DolphinDB version)

        print("\n--- Starting Comprehensive Cleanup ---")
        
        # 3. Unsubscribe from all active subscriptions (dynamic approach)
        if not pub_tables.empty:
            for index, row in pub_tables.iterrows():
                table_name = row['tableName']
                actions = row['actions']
                # Handle both single action and list of actions
                action_list = actions if isinstance(actions, list) else [actions]
                for action in action_list:
                    print(f"Unsubscribing from {table_name} with action {action}")
                    s.run(f"try {{ unsubscribeTable(tableName='{table_name}', actionName='{action}') }} catch(ex) {{}}")

        # 4. Get all shared tables dynamically and clean them up
        print("Getting all shared tables...")
        try:
            shared_tables = s.run("objs(SHARED)")
            print(f"Found {len(shared_tables)} shared objects: {list(shared_tables['name']) if not shared_tables.empty else 'None'}")
        except Exception as e:
            print(f"Could not get shared tables: {e}")
            shared_tables = pd.DataFrame()

        # 5. Get all stream engines dynamically using the proper DolphinDB function
        print("Getting all stream engines...")
        try:
            stream_engines = s.run("getStreamEngineList()")
            if not stream_engines.empty:
                engine_names = list(stream_engines['engineName'])
                print(f"Found {len(engine_names)} stream engines: {engine_names}")
            else:
                engine_names = []
                print("No stream engines found.")
        except Exception as e:
            print(f"Could not get stream engines via getStreamEngineList: {e}")
            engine_names = []

        # 6. Execute dynamic cleanup script
        # Pass the discovered engine names to the DolphinDB script
        engine_names_str = str(engine_names).replace("'", '"')  # Convert to DolphinDB format

        cleanup_script = f"""
        // Get all shared tables dynamically
        sharedObjs = objs(SHARED)
        if (size(sharedObjs) > 0) {{
            print("Cleaning up " + string(size(sharedObjs)) + " shared objects...")
            for (i in 0..(size(sharedObjs)-1)) {{
                objName = sharedObjs.name[i]
                objType = sharedObjs.type[i]
                print("Undefining shared " + objType + ": " + objName)
                try {{
                    undef(objName, SHARED)
                }} catch(ex) {{
                    print("Failed to undefine " + objName + ": " + ex)
                }}
            }}
        }} else {{
            print("No shared objects found to clean up.")
        }}

        // Drop dynamically discovered stream engines
        discoveredEngines = {engine_names_str}
        if (size(discoveredEngines) > 0) {{
            print("Attempting to drop " + string(size(discoveredEngines)) + " discovered stream engines...")
            for (engine in discoveredEngines) {{
                try {{
                    dropStreamEngine(engine)
                    print("Dropped stream engine: " + engine)
                }} catch(ex) {{
                    print("Failed to drop engine " + engine + ": " + ex)
                }}
            }}
        }} else {{
            print("No stream engines to drop.")
        }}

        print("Dynamic cleanup completed.")
        """
        
        print("Executing comprehensive cleanup script...")
        s.run(cleanup_script)
        print("Cleanup script executed successfully.")
        
        # 5. Verify cleanup completion
        print("\n--- Verifying Cleanup ---")
        try:
            pub_tables_after = s.run("getStreamingStat().pubTables")
            if pub_tables_after.empty:
                print("✅ All publisher tables have been cleared successfully.")
            else:
                print("⚠️  Some publisher tables still exist:")
                print(pub_tables_after)
        except Exception as e:
            print(f"Could not verify publisher tables: {e}")
        
        # Skip subscription topics verification (function not available)
    
    except Exception as e:
        print(f"\n❌ An error occurred during cleanup: {e}")
    
    finally:
        try:
            s.close()
            print("\n🔌 Connection closed.")
        except:
            print("\n⚠️  Failed to close connection.")
        
        # Brief pause to ensure cleanup completion
        time.sleep(1)

if __name__ == "__main__":
    import sys

    # Parse command line arguments for connection parameters
    host = "localhost"
    port = 8848
    username = "admin"
    password = "123456"

    # Simple argument parsing
    if len(sys.argv) > 1:
        host = sys.argv[1]
    if len(sys.argv) > 2:
        port = int(sys.argv[2])
    if len(sys.argv) > 3:
        username = sys.argv[3]
    if len(sys.argv) > 4:
        password = sys.argv[4]

    print(f"🚀 Starting cleanup with connection: {username}@{host}:{port}")
    cleanup_ddb(host, port, username, password)
    print("\n🎉 Cleanup process completed!")
