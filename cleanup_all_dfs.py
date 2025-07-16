"""
Standalone DFS Cleanup Script
Properly removes all CTP DFS databases
"""

import dolphindb as ddb

def cleanup_all_dfs():
    """Dynamically discover and clean ALL DFS databases"""
    print("🧹 DYNAMICALLY DISCOVERING AND CLEANING ALL DFS DATABASES...")
    
    session = ddb.session()
    session.connect('192.168.91.124', 8848, 'admin', '123456')
    
    try:
        # Dynamically discover ALL existing DFS databases
        print("🔍 Discovering all existing DFS databases...")
        all_dfs_databases = session.run('getClusterDFSDatabases()')
        
        if all_dfs_databases is None or len(all_dfs_databases) == 0:
            print("ℹ️  No DFS databases found in the cluster")
            session.close()
            return
        
        print(f"📊 Found {len(all_dfs_databases)} DFS databases:")
        for db in all_dfs_databases:
            print(f"  - {db}")
        
        # Clean all discovered DFS databases
        cleaned_count = 0
        failed_count = 0
        
        for db_name in all_dfs_databases:
            try:
                # Use the full database path
                db_path = db_name if db_name.startswith('dfs://') else f'dfs://{db_name}'
                session.run(f'dropDatabase("{db_path}")')
                print(f'✅ Deleted: {db_path}')
                cleaned_count += 1
            except Exception as e:
                print(f'❌ Failed to delete {db_path}: {e}')
                failed_count += 1
        
        print(f"\n🎯 CLEANUP SUMMARY:")
        print(f"✅ Successfully deleted: {cleaned_count} databases")
        if failed_count > 0:
            print(f"❌ Failed to delete: {failed_count} databases")
        
        # Verify cleanup by checking remaining databases
        remaining_dbs = session.run('getClusterDFSDatabases()')
        if remaining_dbs is None or len(remaining_dbs) == 0:
            print("🎉 All DFS databases successfully removed!")
        else:
            print(f"⚠️  {len(remaining_dbs)} databases still remain: {remaining_dbs}")
            
    except Exception as e:
        print(f"❌ Error during DFS discovery: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    cleanup_all_dfs()