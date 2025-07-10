#!/usr/bin/env python3
"""
DolphinDB Complete Cleanup Script

This script cleans up ALL DolphinDB shared objects, subscriptions, and streaming engines.
It handles the S03000 error by properly unsubscribing from published tables before cleanup.

Usage: python original_cleanup.py
"""

import dolphindb as ddb
import sys

def connect_to_dolphindb(host="192.168.91.91", port=8848, user="admin", password="123456"):
    """Connect to DolphinDB server"""
    try:
        session = ddb.session()
        session.connect(host, port, user, password)
        print(f"✅ Connected to DolphinDB at {host}:{port}")
        return session
    except Exception as e:
        print(f"❌ Failed to connect to DolphinDB: {e}")
        return None

def cleanup_published_tables(session):
    """Clean up published tables and their subscriptions to fix S03000 error"""
    print("\n🔍 Cleaning up published tables and subscriptions...")

    total_cleaned = 0

    try:
        # Get published tables - this is the key to fixing S03000 error
        pub_tables = session.run("getStreamingStat().pubTables")

        if pub_tables is not None and not pub_tables.empty:
            print(f"  Found {len(pub_tables)} published tables")

            # Process each published table
            for _, row in pub_tables.iterrows():
                table_name = row.get('tableName', '')
                subscriber = row.get('subscriber', '')
                actions = row.get('actions', '')

                if table_name:
                    print(f"    📋 Table: {table_name}")
                    if subscriber:
                        print(f"       Subscriber: {subscriber}")
                    if actions:
                        print(f"       Actions: {actions} (type: {type(actions)})")

                    # Try multiple unsubscribe methods
                    unsubscribe_methods = []

                    # If we have specific actions, try to unsubscribe each one
                    if actions and isinstance(actions, list):
                        print(f"       📝 Processing as list with {len(actions)} actions")
                        for action in actions:
                            unsubscribe_methods.append(f'unsubscribeTable(tableName=`{table_name}, actionName="{action}")')
                    elif actions and isinstance(actions, str):
                        # Handle string representation of list like "[action1,action2]"
                        if actions.startswith('[') and actions.endswith(']'):
                            action_list = actions[1:-1].split(',')
                            print(f"       📝 Processing as string list: {action_list}")
                            for action in action_list:
                                action = action.strip()
                                print(f"          Adding action: '{action}'")
                                unsubscribe_methods.append(f'unsubscribeTable(tableName=`{table_name}, actionName="{action}")')
                        else:
                            print(f"       📝 Processing as single string action")
                            unsubscribe_methods.append(f'unsubscribeTable(tableName=`{table_name}, actionName="{actions}")')

                    # Always try universal unsubscribe as fallback
                    unsubscribe_methods.append(f'unsubscribeTable(tableName=`{table_name})')
                    print(f"       📋 Total unsubscribe methods to try: {len(unsubscribe_methods)}")

                    # Execute all specific action unsubscribes first
                    success_count = 0
                    for method in unsubscribe_methods[:-1]:  # All except the universal fallback
                        try:
                            print(f"       🔧 Trying: {method}")
                            session.run(method)
                            print(f"       ✓ SUCCESS: {method}")
                            total_cleaned += 1
                            success_count += 1
                        except Exception as e:
                            error_msg = str(e)
                            print(f"       ❌ FAILED: {method}")
                            print(f"          Error: {error_msg}")
                            if "doesn't exist" not in error_msg.lower():
                                print(f"       ⚠ Method failed with unexpected error")

                    # Try universal unsubscribe as final fallback if any specific ones failed
                    if success_count < len(unsubscribe_methods) - 1:
                        try:
                            universal_method = unsubscribe_methods[-1]
                            print(f"       🔧 Trying universal fallback: {universal_method}")
                            session.run(universal_method)
                            print(f"       ✓ SUCCESS: {universal_method}")
                            total_cleaned += 1
                        except Exception as e:
                            error_msg = str(e)
                            print(f"       ❌ FAILED: {universal_method}")
                            print(f"          Error: {error_msg}")
        else:
            print("  ✅ No published tables found")

        # Try to unsubscribe from all remaining published tables
        try:
            remaining_pub_tables = session.run("getStreamingStat().pubTables")
            if remaining_pub_tables is not None and not remaining_pub_tables.empty:
                for _, row in remaining_pub_tables.iterrows():
                    table_name = row.get('tableName', '')
                    if table_name:
                        try:
                            session.run(f'unsubscribeTable(tableName=`{table_name})')
                            print(f"  ✓ Final unsubscribe attempt: {table_name}")
                            total_cleaned += 1
                        except Exception as e:
                            print(f"  ⚠ Final unsubscribe failed for {table_name}: {str(e)[:50]}...")
        except Exception as e:
            print(f"  ⚠ Could not perform final unsubscribe attempts: {e}")

    except Exception as e:
        print(f"❌ Error cleaning published tables: {e}")

    return total_cleaned

def cleanup_streaming_engines(session):
    """Clean up streaming engines"""
    print("\n🔍 Cleaning up streaming engines...")

    total_removed = 0

    try:
        engines = session.run("getStreamEngineStatus()")
        if engines is not None and not engines.empty:
            print(f"  Found {len(engines)} streaming engines")

            for _, engine in engines.iterrows():
                engine_name = engine.get('name', '') if hasattr(engine, 'get') else getattr(engine, 'name', '')
                if engine_name:
                    try:
                        session.run(f'dropStreamEngine("{engine_name}")')
                        print(f"    ✓ Dropped engine: {engine_name}")
                        total_removed += 1
                    except Exception as e:
                        print(f"    ✗ Failed to drop {engine_name}: {e}")
        else:
            print("  ✅ No streaming engines found")

    except Exception as e:
        print(f"  ⚠ Could not check streaming engines (may not be supported): {e}")

    return total_removed

def cleanup_shared_objects(session):
    """Clean up all shared objects"""
    print("\n🔍 Cleaning up shared objects...")

    total_removed = 0
    failed_objects = []

    try:
        shared_objects = session.run("objs(true)")

        if shared_objects is None or shared_objects.empty:
            print("  ✅ No shared objects found")
            return total_removed, failed_objects

        print(f"  Found {len(shared_objects)} shared objects")

        # System objects that should not be undefined
        system_objects = {
            'login', 'logout', 'getUserAccess', 'grant', 'deny', 'revoke',
            'getSessionMemoryStat', 'getClusterPerf', 'getStreamingStat',
            'objs', 'undef', 'share', 'clearAllCache', 'gc',
            'console', 'web', 'streaming', 'system'
        }

        for _, row in shared_objects.iterrows():
            obj_name = row.get('name', '')
            obj_type = row.get('type', 'Unknown')

            if not obj_name:
                continue

            # Skip system objects
            if obj_name in system_objects:
                print(f"    ⏭️ Skipping system object: {obj_name}")
                continue

            # Skip system internals
            if (obj_name.startswith('__') or obj_name.startswith('sys_') or
                obj_name.startswith('ddb_') or obj_name.startswith('_system')):
                print(f"    ⏭️ Skipping system internal: {obj_name}")
                continue

            try:
                session.run(f'undef(`{obj_name}, SHARED)')
                print(f"    ✓ Removed: {obj_name} ({obj_type})")
                total_removed += 1

            except Exception as e:
                error_msg = str(e).lower()

                if "doesn't exist" in error_msg or "not defined" in error_msg:
                    print(f"    ⚠ Already undefined: {obj_name}")
                elif "cancel all subscriptions" in error_msg or "subscription" in error_msg:
                    print(f"    🔒 Has active subscriptions: {obj_name} ({obj_type})")
                    failed_objects.append((obj_name, "active subscriptions", obj_type))
                else:
                    print(f"    ✗ Failed to remove {obj_name} ({obj_type}): {e}")
                    failed_objects.append((obj_name, str(e)[:50], obj_type))

        print(f"  ✅ Removed {total_removed} shared objects")
        if failed_objects:
            print(f"  ⚠️ {len(failed_objects)} objects could not be removed")

    except Exception as e:
        print(f"❌ Error in shared object cleanup: {e}")

    return total_removed, failed_objects

def show_final_status(session):
    """Show final cleanup status"""
    print("\n" + "="*50)
    print("FINAL STATUS")
    print("="*50)

    try:
        # Check remaining shared objects
        shared_objects = session.run("objs(true)")
        if shared_objects is None or shared_objects.empty:
            print("✅ SHARED OBJECTS: None remaining")
        else:
            print(f"⚠️ SHARED OBJECTS: {len(shared_objects)} remaining")
            for _, row in shared_objects.iterrows():
                obj_name = row.get('name', 'Unknown')
                obj_type = row.get('type', 'Unknown')
                print(f"    {obj_name} ({obj_type})")

        # Check remaining published tables
        try:
            pub_tables = session.run("getStreamingStat().pubTables")
            if pub_tables is None or pub_tables.empty:
                print("✅ PUBLISHED TABLES: None remaining")
            else:
                print(f"⚠️ PUBLISHED TABLES: {len(pub_tables)} remaining")
        except:
            print("⚠️ PUBLISHED TABLES: Could not check")

        # Check streaming engines
        try:
            engines = session.run("getStreamEngineStatus()")
            if engines is None or engines.empty:
                print("✅ STREAMING ENGINES: None remaining")
            else:
                print(f"⚠️ STREAMING ENGINES: {len(engines)} remaining")
        except:
            print("✅ STREAMING ENGINES: None or not supported")

    except Exception as e:
        print(f"❌ Error checking final status: {e}")

def main():
    """Main cleanup function - simplified with no complex options"""
    print("🚀 DolphinDB Complete Cleanup")
    print("="*50)

    # Connect to DolphinDB
    session = connect_to_dolphindb()
    if not session:
        sys.exit(1)

    try:
        total_operations = 0

        # Step 1: Clean published tables first (critical for fixing S03000 error)
        total_operations += cleanup_published_tables(session)

        # Step 2: Clean streaming engines
        total_operations += cleanup_streaming_engines(session)

        # Step 3: Clean shared objects
        removed, failed = cleanup_shared_objects(session)
        total_operations += removed

        # Step 4: Clear caches and garbage collect
        print("\n🧹 Final cleanup...")
        try:
            session.run("clearAllCache()")
            print("  ✓ Cleared all caches")
            total_operations += 1
        except Exception as e:
            print(f"  ⚠ Could not clear caches: {e}")

        try:
            session.run("gc()")
            print("  ✓ Garbage collection completed")
            total_operations += 1
        except Exception as e:
            print(f"  ⚠ Could not run garbage collection: {e}")

        # Show final status
        show_final_status(session)

        print(f"\n🎉 CLEANUP COMPLETE: {total_operations} operations performed")

        # Show remaining issues if any
        if failed:
            print(f"\n⚠️ {len(failed)} objects could not be cleaned:")
            for obj_name, reason, obj_type in failed[:5]:  # Show first 5
                print(f"  - {obj_name} ({obj_type}): {reason}")
            if len(failed) > 5:
                print(f"  ... and {len(failed) - 5} more")
            print("\n💡 If objects remain with 'active subscriptions' error:")
            print("   This indicates the S03000 error - published tables with phantom subscriptions")
            print("   Solution: Restart DolphinDB server to clear phantom locks")
        else:
            print("\n✅ All objects cleaned successfully!")

    except Exception as e:
        print(f"❌ Error during cleanup: {e}")
    finally:
        session.close()


if __name__ == "__main__":
    main()
