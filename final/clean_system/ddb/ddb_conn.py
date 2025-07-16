import dolphindb as ddb
import os
import argparse
import sys

import time
from concurrent.futures import ThreadPoolExecutor, as_completed

def connect_to_dolphindb(host="192.168.91.91", port=8848, user="admin", password="123456"):
    """Connect to DolphinDB server and return session object"""
    try:
        s = ddb.session()
        s.connect(host, port, user, password)
        return s
    except Exception as e:
        print(f"Error connecting to DolphinDB: {e}")
        return None

def run_script_file(session, script_path, script_name=None):
    """Run a DolphinDB script file using the given session"""
    if script_name is None:
        script_name = os.path.basename(script_path)

    try:
        # Try different encodings to handle potential encoding issues
        encodings = ['utf-8', 'utf-8-sig', 'gbk', 'cp1252']
        script = None

        for encoding in encodings:
            try:
                with open(script_path, "r", encoding=encoding) as f:
                    script = f.read()
                print(f"[{script_name}] Successfully read script with {encoding} encoding")
                break
            except UnicodeDecodeError:
                continue

        if script is None:
            raise Exception("Could not read script file with any supported encoding")

        print(f"[{script_name}] Executing script...")
        result = session.run(script)
        print(f"[{script_name}] Script completed successfully")
        return result
    except FileNotFoundError:
        print(f"[{script_name}] Script file not found: {script_path}")
    except Exception as e:
        print(f"[{script_name}] Error running script: {e}")
    return None

def run_script_in_thread(script_path, host, port, user, password, delay=0):
    """Run a script in a separate thread with its own DolphinDB connection"""
    script_name = os.path.basename(script_path)

    if delay > 0:
        print(f"[{script_name}] Waiting {delay} seconds before starting...")
        time.sleep(delay)

    print(f"[{script_name}] Starting execution...")

    # Create separate connection for this thread
    session = connect_to_dolphindb(host, port, user, password)
    if session is None:
        print(f"[{script_name}] Failed to connect to DolphinDB")
        return None

    try:
        result = run_script_file(session, script_path, script_name)
        return {"script": script_name, "result": result, "success": True}
    except Exception as e:
        print(f"[{script_name}] Thread execution error: {e}")
        return {"script": script_name, "result": None, "success": False, "error": str(e)}
    finally:
        session.close()
        print(f"[{script_name}] Connection closed")

def list_available_scripts():
    """List all available .dos scripts"""
    current_dir = os.path.dirname(os.path.abspath(__file__))

    # Core trading system scripts
    core_scripts = {
        "strat1.dos": "Strategy 1: Simple Momentum",
        "strat2.dos": "Strategy 2: Multiple Momentum",
        "strat3.dos": "Strategy 3: Cross-sectional Exploration",
        "simulated_exchange.dos": "Simulated Matching Exchange",
        "performance_analytics.dos": "Performance Analytics System",
        "unified_data_distribution_system.dos": "Unified Data Distribution System",
        "dominant_contract_stream.dos": "Dominant Contract Stream Management"
    }

    # Utility scripts
    utility_scripts = {
        "cleanup.dos": "System cleanup utility",
        "final_working_system.dos": "Complete working system demonstration"
    }

    print("CORE TRADING SYSTEM SCRIPTS:")
    print("=" * 60)
    for script, description in core_scripts.items():
        script_path = os.path.join(current_dir, script)
        status = "OK" if os.path.exists(script_path) else "MISSING"
        print(f"  {status} {script:<35} - {description}")

    print("\nUTILITY SCRIPTS:")
    print("=" * 60)
    for script, description in utility_scripts.items():
        script_path = os.path.join(current_dir, script)
        status = "OK" if os.path.exists(script_path) else "MISSING"
        print(f"  {status} {script:<35} - {description}")

    print("\nUSAGE EXAMPLES:")
    print("  Single script:    python ddb_conn.py strat1.dos")
    print("  Multiple scripts: python ddb_conn.py strat1.dos simulated_exchange.dos")
    print("  With delays:      python ddb_conn.py strat1.dos simulated_exchange.dos --delays 0 3")
    print("=" * 60)

def main():
    parser = argparse.ArgumentParser(
        description="Run single or multiple DolphinDB scripts with concurrent execution support",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  Single script:
    python ddb_conn.py strat1.dos
    python ddb_conn.py simulated_exchange.dos

  Multiple scripts (concurrent):
    python ddb_conn.py strat1.dos simulated_exchange.dos
    python ddb_conn.py strat1.dos strat2.dos strat3.dos simulated_exchange.dos

  With execution delays:
    python ddb_conn.py strat1.dos simulated_exchange.dos --delays 0 3
    (runs strat1.dos immediately, simulated_exchange.dos after 3 seconds)

  List available scripts:
    python ddb_conn.py --list
        """
    )

    parser.add_argument('scripts', nargs='*', help='DolphinDB script files to run (.dos extension)')
    parser.add_argument('--list', '-l', action='store_true', help='List all available scripts')
    parser.add_argument('--delays', nargs='*', type=int, help='Execution delays in seconds for each script (default: 0 for all)')
    parser.add_argument('--sequential', '-s', action='store_true', help='Run scripts sequentially instead of concurrently')
    parser.add_argument('--host', default='192.168.91.91', help='DolphinDB server host (default: 192.168.91.91)')
    parser.add_argument('--port', type=int, default=8848, help='DolphinDB server port (default: 8848)')
    parser.add_argument('--user', default='admin', help='DolphinDB username (default: admin)')
    parser.add_argument('--password', default='123456', help='DolphinDB password (default: 123456)')

    args = parser.parse_args()

    # List available scripts
    if args.list:
        list_available_scripts()
        return

    # Check if script arguments are provided
    if not args.scripts:
        print("Error: No scripts specified.")
        print("Use --list to see available scripts or provide script names.")
        parser.print_help()
        sys.exit(1)

    # Get current directory
    current_dir = os.path.dirname(os.path.abspath(__file__))

    # Process script names and validate
    script_paths = []
    for script in args.scripts:
        # Add .dos extension if not provided
        script_name = script if script.endswith('.dos') else script + '.dos'
        script_path = os.path.join(current_dir, script_name)

        # Check if script exists
        if not os.path.exists(script_path):
            print(f"Error: Script file not found: {script_name}")
            print("Use --list to see available scripts.")
            sys.exit(1)

        script_paths.append(script_path)

    # Process delays
    delays = args.delays if args.delays else [0] * len(script_paths)
    if len(delays) < len(script_paths):
        # Extend delays with 0s if not enough provided
        delays.extend([0] * (len(script_paths) - len(delays)))
    elif len(delays) > len(script_paths):
        # Truncate delays if too many provided
        delays = delays[:len(script_paths)]

    print(f"DolphinDB Multi-Script Executor")
    print(f"Connecting to DolphinDB at {args.host}:{args.port}")
    print(f"Scripts to execute: {len(script_paths)}")
    for i, (path, delay) in enumerate(zip(script_paths, delays)):
        script_name = os.path.basename(path)
        delay_info = f" (delay: {delay}s)" if delay > 0 else ""
        print(f"   {i+1}. {script_name}{delay_info}")
    print(f"Execution mode: {'Sequential' if args.sequential else 'Concurrent'}")
    print()

    if args.sequential or len(script_paths) == 1:
        # Sequential execution
        print("Running scripts sequentially...")
        for i, (script_path, delay) in enumerate(zip(script_paths, delays)):
            script_name = os.path.basename(script_path)

            if delay > 0:
                print(f"Waiting {delay} seconds before {script_name}...")
                time.sleep(delay)

            print(f"Running {script_name} ({i+1}/{len(script_paths)})")

            # Connect to DolphinDB
            s = connect_to_dolphindb(args.host, args.port, args.user, args.password)
            if s:
                try:
                    result = run_script_file(s, script_path, script_name)
                    if result is not None:
                        print(f"{script_name} completed successfully")
                    else:
                        print(f"{script_name} completed with warnings")
                except Exception as e:
                    print(f"{script_name} failed: {e}")
                finally:
                    s.close()
            else:
                print(f"Failed to connect for {script_name}")
                sys.exit(1)
    else:
        # Concurrent execution
        print("Running scripts concurrently...")

        with ThreadPoolExecutor(max_workers=len(script_paths)) as executor:
            # Submit all scripts
            futures = []
            for script_path, delay in zip(script_paths, delays):
                future = executor.submit(
                    run_script_in_thread,
                    script_path,
                    args.host,
                    args.port,
                    args.user,
                    args.password,
                    delay
                )
                futures.append(future)

            # Wait for all to complete
            results = []
            for future in as_completed(futures):
                result = future.result()
                results.append(result)
                if result and result.get('success'):
                    print(f"{result['script']} completed successfully")
                else:
                    print(f"{result['script']} failed")

        print(f"\nExecution Summary:")
        successful = sum(1 for r in results if r and r.get('success'))
        print(f"   Successful: {successful}/{len(script_paths)}")
        print(f"   Failed: {len(script_paths) - successful}/{len(script_paths)}")

    print("\nMulti-script execution completed!")

if __name__ == "__main__":
    main()