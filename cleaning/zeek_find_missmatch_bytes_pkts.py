import duckdb
import argparse

# Function to check if the bytes exceed the maximum possible value
def check_flow(orig_pkts, orig_bytes, resp_pkts, resp_bytes, max_payload):
    # Initialize error flags
    orig_error = False
    resp_error = False

    # Calculate max possible bytes for origin and response
    max_orig_bytes = orig_pkts * max_payload
    max_resp_bytes = resp_pkts * max_payload
    
    # Check if orig_bytes exceed the possible maximum (only if orig_bytes is not None)
    if orig_bytes is not None and orig_bytes > max_orig_bytes:
        orig_error = True
    
    # Check if resp_bytes exceed the possible maximum (only if resp_bytes is not None)
    if resp_bytes is not None and resp_bytes > max_resp_bytes:
        resp_error = True
    
    return orig_error, resp_error

# Function to log errors found in the flow
def log_error(log_file, uid, orig_pkts, orig_bytes, resp_pkts, resp_bytes, orig_error, resp_error, max_payload):
    with open(log_file, "a") as log:
        log.write(f"UID: {uid}\n")
        log.write(f"Orig_Pkts: {orig_pkts}, Orig_Bytes: {orig_bytes}, "
                  f"Resp_Pkts: {resp_pkts}, Resp_Bytes: {resp_bytes}\n")
        if orig_error:
            log.write(f"  - Error: Orig Bytes ({orig_bytes}) exceeds max possible ({orig_pkts * max_payload})\n")
        if resp_error:
            log.write(f"  - Error: Resp Bytes ({resp_bytes}) exceeds max possible ({resp_pkts * max_payload})\n")
        log.write("\n")  # Add a newline to separate logs

# Function to process all flows from the database
def process_flows(db_path, log_file, max_payload):
    con = duckdb.connect(db_path)
    
    cursor = con.execute('SELECT uid, orig_pkts, orig_bytes, resp_pkts, resp_bytes FROM logs')
    
    for row in cursor.fetchall():
        uid, orig_pkts, orig_bytes, resp_pkts, resp_bytes = row
        
        orig_error, resp_error = check_flow(orig_pkts, orig_bytes, resp_pkts, resp_bytes, max_payload)
        
        if orig_error or resp_error:
            log_error(log_file, uid, orig_pkts, orig_bytes, resp_pkts, resp_bytes, orig_error, resp_error, max_payload)
    
    con.close()

# Main function to parse arguments and execute the script
def main():
    parser = argparse.ArgumentParser(description="Check network flows for byte discrepancies.")
    
    parser.add_argument('--mtu', type=int, default=1500, help='Maximum Transmission Unit (MTU). Default is 1500 bytes.')
    parser.add_argument('--tcp_ip_overhead', type=int, default=40, help='TCP/IP overhead in bytes. Default is 40 bytes.')
    parser.add_argument('--log_file', type=str, default='flow_errors.log', help='Log file to write errors to. Default is flow_errors.log.')
    parser.add_argument('--db', type=str, default='db/ctu-hornet-65-niner_v0.1.db', help='Path to the DuckDB database file. Default is db/ctu-hornet-65-niner_v0.1.db.')
    
    args = parser.parse_args()

    # Calculate the maximum payload size based on MTU and TCP/IP overhead
    max_payload = args.mtu - args.tcp_ip_overhead
    
    # Process the flows with the given parameters
    process_flows(args.db, args.log_file, max_payload)

if __name__ == "__main__":
    main()
    print("Processing completed. Check the log file for any errors found.")

