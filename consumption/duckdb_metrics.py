import duckdb
import argparse
import logging


def setup_logging(log_file, log_level):
    """Set up logging to the specified log file."""
    logging.basicConfig(filename=log_file, level=log_level,
                        format='%(asctime)s %(levelname)s:%(message)s')


def check_db_info(con):
    """Print general information about the database."""
    tables = con.execute("SHOW TABLES;").fetchall()
    print("Tables in the database:")
    for table in tables:
        print(f" - {table[0]}")

    for table in tables:
        table_name = table[0]
        print(f"\nTable: {table_name}")

        row_count = con.execute(f"SELECT COUNT(*) FROM {table_name};").fetchone()[0]
        print(f" - Number of rows: {row_count}")

        columns_info = con.execute(f"PRAGMA table_info('{table_name}');").fetchall()
        print(f" - Number of columns: {len(columns_info)}")
        print(" - Columns:")
        for col in columns_info:
            print(f"   - {col[1]} ({col[2]})")


# Feature Extraction
def total_bytes(con):
    """
    Calculate the total bytes across all flows in the dataset.
        - Total Bytes = orig_bytes + resp_bytes
        - Use BIGINT for variable
    """

    result = con.execute("SELECT SUM(CAST(orig_bytes AS BIGINT) + CAST(resp_bytes AS BIGINT)) FROM logs;").fetchone()[0]
    print(f"Total bytes: {result}")


def total_packets(con):
    """
    Calculate the total packets across all flows in the dataset.
        - Total Packets = orig_pkts + resp_pkts
        - Use BIGINT for variable
    """

    result = con.execute("SELECT SUM(CAST(orig_pkts AS BIGINT) + CAST(resp_pkts AS BIGINT)) FROM logs;").fetchone()[0]
    print(f"Total packets: {result}")


def total_flows(con):
    """
    Calculate the total number of flows (rows) in the dataset.
        - Dataset is the table 'logs'
        - Flows will be one per row
    """
    result = con.execute("SELECT COUNT(*) FROM logs;").fetchone()[0]

    print(f"Total flows: {result}")


def flows_by_protocol_and_source(con):
    """Calculate and print the total number of flows by protocol and honeypot source."""
    result = con.execute(
        "SELECT proto, source, COUNT(*) as flow_count FROM logs GROUP BY proto, source;"
    ).fetchall()
    
    print("Total number of flows by protocol and honeypot source:")
    for row in result:
        print(f"Protocol: {row[0]}, Source: {row[1]}, Flow Count: {row[2]}")


def packets_per_honeypot_source(con):
    """Calculate and print the amount of packets per honeypot location source."""
    result = con.execute(
        "SELECT source, SUM(orig_pkts + resp_pkts) as packet_count FROM logs GROUP BY source;"
    ).fetchall()
    
    print("Amount of packets per honeypot location source:")
    for row in result:
        print(f"Source: {row[0]}, Packet Count: {row[1]}")


def bytes_per_honeypot_source(con):
    """Calculate and print the amount of bytes per honeypot location source."""
    result = con.execute(
        "SELECT source, SUM(CAST(orig_bytes AS BIGINT) + CAST(resp_bytes AS BIGINT)) as byte_count FROM logs GROUP BY source;"
    ).fetchall()
    
    print("Amount of bytes per honeypot location source:")
    for row in result:
        print(f"Source: {row[0]}, Byte Count: {row[1]}")


def flows_per_honeypot_source(con):
    """Calculate and print the amount of flows per honeypot location source."""
    result = con.execute(
        "SELECT source, COUNT(*) as flow_count FROM logs GROUP BY source;"
    ).fetchall()
    
    print("Amount of flows per honeypot location source:")
    for row in result:
        print(f"Source: {row[0]}, Flow Count: {row[1]}")


def unique_source_ips(con):
    """Calculate and print the total unique source IP addresses."""
    result = con.execute(
        "SELECT COUNT(DISTINCT id_orig_h) as unique_ips FROM logs;"
    ).fetchone()[0]
    
    print(f"Total unique source IP addresses: {result}")


def main():
    """
    Main function to parse arguments and execute the selected actions.
    """
    parser = argparse.ArgumentParser(description="Extract features and metrics from DuckDB.")
    # Logging Options First
    parser.add_argument('--log_level',
                        default='INFO',
                        help='Logging level (default: INFO)')
    parser.add_argument('--log_file',
                        default='feature_extraction.log',
                        help='Log file name (default: feature_extraction.log)')

    # DB
    parser.add_argument('--db_name',
                        required=True,
                        help='Path to the DuckDB database file')
    # DB INFO
    parser.add_argument('--info',
                        action='store_true',
                        help='Print general information about the database')
    # OBTAIN ALL METRICS
    parser.add_argument('--metrics',
                        action='store_true',
                        help='Calculate all available metrics')

    parser.add_argument('--total_flows',
                        action='store_true',
                        help='Calculate the total flows')
    parser.add_argument('--total_bytes',
                        action='store_true',
                        help='Calculate the total bytes')
    parser.add_argument('--total_packets',
                        action='store_true',
                        help='Calculate the total packets')

    parser.add_argument('--packets_per_honeypot_source',
                        action='store_true',
                        help='Calculate the packets per honeypot source')
    parser.add_argument('--bytes_per_honeypot_source',
                        action='store_true',
                        help='Calculate the bytes per honeypot source')
    parser.add_argument('--flows_per_honeypot_source',
                        action='store_true',
                        help='Calculate the flows per honeypot source')

    parser.add_argument('--flows_by_proto_source',
                        action='store_true',
                        help='Calculate the flows by protocol and source')
    parser.add_argument('--unique_source_ips',
                        action='store_true',
                        help='Calculate the total unique source IP addresses')

    args = parser.parse_args()

    log_level = getattr(logging, args.log_level.upper(), logging.INFO)
    setup_logging(args.log_file, log_level)

    logging.info('Starting feature extraction.')

    con = duckdb.connect(args.db_name)

    if args.info:
        check_db_info(con)

    if args.metrics or args.total_flows:
        total_flows(con)

    if args.metrics or args.total_bytes:
        total_bytes(con)

    if args.metrics or args.total_packets:
        total_packets(con)

    if args.metrics or args.flows_by_proto_source:
        flows_by_protocol_and_source(con)

    if args.metrics or args.packets_per_honeypot_source:
        packets_per_honeypot_source(con)

    if args.metrics or args.bytes_per_honeypot_source:
        bytes_per_honeypot_source(con)

    if args.metrics or args.flows_per_honeypot_source:
        flows_per_honeypot_source(con)

    if args.metrics or args.unique_source_ips:
        unique_source_ips(con)

    logging.info('Feature extraction complete.')

if __name__ == '__main__':
    main()

