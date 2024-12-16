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

    print()


# Feature Extraction
def total_bytes(con):
    """
    Calculate the total bytes across all flows in the dataset.
        - Total Bytes = orig_bytes + resp_bytes
        - Use BIGINT for variable
    """

    result = con.execute(
        "SELECT SUM(CAST(orig_bytes AS BIGINT) + CAST(resp_bytes AS BIGINT)) FROM logs;"
    ).fetchone()[0]

    print(f"Total bytes: {result}")
    print()


def total_packets(con):
    """
    Calculate the total packets across all flows in the dataset.
        - Total Packets = orig_pkts + resp_pkts
        - Use BIGINT for variable
    """

    result = con.execute(
        "SELECT SUM(CAST(orig_pkts AS BIGINT) + CAST(resp_pkts AS BIGINT)) FROM logs;"
    ).fetchone()[0]

    print(f"Total packets: {result}")
    print()


def total_flows(con):
    """
    Calculate the total number of flows (rows) in the dataset.
        - Dataset is the table 'logs'
        - Flows will be one per row
    """

    result = con.execute(
        "SELECT COUNT(*) FROM logs;"
    ).fetchone()[0]

    print(f"Total flows: {result}")
    print()


def total_flows_ipv4(con):
    """
    Calculate the total number of IPv4 flows in the DuckDB database.

    IPv4 Flows: Rows where both id_orig_h and id_resp_h are valid IPv4 addresses.
    """
    try:
        # SQL query to filter and count rows with valid IPv4 addresses using regex
        result = con.execute('''
            SELECT COUNT(*)
            FROM logs
            WHERE 
                regexp_matches(id_orig_h, '^((\\d{1,3}.){3}\\d{1,3})$')
            AND 
                regexp_matches(id_resp_h, '^((\\d{1,3}.){3}\\d{1,3})$')
        ''').fetchone()[0]

        print(f'Total IPv4 flows calculated: {result}')
        print()

    except Exception as e:
        print(f"Error calculating IPv4 flows: {e}")
        return None


def total_flows_ipv6(con):
    """
    Calculate the total number of IPv6 flows in the DuckDB database.

    IPv6 Flows: Rows where both id_orig_h and id_resp_h are valid IPv6 addresses.
    """
    try:
        # SQL query to filter and count rows with valid IPv6 addresses using regex
        result = con.execute('''
            SELECT COUNT(*)
            FROM logs
            WHERE NOT 
                regexp_matches(id_orig_h, '^((\\d{1,3}.){3}\\d{1,3})$')
            AND NOT
                regexp_matches(id_resp_h, '^((\\d{1,3}.){3}\\d{1,3})$')
        ''').fetchone()[0]

        print(f'Total IPv6 flows calculated: {result}')

    except Exception as e:
        print(f"Error calculating IPv6 flows: {e}")
        return None


def protocol_summary(con):
    """
    Outputs the number of flows in total, the number of flows using TCP, UDP, and ICMP,
    grouped by IPv4 and IPv6.
    """
    try:
        # Query to calculate flows grouped by IP type (IPv4/IPv6) and protocol
        query = '''
            SELECT
                CASE
                    WHEN regexp_matches(id_orig_h, '^((\\d{1,3}\\.){3}\\d{1,3})$')
                         AND regexp_matches(id_resp_h, '^((\\d{1,3}\\.){3}\\d{1,3})$')
                    THEN 'IPv4'
                    ELSE 'IPv6'
                END AS "IP Proto",
                COUNT(*) AS "Total Flows",
                SUM(CASE WHEN proto = 'tcp' THEN 1 ELSE 0 END) AS "TCP Flows",
                SUM(CASE WHEN proto = 'udp' THEN 1 ELSE 0 END) AS "UDP Flows",
                SUM(CASE WHEN proto = 'icmp' THEN 1 ELSE 0 END) AS "ICMP Flows"
            FROM logs
            GROUP BY "IP Proto";
        '''

        # Execute the query and fetch the result as a Pandas DataFrame
        result_df = con.execute(query).fetchdf()

        print("Total flows grouped by IPv4/IPv6 and protocol):")
        print(result_df)

    except Exception as e:
        print(f"Error generating protocol summary: {e}")
        return None


def flows_by_protocol_and_source(con):
    """
    Calculate the total number of flows by honeypot source, with a distinction between
    IPv4 and IPv6 flows, and output in a CSV-friendly format.
    """
    try:
        # Query to calculate the required data
        query = '''
            SELECT
                source as "Honeypot Name (Source)",
                COUNT(*) AS total_flows,
                SUM(CASE WHEN regexp_matches(id_orig_h, '^((\\d{1,3}\\.){3}\\d{1,3})$') AND
                              regexp_matches(id_resp_h, '^((\\d{1,3}\\.){3}\\d{1,3})$') THEN 1 ELSE 0 END) AS ipv4_flows,
                SUM(CASE WHEN NOT regexp_matches(id_orig_h, '^((\\d{1,3}\\.){3}\\d{1,3})$') AND
                              NOT regexp_matches(id_resp_h, '^((\\d{1,3}\\.){3}\\d{1,3})$') THEN 1 ELSE 0 END) AS ipv6_flows,
                SUM(CASE WHEN proto = 'tcp' THEN 1 ELSE 0 END) AS total_tcp_flows,
                SUM(CASE WHEN proto = 'udp' THEN 1 ELSE 0 END) AS total_udp_flows,
                SUM(CASE WHEN proto = 'icmp' THEN 1 ELSE 0 END) AS total_icmp_flows,
                SUM(CASE WHEN proto = 'tcp' AND regexp_matches(id_orig_h, '^((\\d{1,3}\\.){3}\\d{1,3})$') AND
                                         regexp_matches(id_resp_h, '^((\\d{1,3}\\.){3}\\d{1,3})$') THEN 1 ELSE 0 END) AS ipv4_tcp_flows,
                SUM(CASE WHEN proto = 'udp' AND regexp_matches(id_orig_h, '^((\\d{1,3}\\.){3}\\d{1,3})$') AND
                                         regexp_matches(id_resp_h, '^((\\d{1,3}\\.){3}\\d{1,3})$') THEN 1 ELSE 0 END) AS ipv4_udp_flows,
                SUM(CASE WHEN proto = 'icmp' AND regexp_matches(id_orig_h, '^((\\d{1,3}\\.){3}\\d{1,3})$') AND
                                          regexp_matches(id_resp_h, '^((\\d{1,3}\\.){3}\\d{1,3})$') THEN 1 ELSE 0 END) AS ipv4_icmp_flows,
                SUM(CASE WHEN proto = 'tcp' AND NOT regexp_matches(id_orig_h, '^((\\d{1,3}\\.){3}\\d{1,3})$') AND
                                         NOT regexp_matches(id_resp_h, '^((\\d{1,3}\\.){3}\\d{1,3})$') THEN 1 ELSE 0 END) AS ipv6_tcp_flows,
                SUM(CASE WHEN proto = 'udp' AND NOT regexp_matches(id_orig_h, '^((\\d{1,3}\\.){3}\\d{1,3})$') AND
                                         NOT regexp_matches(id_resp_h, '^((\\d{1,3}\\.){3}\\d{1,3})$') THEN 1 ELSE 0 END) AS ipv6_udp_flows,
                SUM(CASE WHEN proto = 'icmp' AND NOT regexp_matches(id_orig_h, '^((\\d{1,3}\\.){3}\\d{1,3})$') AND
                                          NOT regexp_matches(id_resp_h, '^((\\d{1,3}\\.){3}\\d{1,3})$') THEN 1 ELSE 0 END) AS ipv6_icmp_flows
            FROM logs
            GROUP BY "Honeypot Name (Source)"
            ORDER BY "Honeypot Name (Source)";
        '''

        # Execute the query
        result_df = con.execute(query).fetchdf()

        print(result_df)
    except Exception as e:
        print(f"Error calculating flows by protocol and source: {e}")


def packets_per_honeypot_source(con):
    """
    Calculate the amount of packets per honeypot location source.
    """

    result = con.execute(
        "SELECT source, SUM(orig_pkts + resp_pkts) as packet_count FROM logs GROUP BY source;"
    ).fetchall()

    print("Amount of packets per honeypot location source:")

    for row in result:
        print(f"Source: {row[0]}, Packet Count: {row[1]}")

    print()

def bytes_per_honeypot_source(con):
    """
    Calculate the amount of bytes per honeypot location source.
    """

    result = con.execute(
        "SELECT source, SUM(CAST(orig_bytes AS BIGINT) + CAST(resp_bytes AS BIGINT)) as byte_count FROM logs GROUP BY source;"
    ).fetchall()

    print("Amount of bytes per honeypot location source:")
    for row in result:
        print(f"Source: {row[0]}, Byte Count: {row[1]}")

    print()

def flows_per_honeypot_source(con):
    """
    Calculate the amount of flows per honeypot location source.
    """

    result = con.execute(
        "SELECT source, COUNT(*) as flow_count FROM logs GROUP BY source;"
    ).fetchall()

    print("Amount of flows per honeypot location source:")
    for row in result:
        print(f"Source: {row[0]}, Flow Count: {row[1]}")

    print()

def unique_source_ips(con):
    """
    Calculate the total unique source IP addresses.
    """

    result = con.execute(
        "SELECT COUNT(DISTINCT id_orig_h) as unique_ips FROM logs;"
    ).fetchone()[0]

    print(f"Total unique source IP addresses: {result}")
    print()


def unique_source_ips_per_honeypot(con):
    """
    Calculate the total unique source IP addresses per honeypot
    location source.
    """

    result = con.execute(
        "SELECT source, COUNT(DISTINCT id_orig_h) as unique_ips FROM logs GROUP BY source;"
    ).fetchall()

    print("Total unique source IP addresses per honeypot location source:")

    for row in result:
        print(f"Source: {row[0]}, Unique IPs: {row[1]}")

    print()


def generate_honeypot_summary_csv(con):
    """
    Generate a CSV summarizing honeypot data with the following columns:
    - Honeypot Name (Source)
    - Total Number of Network Flows
    - Total Number of Unique Src IPs
    - Total Number of Bytes
    - Total Number of Packets
    """
    try:
        # SQL query to calculate the required metrics
        query = """
        SELECT 
            source AS "Honeypot Name (Source)",
            COUNT(*) AS "Total Number of Network Flows",
            COUNT(DISTINCT id_orig_h) AS "Total Number of Unique Src IPs",
            SUM(orig_bytes + resp_bytes) AS "Total Number of Bytes",
            SUM(orig_pkts + resp_pkts) AS "Total Number of Packets"
        FROM logs
        GROUP BY source
        ORDER BY source;
        """

        # Execute query and fetch results
        result_df = con.execute(query).fetchdf()

        print(result_df)

    except Exception as e:
        print(f"Error generating honeypot summary: {e}")

def total_flows_per_destination_port_udp(con, top_n=10):
    """
    Get the total flows per destination port (id_resp_p) and return the top N ports.
    """
    try:
        # SQL query to calculate flows per destination port
        query = f"""
        SELECT 
            id_resp_p AS "UDP Port",
            COUNT(*) AS "Total Network Flows"
        FROM logs
        WHERE proto = 'udp'
        GROUP BY id_resp_p
        ORDER BY "Total Network Flows" DESC
        LIMIT {top_n};
        """

        # Execute query and fetch the result
        result_df = con.execute(query).fetchdf()

        # Print results
        print(result_df)
    except Exception as e:
        print(f"Error calculating flows per destination port: {e}")
        return None

def total_flows_per_destination_port_tcp(con, top_n=10):
    """
    Get the total flows per destination port (id_resp_p) and return the top N ports.
    """
    try:
        # SQL query to calculate flows per destination port
        query = f"""
        SELECT 
            id_resp_p AS "TCP Port",
            COUNT(*) AS "Total Network Flows"
        FROM logs
        WHERE proto = 'tcp'
        GROUP BY id_resp_p
        ORDER BY "Total Network Flows" DESC
        LIMIT {top_n};
        """

        # Execute query and fetch the result
        result_df = con.execute(query).fetchdf()

        # Print results
        print(result_df)
    except Exception as e:
        print(f"Error calculating flows per destination port: {e}")
        return None


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
    parser.add_argument('--total_flows_ipv4',
                        action='store_true',
                        help='Calculate the total IPv4 flows')
    parser.add_argument('--total_flows_ipv6',
                        action='store_true',
                        help='Calculate the total IPv6 flows')
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
    parser.add_argument('--unique_source_ips_per_honeypot',
                        action='store_true',
                        help='Calculate the total unique source IPs per honeypot source')

    parser.add_argument('--flows_by_top_dst_ports',
                        action='store_true',
                        help='Calculate the total flows by top destination ports')


    args = parser.parse_args()

    log_level = getattr(logging, args.log_level.upper(), logging.INFO)
    setup_logging(args.log_file, log_level)

    logging.info('Starting feature extraction.')

    con = duckdb.connect(args.db_name)

    if args.info:
        check_db_info(con)

    if args.metrics:
        print("-------------------------------------------------------------------------------------")
        print("Table 1: CTU Hornet 65 Niner dataset metrics overview per honeypot during the 65 days")
        print("-------------------------------------------------------------------------------------")
        generate_honeypot_summary_csv(con)
        print("-------------------------------------------------------------------------------------")
        print()
        print("-------------------------------------------------------------------------------------")
        print("Table 2 and 3: CTU Hornet 65 Niner dataset total network flows by L3 and L4")
        print("-------------------------------------------------------------------------------------")
        flows_by_protocol_and_source(con)
        print("-------------------------------------------------------------------------------------")
        print()
        print("-------------------------------------------------------------------------------------")
        print("Table 4: Top 10 TCP destination ports by total number of network flows")
        print("-------------------------------------------------------------------------------------")
        total_flows_per_destination_port_tcp(con)
        print("-------------------------------------------------------------------------------------")
        print()
        print("-------------------------------------------------------------------------------------")
        print("Table 5: Top 10 UDP destination ports by total number of network flows")
        print("-------------------------------------------------------------------------------------")
        total_flows_per_destination_port_udp(con)
        print("-------------------------------------------------------------------------------------")
        print()

    if args.total_flows:
        total_flows(con)

    if args.total_flows_ipv4:
        total_flows_ipv4(con)

    if args.total_flows_ipv6:
        total_flows_ipv6(con)

    if args.total_bytes:
        total_bytes(con)

    if args.total_packets:
        total_packets(con)

    if args.flows_by_proto_source:
        flows_by_protocol_and_source(con)

    if args.packets_per_honeypot_source:
        packets_per_honeypot_source(con)

    if args.bytes_per_honeypot_source:
        bytes_per_honeypot_source(con)

    if args.flows_per_honeypot_source:
        flows_per_honeypot_source(con)

    if args.unique_source_ips:
        unique_source_ips(con)

    if args.unique_source_ips_per_honeypot:
        unique_source_ips_per_honeypot(con)
    
    if args.flows_by_top_dst_ports:
        total_flows_per_destination_port(con)

    logging.info('Feature extraction complete.')

if __name__ == '__main__':
    main()

