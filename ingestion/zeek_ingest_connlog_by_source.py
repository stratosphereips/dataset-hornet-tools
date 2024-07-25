import os
import duckdb
import gzip
import ijson
import argparse
import logging
from concurrent.futures import ThreadPoolExecutor


def setup_logging(log_file, log_level):
    """Set up logging to the specified log file."""
    logging.basicConfig(filename=log_file, level=log_level,
                        format='%(asctime)s %(levelname)s:%(message)s')


def create_table(con):
    """Create the logs table in the DuckDB database if it doesn't exist."""
    con.execute('''
    CREATE TABLE IF NOT EXISTS logs (
        ts DOUBLE,
        uid STRING PRIMARY KEY,
        id_orig_h STRING,
        id_orig_p INTEGER,
        id_resp_h STRING,
        id_resp_p INTEGER,
        proto STRING,
        duration DOUBLE,
        orig_bytes INTEGER,
        resp_bytes INTEGER,
        conn_state STRING,
        local_orig BOOLEAN,
        local_resp BOOLEAN,
        missed_bytes INTEGER,
        history STRING,
        orig_pkts INTEGER,
        orig_ip_bytes INTEGER,
        resp_pkts INTEGER,
        resp_ip_bytes INTEGER,
        source STRING
    )
    ''')
    logging.debug('Table created or already exists.')


def process_log_file(db_name, file_path, source):
    """
    Process a single log file and insert its data into the DuckDB database.
    """
    con = duckdb.connect(db_name)
    with gzip.open(file_path, 'rt') as f:
        for line in f:
            # Use ijson to parse the JSON line
            data = ijson.items(line, '')
            # Prepare a tuple of values, None if keys are missing
            for item in data:
                values = (
                    item.get('ts', None),
                    item.get('uid', None),
                    item.get('id.orig_h', None),
                    item.get('id.orig_p', None),
                    item.get('id.resp_h', None),
                    item.get('id.resp_p', None),
                    item.get('proto', None),
                    item.get('duration', None),
                    item.get('orig_bytes', None),
                    item.get('resp_bytes', None),
                    item.get('conn_state', None),
                    item.get('local_orig', None),
                    item.get('local_resp', None),
                    item.get('missed_bytes', None),
                    item.get('history', None),
                    item.get('orig_pkts', None),
                    item.get('orig_ip_bytes', None),
                    item.get('resp_pkts', None),
                    item.get('resp_ip_bytes', None),
                    source
                )

            try:
                con.execute(
                    '''
                    INSERT INTO logs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(uid) DO NOTHING
                    ''', values
                )
            except duckdb.ConversionException as e:
                logging.error(f'Error processing line: {line}. Error: {e}')
            except duckdb.InvalidInputException as e:
                logging.error(f'Error processing line: {line}. Error: {e}')
            except KeyboardInterrupt:
                exit()

    logging.debug(f'Processed file: {file_path} with source: {source}')
    con.close()


def main():
    """Main function to parse arguments and load log data into DuckDB."""
    parser = argparse.ArgumentParser(description="Load log data into DuckDB.")
    parser.add_argument('--log_dir',
                        help='Folder to read log files from')
    parser.add_argument('--source',
                        help='Source name for the logs')
    parser.add_argument('--db_name',
                        default='logs.db',
                        help='Database name (default: logs.db)')
    parser.add_argument('--log_file',
                        default='log_import.log',
                        help='Log file name (default: log_import.log)')
    parser.add_argument('--workers',
                        type=int,
                        default=4,
                        help='Number of worker threads (default: 4)')
    parser.add_argument('--log_level',
                        default='INFO',
                        help='Logging level (default: INFO)')
    args = parser.parse_args()

    # Set up logging level
    log_level = getattr(logging, args.log_level.upper(), logging.INFO)
    setup_logging(args.log_file, log_level)
    logging.info('Starting log data import.')

    con = duckdb.connect(args.db_name)

    create_table(con)
    con.close()

    files_to_process = [os.path.join(root, file)
                        for root, _, files in os.walk(args.log_dir)
                        for file in files if file.startswith('conn.') and file.endswith('.log.gz')]

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        for file_path in files_to_process:
            executor.submit(process_log_file, args.db_name, file_path, args.source)

    logging.info('Data import complete.')


if __name__ == '__main__':
    """Entry point of the script."""
    main()

