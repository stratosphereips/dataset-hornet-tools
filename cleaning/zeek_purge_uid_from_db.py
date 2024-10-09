import argparse
import duckdb
import logging
import sys
from datetime import datetime

def setup_logging(log_file):
    """Set up logging to the specified log file."""
    logging.basicConfig(filename=log_file, level=logging.INFO,
                        format='%(asctime)s %(levelname)s:%(message)s')

def read_uids(file_path):
    """Read UIDs from a file, one per line."""
    with open(file_path, 'r') as f:
        uids = [line.strip() for line in f.readlines()]
    return uids

def delete_entries(con, uids):
    """Delete log entries matching the given UIDs and return total rows deleted."""
    total_deleted = 0
    for uid in uids:
        affected_rows = con.execute("DELETE FROM logs WHERE uid = ?", (uid,)).rowcount
        total_deleted += affected_rows
        if affected_rows == 0:
            logging.warning(f"No entries found for UID: {uid}")
        else:
            logging.info(f"Deleted {affected_rows} entries with UID: {uid}")
    return total_deleted

def main():
    """Main function to parse arguments and execute the deletion."""
    parser = argparse.ArgumentParser(description="Delete Zeek log entries by UID from DuckDB.")
    parser.add_argument('--db_name', required=True, help='Path to the DuckDB database file')
    parser.add_argument('--uid_file', required=True, help='File containing UIDs to delete, one per line')
    parser.add_argument('--log_file', default='deletion.log', help='Log file name (default: deletion.log)')
    parser.add_argument('--confirm', action='store_true', help='Ask for confirmation before deleting')

    args = parser.parse_args()

    setup_logging(args.log_file)

    uids = read_uids(args.uid_file)

    if args.confirm:
        confirmation = input(f"Are you sure you want to delete {len(uids)} entries? [y/N]: ")
        if confirmation.lower() != 'y':
            print("Deletion aborted.")
            sys.exit(0)

    start_time = datetime.now()
    logging.info(f"Deletion process started at {start_time}")

    con = duckdb.connect(args.db_name)
    total_deleted = delete_entries(con, uids)
    con.close()

    end_time = datetime.now()
    logging.info(f"Deletion process completed at {end_time}")
    logging.info(f"Total rows deleted: {total_deleted}")
    logging.info(f"Total time taken: {end_time - start_time}")

if __name__ == '__main__':
    main()

