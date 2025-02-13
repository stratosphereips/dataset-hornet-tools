# Hornet Zeek Cleaning Tools

These set of scripts help on various data cleaning tasks. Data cleaning can take place on the raw source files (.gz) or on the Database.

## zeek_find_missmatch_bytes_pkts

There is a known bug in Zeek that affects the flow bytes in the `conn.log` [[ref](https://github.com/zeek/zeek/issues/3313)]. This script attempts to identify the number of affected flows to help evaluate what is the best strategy to cleaning up.

```bash
:~$ python3 zeek_find_missmatch_bytes_pkts.py --help
usage: zeek_find_missmatch_bytes_pkts.py [-h] [--mtu MTU] [--tcp_ip_overhead TCP_IP_OVERHEAD] [--log_file LOG_FILE] [--db DB]

Check network flows for byte discrepancies.

options:
  -h, --help            show this help message and exit
  --mtu MTU             Maximum Transmission Unit (MTU). Default is 1500 bytes.
  --tcp_ip_overhead TCP_IP_OVERHEAD
                        TCP/IP overhead in bytes. Default is 40 bytes.
  --log_file LOG_FILE   Log file to write errors to. Default is flow_errors.log.
  --db DB               Path to the DuckDB database file. Default is db/ctu-hornet-65-niner_v0.1.db.
```


## zeek_purge_batch-uid_from_data.sh

Safely process Zeek logs to delete a specific UID from the logs, in batches.

```bash
:~$ bash zeek_purge_batch-uid_from_data.sh
Usage: zeek_purge_batch-uid_from_data.sh -u UID_LIST_FILE -p PATH_TO_LOGS
```

## zeek_purge_ip_from_data.sh

This utility reads from a folder of Zeek logs (.gz) and processes them to remove a given IP. The script performs various safety checks, including creating backups of each log before modifying them.

```bash
:~$ bash zeek_purge_ip_from_data.sh
Usage: zeek_purge_ip_from_data.sh -i IP_TO_REMOVE -p PATH_TO_LOGS
:~$ # bash zeek_purge_ip_from_data.sh -i w.x.y.z -p /opt/zeek/logs/2024-05-31/
```

## zeek_purge_uid_from_data.sh

Safely process Zeek logs to delete a specific UID from the logs. One UID passed as parameter.

```bash
:~$ bash zeek_purge_uid_from_data.sh
Usage: zeek_purge_uid_from_data.sh -u UID_TO_REMOVE -p PATH_TO_LOGS

```

## zeek_purge_uid_from_db.py

Safely delete DuckDB entries matching a UID.

```bash
:~$ python3 zeek_purge_uid_from_db.py --help
usage: zeek_purge_uid_from_db.py [-h] --db_name DB_NAME --uid_file UID_FILE [--log_file LOG_FILE] [--confirm]

Delete Zeek log entries by UID from DuckDB.

options:
  -h, --help           show this help message and exit
  --db_name DB_NAME    Path to the DuckDB database file
  --uid_file UID_FILE  File containing UIDs to delete, one per line
  --log_file LOG_FILE  Log file name (default: deletion.log)
  --confirm            Ask for confirmation before deleting
```
