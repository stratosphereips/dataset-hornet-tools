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


## zeek_purge_batch_uid_from_data.sh

TODO: Add description and usage 

## zeek_purge_ip_from_data.sh

This utility reads from a folder of Zeek logs (.gz) and processes them to remove a given IP. The script performs various safety checks, including creating backups of each log before modifying them.

```bash
:~$ bash zeek_purge_ip_from_data.sh
Usage: zeek_purge_ip_from_data.sh -i IP_TO_REMOVE -p PATH_TO_LOGS
:~$ # bash zeek_purge_ip_from_data.sh -i w.x.y.z -p /opt/zeek/logs/2024-05-31/
```

## zeek_purge_uid_from_data.sh

TODO: Add description and usage 

## zeek_purge_uid_from_db.py

TODO: Add description and usage 
