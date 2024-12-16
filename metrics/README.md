# Hornet Metric Tools

The core idea of the metric tools is to provide reproducibility to the dataset. By providing the metrics and how to calculate them, we make the research and results more reproducible and transparent, and open it to the community for possible expansion and improvements.

The tools are separated in two groups:
- By source: to get metrics for a specific scenario or region.
- All data: to get metrics for the entire dataset.

## duckdb_flows_per_day_per_source.py

Generates a CSV with flows per honeypot per day from DuckDB.

```bash
:~$ python3 metrics/duckdb_flows_per_day_per_source.py --help
usage: duckdb_flows_per_day_per_source.py [-h] --db_name DB_NAME --output_csv OUTPUT_CSV

Generate CSV with flows per honeypot per day from DuckDB.

options:
  -h, --help            show this help message and exit
  --db_name DB_NAME     Path to the DuckDB database file
  --output_csv OUTPUT_CSV
                        Path to the output CSV file
```

Example of how to run:
```bash
:~$ python3 metrics/duckdb_flows_per_day_per_source.py \
    --db_name ../CTU-Hornet-65-Niner/duckdb/ctu-hornet-65-niner_v0.1.db \
    --output_csv /tmp/output.csv

CSV with flows per honeypot per day saved to /tmp/output.csv
```

## duckdb_metrics.py

This tool allows to extract features and metrics from DuckDB. Each metric is usually a parameter that can be invoked manually.

```bash
:~$ python3 metrics/duckdb_metrics.py --help
usage: duckdb_metrics.py [-h] [--log_level LOG_LEVEL] [--log_file LOG_FILE] --db_name DB_NAME [--info] [--metrics] [--total_flows] [--total_bytes] [--total_packets] [--packets_per_honeypot_source] [--bytes_per_honeypot_source] [--flows_per_honeypot_source]
                         [--flows_by_proto_source] [--unique_source_ips] [--unique_source_ips_per_honeypot]

Extract features and metrics from DuckDB.

options:
  -h, --help            show this help message and exit
  --log_level LOG_LEVEL
                        Logging level (default: INFO)
  --log_file LOG_FILE   Log file name (default: feature_extraction.log)
  --db_name DB_NAME     Path to the DuckDB database file
  --info                Print general information about the database
  --metrics             Calculate all available metrics
  --total_flows         Calculate the total flows
  --total_bytes         Calculate the total bytes
  --total_packets       Calculate the total packets
  --packets_per_honeypot_source
                        Calculate the packets per honeypot source
  --bytes_per_honeypot_source
                        Calculate the bytes per honeypot source
  --flows_per_honeypot_source
                        Calculate the flows per honeypot source
  --flows_by_proto_source
                        Calculate the flows by protocol and source
  --unique_source_ips   Calculate the total unique source IP addresses
  --unique_source_ips_per_honeypot
                        Calculate the total unique source IPs per honeypot source
```

Example of how to use:
```bash
:~$ python3 metrics/duckdb_metrics.py \
    --db_name ../CTU-Hornet-65-Niner/duckdb/ctu-hornet-65-niner_v0.1.db \
    --total_flows

Total flows: 12477164
```

## Metrics for L7 traffic

The database (DuckDB) contains only conn.log. Zeek generates additional log files for protocols it recognizes from the traffic, such as HTTP and DNS, etc. To get the total number of flows per honeypot scenario for each of these recognized application protocols, we used command line tools.

To retrieve the number of flows per honeypot scenario for the protocol:
* `DNS`: `PROTO="dns"; for HONEY in zeek/*; do zcat $HONEY/*/${PROTO}.*.gz 2>/dev/null | wc -l; done`
* `HTTP`: `PROTO="http"; for HONEY in zeek/*; do zcat $HONEY/*/${PROTO}.*.gz 2>/dev/null | wc -l; done`
* `NTP`: `PROTO="ntp"; for HONEY in zeek/*; do zcat $HONEY/*/${PROTO}.*.gz 2>/dev/null | wc -l; done`
* `SMTP`: `PROTO="smtp"; for HONEY in zeek/*; do zcat $HONEY/*/${PROTO}.*.gz 2>/dev/null | wc -l; done`
* `SSH`: `PROTO="ssh"; for HONEY in zeek/*; do zcat $HONEY/*/${PROTO}.*.gz 2>/dev/null | wc -l; done`
* `SSL`: `PROTO="ssl"; for HONEY in zeek/*; do zcat $HONEY/*/${PROTO}.*.gz 2>/dev/null | wc -l; done`
* `SIP`: `PROTO="sip"; for HONEY in zeek/*; do zcat $HONEY/*/${PROTO}.*.gz 2>/dev/null | wc -l; done`
* `SNMP`: `PROTO="snmp"; for HONEY in zeek/*; do zcat $HONEY/*/${PROTO}.*.gz 2>/dev/null | wc -l; done`
* `KERBEROS`: `PROTO="kerberos"; for HONEY in zeek/*; do zcat $HONEY/*/${PROTO}.*.gz 2>/dev/null | wc -l; done`
* `RADIUS`: `PROTO="radius"; for HONEY in zeek/*; do zcat $HONEY/*/${PROTO}.*.gz 2>/dev/null | wc -l; done`
* `DHCP`: `PROTO="dhcp"; for HONEY in zeek/*; do zcat $HONEY/*/${PROTO}.*.gz 2>/dev/null | wc -l; done`