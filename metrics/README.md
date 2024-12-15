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

TODO: add description and usage
