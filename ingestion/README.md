# Hornet Ingestion Tools

To facilitate the processing and analysis of Zeek logs, we ingest them into a structural DB. We chose DuckDB as it is better optimized for this type of data. Having the data in a DB makes it easier to create tooling and applications surrouding it.

## zeek_ingest_connlog_by_source.py

This is the main tool to ingest data in DuckDB. To see the structure of the DB, see the source code of the tool.

```bash
:~$ python3 zeek_ingest_connlog_by_source.py --help
usage: zeek_ingest_connlog_by_source.py [-h] [--log_dir LOG_DIR] [--source SOURCE] [--db_name DB_NAME] [--log_file LOG_FILE] [--workers WORKERS] [--log_level LOG_LEVEL]

Load log data into DuckDB.

options:
  -h, --help            show this help message and exit
  --log_dir LOG_DIR     Folder to read log files from
  --source SOURCE       Source name for the logs
  --db_name DB_NAME     Database name (default: logs.db)
  --log_file LOG_FILE   Log file name (default: log_import.log)
  --workers WORKERS     Number of worker threads (default: 4)
  --log_level LOG_LEVEL
                        Logging level (default: INFO)
```

An example of how to use it is shown below:

```bash
python3 ingestion/zeek_ingest_connlog_by_source.py \
    --log_dir ../zeek/Honeypot-Cloud-DigitalOcean-Geo-6 \
    --source "Honeypot-Cloud-DigitalOcean-Geo-6" \
    --db_name ../db/ctu-hornet-65-niner_v0.1.db \
    --log_level DEBUG
```