# Dataset Hornet Tools

This repository contains a tool suite to work with the Stratosphere Hornet Datasets of geografically distributed honeypots.

# Cleaning, Ingestion, and Analysis Tools

The repository contains scripts and tools to process the Hornet data. These are organised by activity:

 - **Cleaning**: contains scripts used for data cleaning, such as removing of specific IPs, etc.
 - **Ingestion**: contains tools and scripts to ingest the data into a usable DB that can be used for further processing.
 - **Metrics**: contains tools and scripts to perform various data analysis tasks, gathering of statistics, etc.

# Dataset Metrics

To generate the key dataset metrics as they appear in the paper, we recommend running the `metrics/duckdb_metrics.py` tool with the parameter `--metrics`:

```bash
python3 metrics/duckdb_metrics.py --db_name ./ctu-hornet-65-niner_v0.1.db --metrics
```

For additional information see the additional information on the `metrics/` folder.
# About

This repo was developed at the Stratosphere Laboratory at the Czech Technical University in Prague.
