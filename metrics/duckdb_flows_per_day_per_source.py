import duckdb
import pandas as pd
import argparse

# to_timestamp(ts)::timestamptz AS date,
# CAST(to_timestamp(ts) AS DATE) AS date,
def flows_per_honeypot_per_day(db_name, output_csv, timezone='UTC'):
    """Generate a CSV with the number of flows per honeypot source per day."""
    con = duckdb.connect(db_name)
    
    # Query to get the number of flows per honeypot per day
    query = """
    SELECT 
        source, 
        CAST(to_timestamp(ts) AT TIME ZONE '{timezone}' AS DATE) AS date,
        COUNT(*) as flow_count
    FROM 
        logs
    GROUP BY 
        source, date
    ORDER BY 
        source, date;
    """
    
    # Execute the query and load into a pandas DataFrame
    df = con.execute(query).df()
    
    # Pivot the DataFrame to have honeypots as rows and days as columns
    pivot_df = df.pivot(index='source', columns='date', values='flow_count').fillna(0)
    
    # Save the result to a CSV file
    pivot_df.to_csv(output_csv)
    
    con.close()
    print(f"CSV with flows per honeypot per day saved to {output_csv}")

def main():
    """Main function to parse arguments and run the flow aggregation."""
    parser = argparse.ArgumentParser(description="Generate CSV with flows per honeypot per day from DuckDB.")
    parser.add_argument('--db_name', required=True, help='Path to the DuckDB database file')
    parser.add_argument('--output_csv', required=True, help='Path to the output CSV file')
    
    args = parser.parse_args()
    
    flows_per_honeypot_per_day(args.db_name, args.output_csv)

if __name__ == '__main__':
    main()

