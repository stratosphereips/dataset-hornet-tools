import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import argparse

def create_small_multiples_from_wide_csv(csv_file, chart_file):
    """
    Create small multiples (facet plots) for flows per honeypot source over time.
    """
    try:
        # Read the CSV file
        df = pd.read_csv(csv_file)

        # Ensure the source column is treated as an index
        df.set_index('source', inplace=True)

        # Transpose the DataFrame to have dates as the index
        df = df.T

        # Convert the index to datetime
        df.index = pd.to_datetime(df.index, format='%Y-%m-%d', errors='coerce')

        # Define grid size for small multiples
        n_sources = df.columns.size
        n_cols = 3  # Number of columns in the grid
        n_rows = -(-n_sources // n_cols)  # Calculate rows needed (ceil division)

        # Set figure size
        fig, axes = plt.subplots(n_rows, n_cols, figsize=(16, 4 * n_rows), dpi=100)
        axes = axes.flatten()  # Flatten to make indexing easier

        # Plot each honeypot source on its own subplot
        for idx, column in enumerate(df.columns):
            ax = axes[idx]
            df[column].plot(kind='area', stacked=False, alpha=0.7, ax=ax, color='tab:blue')
            ax.set_title(column, fontsize=10)
            #ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))  # Format x-axis
            ax.xaxis.set_major_locator(mdates.AutoDateLocator())
            ax.tick_params(axis='x', rotation=45)

        # Remove empty subplots if any
        for idx in range(n_sources, len(axes)):
            fig.delaxes(axes[idx])

        # Set global title
        fig.suptitle("Flows Per Day Per Honeypot - Small Multiples", fontsize=14)

        # Adjust layout
        plt.tight_layout(rect=[0, 0, 1, 0.96])

        # Save the chart
        plt.savefig(chart_file, dpi=100, bbox_inches='tight')
        print(f"Chart saved to {chart_file}")

    except Exception as e:
        print(f"Error creating small multiples: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate small multiples for flows per honeypot source.")
    parser.add_argument("--csv_file", required=True, help="Path to the input CSV file.")
    parser.add_argument("--chart_file", default="/tmp/flows_small_multiples.png", help="Path to save the output chart.")
    args = parser.parse_args()

    create_small_multiples_from_wide_csv(args.csv_file, args.chart_file)

