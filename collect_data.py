import os
import requests
import pandas as pd
import subprocess
import duckdb
from bs4 import BeautifulSoup
from io import StringIO
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def download_file(url, local_path):
    """Download a file from a URL to a local path."""
    try:
        response = requests.get(url)
        response.raise_for_status()  # Check if the request was successful
        with open(local_path, "wb") as file:
            file.write(response.content)
        logging.info(f"Downloaded file from {url} to {local_path}")
    except requests.RequestException as e:
        logging.error(f"Failed to download file from {url}: {e}")
        raise


def convert_excel_to_csv(excel_path, csv_path):
    """Convert an Excel file to a CSV file."""
    try:
        df = pd.read_excel(excel_path)
        df.columns = [col.replace(" ", "_") for col in df.columns]
        df.columns = [col.replace("?", "") for col in df.columns]

        df.to_csv(csv_path, index=False)
        logging.info(f"Converted {excel_path} to {csv_path}")
    except Exception as e:
        logging.error(f"Failed to convert {excel_path} to CSV: {e}")
        raise


def create_duckdb_tables(duckdb_file_path, csv_paths):
    """Create tables in DuckDB from CSV files."""
    try:
        con = duckdb.connect(duckdb_file_path)
        for table_name, csv_path in csv_paths.items():
            con.execute(f"DROP TABLE check_csv.{table_name}")
            con.execute(
                f"CREATE TABLE IF NOT EXISTS check_csv.{table_name} AS SELECT * FROM read_csv_auto('{csv_path}')"
            )
        logging.info("Created tables in DuckDB.")
    except Exception as e:
        logging.error(f"Failed to create tables in DuckDB: {e}")
        raise
    finally:
        con.close()


def query_duckdb_metadata(duckdb_file_path, schema="check_csv"):
    """Query DuckDB's information_schema.columns view and return a DataFrame."""
    try:
        con = duckdb.connect(duckdb_file_path)
        query = f"""
        SELECT table_schema, table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = '{schema}'
        """
        df = con.execute(query).fetchdf()
        logging.info("Queried DuckDB metadata.")

        # Save the DataFrame to a CSV file
        metadata_csv_path = os.path.join(
            os.path.dirname(duckdb_file_path), "metadata.csv"
        )
        df.to_csv(metadata_csv_path, index=False)
        logging.info(f"Metadata saved to {metadata_csv_path}")
        return df
    except Exception as e:
        logging.error(f"Failed to query DuckDB metadata: {e}")
        raise
    finally:
        con.close()


"""
Need to break this apart and add where it iterates through a 
folder to create tables based on all the csv except those defined
to exclude like data_definition.csv


"""


def ingest_data_to_duckdb(internal_data_folder):
    """Main function to ingest data into DuckDB."""
    duckdb_file_path = os.path.join(internal_data_folder, "nfl_data.duckdb")

    # Ensure the DuckDB file exists or create the schema
    if not os.path.exists(duckdb_file_path):
        subprocess.run(
            ["duckdb", duckdb_file_path, "-s", "CREATE SCHEMA IF NOT EXISTS check_csv"]
        )

    # Load internal data sources
    spreadspoke_scores_path = os.path.join(
        internal_data_folder, "spreadspoke_scores.csv"
    )
    nfl_teams_path = os.path.join(internal_data_folder, "nfl_teams.csv")

    # Aussportsbetting.com Historical NFL Data
    nfl_asb_url = "http://www.aussportsbetting.com/historical_data/nfl.xlsx"
    nfl_asb_excel_path = os.path.join(internal_data_folder, "nfl_aussportsbetting.xlsx")
    nfl_asb_csv_path = os.path.join(internal_data_folder, "nfl_aussportsbetting.csv")

    download_file(nfl_asb_url, nfl_asb_excel_path)
    convert_excel_to_csv(nfl_asb_excel_path, nfl_asb_csv_path)

    # Prepare paths for DuckDB
    csv_paths = {
        "spreadspoke_scores": spreadspoke_scores_path,
        "nfl_teams": nfl_teams_path,
        "nfl_asb": nfl_asb_csv_path,
    }

    # Create tables in DuckDB
    create_duckdb_tables(duckdb_file_path, csv_paths)


# Example usage
internal_data_path = "/Users/dougstrouth/Documents/Code/datasets/sports/NFL/raw_data"
ingest_data_to_duckdb(internal_data_path)

# Query DuckDB metadata
duckdb_file_path = os.path.join(internal_data_path, "nfl_data.duckdb")
metadata_df = query_duckdb_metadata(duckdb_file_path)
print(metadata_df)
