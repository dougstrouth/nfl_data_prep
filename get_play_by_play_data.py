import nfl_data_py as nfl
import os
import chardet
import subprocess
import glob
import pandas as pd
import logging
from general_utilities import setup_logging
import duckdb
from chardet.universaldetector import UniversalDetector


def clean_column_names(df):
    """Convert column names to lowercase and replace spaces with underscores."""
    df.columns = [col.lower().replace(" ", "_") for col in df.columns]
    return df


def save_data_to_csv(data, folder_path, data_point, *args):
    """Save DataFrame to CSV file within a folder named after the data_point."""
    folder_path = os.path.join(folder_path, data_point)
    os.makedirs(folder_path, exist_ok=True)
    file_name = (
        f"{data_point}__{'_'.join(map(str, args))}.csv" if args else f"{data_point}.csv"
    )
    file_path = os.path.join(folder_path, file_name)

    data = clean_column_names(data)
    data.to_csv(file_path, index=False)
    logging.info(f"Successfully saved data to {file_path}.")


def fetch_and_save_weekly_rosters(file_path, start_year=2010, final_year=2024):
    """
    Fetch weekly rosters data year by year, align columns, and save to CSV.
    Had to break it up because each df that came back when calling more than one year
    wouldnt stack correctly in the way the library handles it
    Probably a parquet having indices in them thing when going out for data
    """
    years = list(range(start_year, final_year))

    combined_data = []

    try:
        for year in years:
            logging.info(f"Fetching data for year {year}...")
            data = nfl.import_weekly_rosters(years=[year])
            logging.debug(f"Data for year {year} fetched successfully.")

            # Reset index to remove duplicate indices
            if data.index.duplicated().any():
                logging.error(
                    f"Duplicate indices found for year {year}. Removing duplicates."
                )
                data = data[~data.index.duplicated(keep="first")]

            # Reset index if duplicates are still present
            if data.index.duplicated().any():
                data = data.reset_index(drop=True)
                logging.debug(f"Index reset for year {year}.")

            # Append the processed data
            combined_data.append(data)
            logging.info(f"Data for year {year} processed and appended.")

        # Concatenate all DataFrames into a single DataFrame
        if combined_data:
            final_data = pd.concat(combined_data, ignore_index=True)
            logging.debug("DataFrames concatenated successfully.")

            # Check for and remove duplicate columns
            if final_data.columns.duplicated().any():
                duplicate_columns = final_data.columns[
                    final_data.columns.duplicated()
                ].unique()
                logging.error(
                    f"Duplicate columns found in combined data: {duplicate_columns}"
                )
                final_data = final_data.loc[:, ~final_data.columns.duplicated()]
                logging.info("Duplicate columns removed from combined data.")

            # Save the combined data to CSV
            save_data_to_csv(
                final_data, file_path, "weekly_rosters", start_year, final_year - 1
            )
            logging.info("Combined data saved to CSV successfully.")

    except ValueError as ve:
        logging.error(f"ValueError occurred: {ve}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")


def fetch_play_by_play_data(years, folder_path):
    """Fetch and save play-by-play data for all years."""
    try:
        for year in years:
            data = nfl.import_pbp_data(years=[year])
            if not data.empty:
                data["year"] = year
                data["data_point"] = "play_by_play"
                save_data_to_csv(data, folder_path, "play_by_play", year)
    except Exception as e:
        logging.error(f"Failed to fetch or save play-by-play data: {e}")


def fetch_seasonal_pfr_data(folder_path):
    """Fetch and save seasonal PFR data."""

    seasonal_s_types = ["pass", "rec", "rush"]
    for s_type in seasonal_s_types:
        try:
            data = nfl.import_seasonal_pfr(s_type)
            if not data.empty:
                data["s_type"] = s_type
                data["data_point"] = "seasonal_pfr"
                save_data_to_csv(data, folder_path, "seasonal_pfr", s_type)
        except Exception as e:
            logging.error(
                f"Failed to fetch or save seasonal PFR data for s_type {s_type}: {e}"
            )


def fetch_weekly_pfr_data(folder_path):
    """Fetch and save weekly PFR data."""

    weekly_s_types = ["pass", "rec", "rush"]
    for s_type in weekly_s_types:
        try:
            data = nfl.import_weekly_pfr(s_type)
            if not data.empty:
                data["s_type"] = s_type
                data["data_point"] = "weekly_pfr"
                save_data_to_csv(data, folder_path, "weekly_pfr", s_type)
            else:
                logging.warning(f"No data returned for s_type {s_type}")
        except Exception as e:
            logging.error(
                f"Failed to fetch or save weekly PFR data for s_type {s_type}: {e}"
            )


def fetch_yearly_data(func, data_point, years, folder_path):
    """Fetch and save yearly data with a minimum year constraint."""
    for year in years:
        try:
            data = func(years=[year])
            if not data.empty:
                data["year"] = year
                data["data_point"] = data_point
                save_data_to_csv(data, folder_path, data_point, year)
        except Exception as e:
            logging.error(
                f"Failed to fetch or save {data_point} data for year {year}: {e}"
            )


def fetch_ids_data(folder_path):
    """Fetch and save IDs data."""
    try:
        data = nfl.import_ids()
        if not data.empty:
            data["data_point"] = "ids_data"
            save_data_to_csv(data, folder_path, "ids_data")
    except Exception as e:
        logging.error(f"Failed to fetch or save ids_data: {e}")


def fetch_team_descriptions(folder_path):
    """Fetch and save team descriptions."""
    try:
        data = nfl.import_team_desc()
        if not data.empty:
            data["data_point"] = "team_descriptions"
            save_data_to_csv(data, folder_path, "team_descriptions")
    except Exception as e:
        logging.error(f"Failed to fetch or save team_descriptions: {e}")


def fetch_and_save_data(folder_path, start_year=2010, final_year=2024):
    """Fetch and save NFL data to CSV files."""
    years = list(range(start_year, final_year))

    min_years = {"depth_charts": 2001, "injuries": 2009, "qbr": 2006, "ftn_data": 2022}

    fetch_and_save_weekly_rosters(folder_path, start_year, final_year)
    print("weekly_roster saved to : ",folder_path)
    fetch_play_by_play_data(years, folder_path)
    fetch_seasonal_pfr_data(folder_path)
    fetch_weekly_pfr_data(folder_path)

    for data_point, min_year in min_years.items():
        func = getattr(nfl, f"import_{data_point}", None)
        if func:
            adjusted_years = list(range(min_year + 1, final_year))
            fetch_yearly_data(func, data_point, adjusted_years, folder_path)




def detect_and_convert_encoding(file_path, max_sample_size=52428800):  # 50MB
    """Detects and converts file encoding to UTF-8 if necessary."""

    detector = UniversalDetector()
    bytes_read = 0

    # Open the file in binary mode and read chunks to detect encoding
    with open(file_path, "rb") as file:
        while True:
            chunk = file.read(1048576)  # 1MB chunks
            if not chunk or bytes_read >= max_sample_size:
                break
            detector.feed(chunk)
            bytes_read += len(chunk)
            if detector.done:
                break
        detector.close()

    # Get detected encoding
    encoding = detector.result["encoding"]

    # If encoding is not UTF-8, convert the file
    if encoding and encoding.lower() != "utf-8":
        logging.info("Converting %s from %s to UTF-8", file_path, encoding)

        temp_file_path = file_path + ".tmp"
        try:
            with open(temp_file_path, "wb") as output_file:
                subprocess.run(
                    ["iconv", "-f", encoding, "-t", "UTF-8", file_path],
                    check=True,
                    stdout=output_file,
                )
            os.replace(temp_file_path, file_path)
            logging.info("Conversion successful for %s", file_path)
        except subprocess.CalledProcessError as e:
            logging.error("Conversion failed for %s: %s", file_path, e)
            if os.path.exists(temp_file_path):
                os.remove(temp_file_path)
    else:
        logging.info(
            "No conversion needed for %s, detected encoding: %s", file_path, encoding
        )


def convert_and_upload_csvs_to_duckdb(
    duckdb_file, schema_name, folder_path, max_sample_size=52428800
):  # 50MB
    """Detect, convert if necessary, and upload CSVs to DuckDB."""

    conn = duckdb.connect(duckdb_file)

    # Create schema if it doesn't exist
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")

    # Search for all CSV files in the folder and its subdirectories
    csv_files = glob.glob(os.path.join(folder_path, "*.csv"), recursive=True)
    logging.debug("CSV Files:\n%s", csv_files)

    for csv_file in csv_files:
        # Detect and convert encoding
        detect_and_convert_encoding(csv_file, max_sample_size=max_sample_size)

        # Extract table name from the CSV file path
        table_name = os.path.splitext(os.path.basename(csv_file))[0]

        # Create SQL statement to read CSV and create a table
        sql = f"""
        DROP TABLE IF EXISTS {schema_name}.{table_name};

        CREATE TABLE {schema_name}.{table_name} AS
        SELECT * FROM read_csv('{csv_file}');
        """

        # Execute SQL statement
        try:
            conn.execute(sql)
            logging.info("Table %s created successfully in DuckDB.", table_name)
        except Exception as e:
            logging.error("Failed to create table %s in DuckDB: %s", table_name, e)

    conn.close()

import os
import duckdb
import sys

def check_or_create_duckdb_file(duckdb_file_path):
    """Checks if the DuckDB file exists and is accessible. 
    If it doesn't exist, creates it. If it exists but is not accessible, stops the script."""
    
    if not os.path.isfile(duckdb_file_path):
        try:
            # Attempt to create the DuckDB file by opening a connection
            conn = duckdb.connect(duckdb_file_path)
            conn.close()
            logging.info("DuckDB file created at %s", duckdb_file_path)
        except Exception as e:
            logging.error("Failed to create DuckDB file at %s: %s", duckdb_file_path, e)
            sys.exit(1)  # Stop the script if creation fails
    else:
        try:
            # Try to open the existing DuckDB file
            conn = duckdb.connect(duckdb_file_path)
            conn.close()
            logging.info("DuckDB file %s is accessible.", duckdb_file_path)
        except Exception as e:
            logging.error("DuckDB file %s exists but cannot be opened: %s", duckdb_file_path, e)
            sys.exit(1)  # Stop the script if the file cannot be opened
def main():
    file_path = (
        "/Users/dougstrouth/Documents/Code/datasets/sports/NFL/raw_data/play_by_play"
    )
    setup_logging()
    fetch_and_save_data(file_path)


# Example usage:
# main()

if __name__ == "__main__":
    main()
