import nfl_data_py as nfl
import os
import logging
from general_utilities import setup_logging


import nfl_data_py as nfl
import pandas as pd
import logging
import os
from general_utilities import setup_logging


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
                logging.error(f"Duplicate indices found for year {year}. Removing duplicates.")
                data = data[~data.index.duplicated(keep='first')]

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
                duplicate_columns = final_data.columns[final_data.columns.duplicated()].unique()
                logging.error(f"Duplicate columns found in combined data: {duplicate_columns}")
                final_data = final_data.loc[:, ~final_data.columns.duplicated()]
                logging.info("Duplicate columns removed from combined data.")

            # Save the combined data to CSV
            save_data_to_csv(final_data, file_path, "weekly_rosters", start_year, final_year - 1)
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
    # fetch_play_by_play_data(years, folder_path)
    # fetch_seasonal_pfr_data(folder_path)
    # fetch_weekly_pfr_data(folder_path)

    for data_point, min_year in min_years.items():
        func = getattr(nfl, f"import_{data_point}", None)
        if func:
            adjusted_years = list(range(min_year + 1, final_year))
            # fetch_yearly_data(func, data_point, adjusted_years, folder_path)

    # fetch_ids_data(folder_path)
    # fetch_team_descriptions(folder_path)


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
