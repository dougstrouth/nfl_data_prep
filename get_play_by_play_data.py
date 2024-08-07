import nfl_data_py as nfl
import pandas as pd
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

def clean_column_names(df):
    """Convert column names to lowercase and replace spaces with underscores."""
    df.columns = [col.lower().replace(" ", "_") for col in df.columns]
    return df

def save_data_to_csv(data, folder_path, data_point, *args):
    """Save DataFrame to CSV file within a folder named after the data_point."""
    folder_path = os.path.join(folder_path, data_point)
    os.makedirs(folder_path, exist_ok=True)
    file_name = f"{data_point}__{'_'.join(map(str, args))}.csv" if args else f"{data_point}.csv"
    file_path = os.path.join(folder_path, file_name)

    data = clean_column_names(data)
    data.to_csv(file_path, index=False)
    logging.info(f"Successfully saved data to {file_path}.")

def fetch_play_by_play_data(years, folder_path):
    """Fetch and save play-by-play data for all years."""
    try:
        for year in years:
            data = nfl.import_pbp_data(years=[year])
            if not data.empty:
                data['year'] = year
                data['data_point'] = 'play_by_play'
                save_data_to_csv(data, folder_path, 'play_by_play', year)
    except Exception as e:
        logging.error(f"Failed to fetch or save play-by-play data: {e}")

def fetch_seasonal_pfr_data(years, folder_path):
    """Fetch and save seasonal PFR data."""
    seasonal_s_types = ["pass", "rec", "rush"]
    for s_type in seasonal_s_types:
        try:
            data = nfl.import_seasonal_pfr(s_type, years)
            if not data.empty:
                data['s_type'] = s_type
                data['data_point'] = 'seasonal_pfr'
                for year in years:
                    year_data = data[data['year'] == year]
                    if not year_data.empty:
                        save_data_to_csv(year_data, folder_path, 'seasonal_pfr', s_type, year)
        except Exception as e:
            logging.error(f"Failed to fetch or save seasonal PFR data for s_type {s_type}: {e}")

def fetch_weekly_pfr_data(years, folder_path):
    """Fetch and save weekly PFR data."""
    weekly_s_types = ["pass", "rec", "rush"]
    for s_type in weekly_s_types:
        if all(year < 2018 for year in years):  # Ensure data availability based on years
            continue
        try:
            data = nfl.import_weekly_pfr(s_type, years)
            if not data.empty:
                data['s_type'] = s_type
                data['data_point'] = 'weekly_pfr'
                for year in years:
                    year_data = data[data['year'] == year]
                    if not year_data.empty:
                        save_data_to_csv(year_data, folder_path, 'weekly_pfr', s_type, year)
        except Exception as e:
            logging.error(f"Failed to fetch or save weekly PFR data for s_type {s_type}: {e}")

def fetch_yearly_data(func, data_point, years, folder_path):
    """Fetch and save yearly data with a minimum year constraint."""
    for year in years:
        try:
            data = func(years=[year])
            if not data.empty:
                data['year'] = year
                data['data_point'] = data_point
                save_data_to_csv(data, folder_path, data_point, year)
        except Exception as e:
            logging.error(f"Failed to fetch or save {data_point} data for year {year}: {e}")

def fetch_ids_data(folder_path):
    """Fetch and save IDs data."""
    try:
        data = nfl.import_ids()
        if not data.empty:
            data['data_point'] = 'ids_data'
            save_data_to_csv(data, folder_path, 'ids_data')
    except Exception as e:
        logging.error(f"Failed to fetch or save ids_data: {e}")

def fetch_team_descriptions(folder_path):
    """Fetch and save team descriptions."""
    try:
        data = nfl.import_team_desc()
        if not data.empty:
            data['data_point'] = 'team_descriptions'
            save_data_to_csv(data, folder_path, 'team_descriptions')
    except Exception as e:
        logging.error(f"Failed to fetch or save team_descriptions: {e}")

def fetch_and_save_data(folder_path, years=None):
    """Fetch and save NFL data to CSV files."""
    if years is None:
        years = list(range(2010, 2024))

    min_years = {
        "depth_charts": 2001,
        "injuries": 2009,
        "qbr": 2006,
        "ftn_data": 2022
    }

    fetch_play_by_play_data(years, folder_path)
    fetch_seasonal_pfr_data(years, folder_path)
    fetch_weekly_pfr_data(years, folder_path)

    for data_point, min_year in min_years.items():
        func = getattr(nfl, f"import_{data_point}", None)
        if func:
            fetch_yearly_data(func, data_point, years, folder_path)

    fetch_ids_data(folder_path)
    fetch_team_descriptions(folder_path)


