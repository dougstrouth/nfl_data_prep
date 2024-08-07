import os
import requests
import pandas as pd
import subprocess
import duckdb
from bs4 import BeautifulSoup


def ingest_data_to_duckdb(internal_data_folder):
    # Define the path for the DuckDB file
    duckdb_file_path = os.path.join(internal_data_folder, "nfl_data.duckdb")

    # Ensure the DuckDB file exists or create it using the CLI command
    if not os.path.exists(duckdb_file_path):
        subprocess.run(
            ["duckdb", duckdb_file_path, "-s", "CREATE SCHEMA IF NOT EXISTS check_csv"]
        )

    # Load internal data sources
    spreadspoke_scores_path = os.path.join(
        internal_data_folder, "spreadspoke_scores.csv"
    )
    nfl_teams_path = os.path.join(internal_data_folder, "nfl_teams.csv")

    spreadspoke_scores = pd.read_csv(spreadspoke_scores_path)
    nfl_teams = pd.read_csv(nfl_teams_path)

    # Request and load external data sources
    # Pro Football Reference (1966-2017)
    years = range(1966, 2017 + 1)
    pfr_games = []

    for year in years:
        url = (
            f"https://www.pro-football-reference.com/years/{year}/games.htm#games::none"
        )
        response = requests.get(url)
        soup = BeautifulSoup(response.content, "html.parser")
        table = soup.find("table")
        df = pd.read_html(str(table))[0]
        df["year"] = year
        pfr_games.append(df)

    pfr_games_df = pd.concat(pfr_games, ignore_index=True)
    pfr_games_csv_path = os.path.join(internal_data_folder, "pfr_games_1966_2017.csv")
    pfr_games_df.to_csv(pfr_games_csv_path, index=False)

    # Aussportsbetting.com Historical NFL Data
    url = "http://www.aussportsbetting.com/historical_data/nfl.xlsx"
    response = requests.get(url)
    nfl_asb_excel_path = os.path.join(internal_data_folder, "nfl_aussportsbetting.xlsx")

    with open(nfl_asb_excel_path, "wb") as file:
        file.write(response.content)

    # Convert Excel to CSV
    nfl_asb = pd.read_excel(nfl_asb_excel_path)
    nfl_asb_csv_path = os.path.join(internal_data_folder, "nfl_aussportsbetting.csv")
    nfl_asb.to_csv(nfl_asb_csv_path, index=False)

    # Connect to DuckDB and create tables
    con = duckdb.connect(duckdb_file_path)

    # Creating tables and inserting data with the required format
    con.execute(
        f"CREATE TABLE IF NOT EXISTS check_csv.spreadspoke_scores AS SELECT * FROM read_csv_auto('{spreadspoke_scores_path}')"
    )
    con.execute(
        f"CREATE TABLE IF NOT EXISTS check_csv.nfl_teams AS SELECT * FROM read_csv_auto('{nfl_teams_path}')"
    )
    con.execute(
        f"CREATE TABLE IF NOT EXISTS check_csv.pfr_games AS SELECT * FROM read_csv_auto('{pfr_games_csv_path}')"
    )
    con.execute(
        f"CREATE TABLE IF NOT EXISTS check_csv.nfl_asb AS SELECT * FROM read_csv_auto('{nfl_asb_csv_path}')"
    )

    # Close the connection
    con.close()


# Example usage:
internal_data_path = "/Users/dougstrouth/Documents/Code/datasets/sports/NFL/raw_data"
ingest_data_to_duckdb(internal_data_path)
