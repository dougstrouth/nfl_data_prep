import logging
import duckdb
import glob
import os


def execute_sql_from_file(sql_file, folder_path, duckdb_file):
    # Read the SQL file content
    with open(sql_file, "r") as file:
        sql_content = file.read()

    # Replace the placeholder with actual path
    sql_content = sql_content.format(path=folder_path)

    # Connect to DuckDB
    conn = duckdb.connect(duckdb_file)

    # Execute the SQL script
    conn.execute(sql_content)

    # Close the connection
    conn.close()


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


def create_tables_from_csvs(duckdb_file, schema_name, folder_path):
    # Construct the pattern to search for CSV files in all subdirectories
    pattern = os.path.join(folder_path, "**", "*.csv")
    csv_files = glob.glob(pattern, recursive=True)

    conn = duckdb.connect(duckdb_file)

    for csv_file in csv_files:
        # Extract table name from the CSV file name
        table_name = os.path.splitext(os.path.basename(csv_file))[0]

        # Create SQL statement to read CSV and create a table
        sql = f"""
        DROP TABLE IF EXISTS {schema_name}.{table_name};

        CREATE TABLE {schema_name}.{table_name} AS
        SELECT * FROM read_csv('{csv_file}', union_by_name=true);
        """

        # Execute SQL statement
        try:
            conn.execute(sql)
            print(f"Table {table_name} created successfully.")
        except Exception as e:
            print(f"Failed to create table {table_name}: {e}")

    conn.close()


folder_path = (
    "/Users/dougstrouth/Documents/Code/datasets/sports/NFL/raw_data/play_by_play"
)
duckdb_file = (
    "/Users/dougstrouth/Documents/Code/datasets/sports/NFL/raw_data/nfl_data.duckdb"
)
schema= "play_by_play"
# Example usage
#create_tables_from_csvs(duckdb_file,schema , folder_path)
query_duckdb_metadata(duckdb_file,schema)

# Example usage
# create_tables_from_csv('', duckdb_file, 'play_by_play')

# Define the SQL file containing the table creation script
sql_file = "play_by_play_setup.sql"
# Example usage:
# execute_sql_from_file("play_by_play_setup.sql",folder_path,duckdb_file)
