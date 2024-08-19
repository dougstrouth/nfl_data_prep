import pandas as pd
import general_utilities as gu

gu.setup_logging()
duckdb_path = "/Users/dougstrouth/Documents/Code/datasets/sports/NFL/nfl.duckdb"

conn = gu.connect_to_duckdb(duckdb_path)

schema = "main"
tables = gu.get_schema_info(conn, schema)
distinct_table_names = tables["table_name"].unique()

for table_name in distinct_table_names:
    table_df = pd.DataFrame()
    print(f"Processing table: {table_name}")
    # Here you can perform operations specific to each table
    # For example, retrieve table data or schema details
    sql = f"SELECT * FROM {schema}.{table_name};"
    table_df = gu.sql_to_df(conn, sql)
    df = gu.clean_df_time_for_profiling(table_df)
    df = gu.clean_df_convert_object_for_profiling(df)
    gu.profiling(df, table_name)
    print(table_df.head())  # Print the first few rows of the table
conn.close()
