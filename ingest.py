import duckdb
import pandas as pd

# Input CSV file
CSV_PATH = "ads_spend.csv"

# Output DuckDB database
DB_PATH = "ads_spend.duckdb"

# Table name inside DuckDB
TABLE_NAME = "ads_spend"

def main():
    print("Loading CSV into DuckDB...")

    # Read CSV into pandas
    df = pd.read_csv(CSV_PATH)

    # Connect to DuckDB (creates the file if it doesn’t exist)
    con = duckdb.connect(DB_PATH)

    # Write the dataframe into DuckDB
    con.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    con.execute(f"CREATE TABLE {TABLE_NAME} AS SELECT * FROM df")

    print(f"Data from {CSV_PATH} loaded into {DB_PATH} (table: {TABLE_NAME})")

if __name__ == "__main__":
    main()
