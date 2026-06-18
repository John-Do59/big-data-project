# src/ingestion/to_bronze.py

import os

BRONZE_PATH = "data/bronze"

def write_bronze(df, name):
    output_path = f"{BRONZE_PATH}/{name}"

    (
        df.write
        .mode("overwrite")
        .parquet(output_path)
    )

    print(f"Saved bronze table: {name} -> {output_path}")


def save_all_bronze(dataframes: dict):
    os.makedirs(BRONZE_PATH, exist_ok=True)

    for name, df in dataframes.items():
        write_bronze(df, name)


if __name__ == "__main__":
    print("This module expects dataframes from load_raw.py")