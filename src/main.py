from pyspark.sql import SparkSession
from src.pipeline import run_pipeline


def main():
    spark = (
        SparkSession.builder
        .appName("Olist-Pipeline")
        .master("local[*]")
        .getOrCreate()
    )

    run_pipeline(spark)

    spark.stop()


if __name__ == "__main__":
    main()