from pyspark.sql import SparkSession
from src.pipeline import run_pipeline


def main():
    spark = (
        SparkSession.builder
        .appName("Olist-Pipeline")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .getOrCreate()
    )

    run_pipeline(spark)

    spark.stop()


if __name__ == "__main__":
    main()