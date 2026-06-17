from pyspark.sql import functions as F


def calculate_payment_distribution(payments):
    return payments.groupBy("payment_type").agg(F.sum("payment_value").alias("total")).orderBy(F.desc("total"))
