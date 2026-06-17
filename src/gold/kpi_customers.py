from pyspark.sql import functions as F


def calculate_avg_review(reviews):
    return reviews.select(F.avg("review_score")).collect()[0][0]
