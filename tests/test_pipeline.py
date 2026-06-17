import pytest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from src.transformations.silver_cleaning import (
    clean_orders,
    clean_order_items,
    clean_payments,
    clean_reviews,
    clean_products
)
from src.gold.kpi_sales import (
    calculate_total_revenue,
    calculate_avg_basket,
    calculate_monthly_revenue
)
from src.gold.kpi_delivery import calculate_late_rate
from src.gold.kpi_customers import calculate_avg_review
from src.transformations.quality_checks import check_nulls, check_duplicates


# ─────────────────────────────────────────────
# FIXTURE SPARK
# ─────────────────────────────────────────────

@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder \
        .appName("TestPipeline") \
        .master("local[1]") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("OFF")
    yield spark
    spark.stop()


# ─────────────────────────────────────────────
# TEST 1 — Session Spark
# ─────────────────────────────────────────────

def test_spark_session(spark):
    assert spark is not None


# ─────────────────────────────────────────────
# TEST 2 — clean_orders : suppression doublons
# ─────────────────────────────────────────────

def test_clean_orders_removes_duplicates(spark):
    data = [
        Row(order_id="1", customer_id="c1", order_status="delivered",
            order_purchase_timestamp="2021-01-01 10:00:00",
            order_approved_at=None,
            order_delivered_carrier_date=None,
            order_delivered_customer_date=None,
            order_estimated_delivery_date=None),
        Row(order_id="1", customer_id="c1", order_status="delivered",
            order_purchase_timestamp="2021-01-01 10:00:00",
            order_approved_at=None,
            order_delivered_carrier_date=None,
            order_delivered_customer_date=None,
            order_estimated_delivery_date=None),
    ]
    df = spark.createDataFrame(data)
    cleaned = clean_orders(df)
    assert cleaned.count() == 1


# ─────────────────────────────────────────────
# TEST 3 — clean_orders : filtre les nulls sur order_id
# ─────────────────────────────────────────────

def test_clean_orders_filters_null_order_id(spark):
    data = [
        Row(order_id=None, customer_id="c1", order_status="delivered",
            order_purchase_timestamp=None, order_approved_at=None,
            order_delivered_carrier_date=None,
            order_delivered_customer_date=None,
            order_estimated_delivery_date=None),
        Row(order_id="2", customer_id="c2", order_status="delivered",
            order_purchase_timestamp=None, order_approved_at=None,
            order_delivered_carrier_date=None,
            order_delivered_customer_date=None,
            order_estimated_delivery_date=None),
    ]
    df = spark.createDataFrame(data)
    cleaned = clean_orders(df)
    assert cleaned.count() == 1


# ─────────────────────────────────────────────
# TEST 4 — clean_order_items : cast prix en double
# ─────────────────────────────────────────────

def test_clean_order_items_price_cast(spark):
    data = [
        Row(order_id="1", product_id="p1", seller_id="s1",
            price="29.99", freight_value="5.00",
            shipping_limit_date=None, order_item_id=1)
    ]
    df = spark.createDataFrame(data)
    cleaned = clean_order_items(df)
    assert dict(cleaned.dtypes)["price"] == "double"
    assert dict(clean_order_items(df).dtypes)["freight_value"] == "double"


# ─────────────────────────────────────────────
# TEST 5 — calculate_total_revenue
# ─────────────────────────────────────────────

def test_calculate_total_revenue(spark):
    data = [
        Row(order_id="1", product_id="p1", seller_id="s1",
            price=100.0, freight_value=10.0,
            shipping_limit_date=None, order_item_id=1),
        Row(order_id="1", product_id="p2", seller_id="s1",
            price=50.0, freight_value=5.0,
            shipping_limit_date=None, order_item_id=2),
        Row(order_id="2", product_id="p1", seller_id="s2",
            price=200.0, freight_value=15.0,
            shipping_limit_date=None, order_item_id=1),
    ]
    df = spark.createDataFrame(data)
    total = calculate_total_revenue(df)
    assert total == 350.0


# ─────────────────────────────────────────────
# TEST 6 — calculate_avg_basket
# ─────────────────────────────────────────────

def test_calculate_avg_basket(spark):
    data = [
        Row(order_id="1", product_id="p1", seller_id="s1",
            price=100.0, freight_value=10.0,
            shipping_limit_date=None, order_item_id=1),
        Row(order_id="2", product_id="p2", seller_id="s2",
            price=200.0, freight_value=15.0,
            shipping_limit_date=None, order_item_id=1),
    ]
    df = spark.createDataFrame(data)
    avg = calculate_avg_basket(df)
    assert avg == 150.0


# ─────────────────────────────────────────────
# TEST 7 — calculate_late_rate
# ─────────────────────────────────────────────

def test_calculate_late_rate(spark):
    data = [
        Row(order_id="1",
            order_delivered_customer_date="2021-01-10 00:00:00",
            order_estimated_delivery_date="2021-01-08 00:00:00",
            order_purchase_timestamp="2021-01-01 00:00:00",
            order_status="delivered", customer_id="c1",
            order_approved_at=None, order_delivered_carrier_date=None),
        Row(order_id="2",
            order_delivered_customer_date="2021-01-07 00:00:00",
            order_estimated_delivery_date="2021-01-08 00:00:00",
            order_purchase_timestamp="2021-01-01 00:00:00",
            order_status="delivered", customer_id="c2",
            order_approved_at=None, order_delivered_carrier_date=None),
    ]
    df = spark.createDataFrame(data)
    cleaned = clean_orders(df)
    rate = calculate_late_rate(cleaned)
    assert rate == 50.0


# ─────────────────────────────────────────────
# TEST 8 — calculate_avg_review
# ─────────────────────────────────────────────

def test_calculate_avg_review(spark):
    data = [
        Row(order_id="1", review_id="r1", review_score=5,
            review_comment_title="", review_comment_message=""),
        Row(order_id="2", review_id="r2", review_score=3,
            review_comment_title="", review_comment_message=""),
    ]
    df = spark.createDataFrame(data)
    avg = calculate_avg_review(df)
    assert avg == 4.0


# ─────────────────────────────────────────────
# TEST 9 — check_duplicates ne plante pas
# ─────────────────────────────────────────────

def test_check_duplicates_runs(spark, capsys):
    data = [
        Row(order_id="1", customer_id="c1"),
        Row(order_id="1", customer_id="c1"),
        Row(order_id="2", customer_id="c2"),
    ]
    df = spark.createDataFrame(data)
    check_duplicates(df, "order_id", "orders")
    captured = capsys.readouterr()
    assert "doublon" in captured.out.lower()


# ─────────────────────────────────────────────
# TEST 10 — monthly_revenue retourne un DataFrame
# ─────────────────────────────────────────────

def test_calculate_monthly_revenue_returns_dataframe(spark):
    from pyspark.sql import DataFrame
    items_data = [
        Row(order_id="1", product_id="p1", seller_id="s1",
            price=100.0, freight_value=10.0,
            shipping_limit_date=None, order_item_id=1),
    ]
    orders_data = [
        Row(order_id="1", customer_id="c1", order_status="delivered",
            order_purchase_timestamp="2021-01-15 10:00:00",
            order_approved_at=None, order_delivered_carrier_date=None,
            order_delivered_customer_date=None,
            order_estimated_delivery_date=None),
    ]
    items_df = spark.createDataFrame(items_data)
    orders_df = spark.createDataFrame(orders_data)
    orders_cleaned = clean_orders(orders_df)
    result = calculate_monthly_revenue(items_df, orders_cleaned)
    assert isinstance(result, DataFrame)
    assert result.count() == 1