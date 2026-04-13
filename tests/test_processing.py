from pyspark.sql import SparkSession
from src.processing.transform import transform_data
from src.processing.aggregations import compute_aggregations
import os


os.environ["PYSPARK_PYTHON"] = "python"

def get_spark():
    spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
    return spark


def test_transform():
    spark = get_spark()

    sample_data = [{
        "id": "bitcoin",
        "symbol": "btc",
        "name": "Bitcoin",
        "current_price": 50000,
        "market_cap": 1000000,
        "total_volume": 50000,
        "high_24h": 51000,
        "low_24h": 49000,
        "price_change_percentage_24h": 2.5,
        "last_updated": "2024-01-01"
    }]

    df = spark.createDataFrame(sample_data)
    result_df = transform_data(df)

    assert result_df.count() == 1


def test_aggregations():
    spark = get_spark()

    sample_data = [{
        "id": "bitcoin",
        "symbol": "btc",
        "name": "Bitcoin",
        "current_price": 50000,
        "market_cap": 1000000,
        "total_volume": 50000,
        "high_24h": 51000,
        "low_24h": 49000,
        "price_change_percentage_24h": 2.5,
        "last_updated": "2024-01-01"
    }]

    df = spark.createDataFrame(sample_data)
    agg_df = compute_aggregations(df)

    assert agg_df.count() == 1