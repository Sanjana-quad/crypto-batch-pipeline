import os

from src.processing.spark_session import get_spark_session
from src.processing.transform import transform_data
from src.processing.aggregations import compute_aggregations
from src.utils.logger import setup_logger

logger = setup_logger()

RAW_PATH = "hdfs://namenode:9000/data/raw"
PROCESSED_PATH = "hdfs://namenode:9000/data/processed"
LOCAL_PATH= "data/processed"
ANALYTICS_PATH = "hdfs://namenode:9000/data/analytics"


def run_pipeline():
    logger.info("Starting Spark processing pipeline")

    spark = get_spark_session()

    # Read raw JSON
    df = spark.read.json(RAW_PATH)
    logger.info("Raw data loaded")

    # Transform
    transformed_df = transform_data(df)
    logger.info("Data transformed")
    if transformed_df.rdd.isEmpty():
        raise ValueError("Transformed data is empty! Pipeline failed.")

    # Write processed data
    transformed_df.write.mode("overwrite").parquet(PROCESSED_PATH)
    logger.info("Processed data saved")
    transformed_df.write.mode("overwrite").parquet(LOCAL_PATH)
    logger.info("Local data saved")

    # Aggregations
    agg_df = compute_aggregations(transformed_df)

    agg_df.write.mode("overwrite").parquet(ANALYTICS_PATH)
    logger.info("Analytics data saved")

    spark.stop()

    logger.info("Pipeline completed successfully")


if __name__ == "__main__":
    run_pipeline()