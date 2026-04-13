from pyspark.sql import SparkSession

def get_spark_session(app_name="CryptoPipeline"):
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")  # later we switch to cluster
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )

    return spark