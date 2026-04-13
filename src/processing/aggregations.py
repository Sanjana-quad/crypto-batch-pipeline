from pyspark.sql.functions import avg, max, min

def compute_aggregations(df):
    """
    Compute daily statistics
    """

    agg_df = df.groupBy().agg(
        avg("current_price").alias("avg_price"),
        max("current_price").alias("max_price"),
        min("current_price").alias("min_price"),
        avg("price_change_percentage_24h").alias("avg_change_24h")
    )

    return agg_df