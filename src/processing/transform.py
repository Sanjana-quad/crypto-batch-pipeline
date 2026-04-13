from pyspark.sql.functions import col

def transform_data(df):
    """
    Clean + select required fields
    """
    required_columns = ["id", "current_price"]

    missing = [c for c in required_columns if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    selected_df = df.select(
        col("id"),
        col("symbol"),
        col("name"),
        col("current_price"),
        col("market_cap"),
        col("total_volume"),
        col("high_24h"),
        col("low_24h"),
        col("price_change_percentage_24h"),
        col("last_updated")
    )

    # Handle nulls
    cleaned_df = selected_df.dropna(subset=["current_price"])

    return cleaned_df