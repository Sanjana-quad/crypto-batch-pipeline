import streamlit as st
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pandas as pd

st.set_page_config(page_title="Batch Dashboard", layout="wide")

st.title("📊 Crypto Batch Analytics")

try:
    pdf = pd.read_parquet("data/processed")

except Exception as e:
    st.error(f"Data not found: {e}")
    st.stop()

# ---- METRICS ----
st.subheader("🔢 Key Metrics")

col1, col2, col3 = st.columns(3)

col1.metric("Total Coins", len(pdf))
col2.metric("Avg Price", round(pdf["current_price"].mean(), 2))
col3.metric("Max Price", round(pdf["current_price"].max(), 2))

# ---- TOP COINS ----
st.subheader("🏆 Top Coins by Market Cap")

top_coins = pdf.sort_values(by="market_cap", ascending=False).head(10)

st.bar_chart(top_coins.set_index("name")["market_cap"])

# ---- PRICE DISTRIBUTION ----
st.subheader("📉 Price Distribution")

st.line_chart(pdf["current_price"])

# ---- RAW DATA ----
with st.expander("🔍 View Raw Data"):
    st.dataframe(pdf)