# streamlit_app/app.py
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text

from src.config import DB_URL

engine = create_engine(DB_URL)

@st.cache_data(ttl=5)
def load_recent_windows(limit: int = 500):
    query = text("""
        SELECT *
        FROM classified_windows
        ORDER BY timestamp_start_ns DESC
        LIMIT :limit;
    """)
    with engine.begin() as conn:
        df = pd.read_sql(query, conn, params={"limit": limit})
    return df

st.title("Exchange Behavior Fingerprinting – Live Monitor")

df = load_recent_windows()

if df.empty:
    st.info("No classified windows yet.")
else:
    df["timestamp_start"] = pd.to_datetime(df["timestamp_start_ns"])
    df = df.sort_values("timestamp_start")

    st.subheader("Latest classified windows")
    st.dataframe(
        df[["timestamp_start", "exchange_name", "stream_id", "predicted_exchange"]]
        .tail(50)
        .iloc[::-1]
    )

    st.subheader("Prediction timeline")
    df["pred_code"] = df["predicted_exchange"].astype("category").cat.codes
    st.line_chart(df.set_index("timestamp_start")["pred_code"])

    st.subheader("Class distribution (last N windows)")
    st.bar_chart(df["predicted_exchange"].value_counts())
