import streamlit as st
import pandas as pd
from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(page_title="Seller Insights", layout="wide")

@st.cache_data
def load_data():
    client = MongoClient(os.getenv("MONGO_URI"))
    data = list(client["hatla2ee"]["listings"].find({}, {"_id": 0}))
    return pd.DataFrame(data)

df = load_data()

st.title("📊 Seller Insights")

st.subheader("💰 Average Price by Model")
brand = st.selectbox("Select Brand", sorted(df["make"].dropna().unique().tolist()))
models = df[df["make"] == brand].groupby("model")["price"].mean().sort_values(ascending=False)
st.bar_chart(models)

col1, col2 = st.columns(2)

with col1:
    st.subheader("🔧 Average Mileage by Brand")
    avg_mileage = df.groupby("make")["mileage"].mean().sort_values(ascending=False).head(15)
    st.bar_chart(avg_mileage)

with col2:
    st.subheader("🚗 Price by Body Type")
    avg_price_body = df.groupby("body_type")["price"].mean().sort_values(ascending=False)
    st.bar_chart(avg_price_body)

st.subheader("📦 Market Supply by Brand")
st.bar_chart(df["make"].value_counts().head(15))