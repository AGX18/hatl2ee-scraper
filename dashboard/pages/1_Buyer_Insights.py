# MongoDB → DataFrame → filter by user input → display table/metrics
import streamlit as st
import pandas as pd
from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(page_title="Buyer Insights", layout="wide")

@st.cache_data
def load_data():
    client = MongoClient(os.getenv("MONGO_URI"))
    data = list(client["hatla2ee"]["listings"].find({}, {"_id": 0}))
    return pd.DataFrame(data)

df = load_data()

st.title("🔍 Buyer Insights")

st.subheader("🏆 Best Deals")

col1, col2, col3 = st.columns(3)

with col1:
    brands = ["All"] + sorted(df["make"].dropna().unique().tolist())
    brand = st.selectbox("Brand", brands)

with col2:
    condition = st.selectbox("Condition", ["All", "new", "used"])

with col3:
    max_price = int(df["price"].max())
    price_range = st.slider("Max Price (EGP)", 0, max_price, max_price)

filtered = df.copy()
if brand != "All":
    filtered = filtered[filtered["make"] == brand]
if condition != "All":
    filtered = filtered[filtered["condition"] == condition]
filtered = filtered[filtered["price"] <= price_range]

filtered = filtered.sort_values("value_for_money", ascending=False)
st.dataframe(filtered[["title", "make", "model", "year", "price", "mileage", "location", "condition", "value_for_money", "value_reasoning"]])

st.subheader("💰 Budget Finder")

budget = st.number_input("Enter your budget (EGP)", min_value=0, step=50000, value=500000)

affordable = df[df["price"] <= budget]

if len(affordable) == 0:
    st.warning("No cars found within this budget.")
else:
    best = affordable.loc[affordable["value_for_money"].idxmax()]

    col1, col2, col3 = st.columns(3)
    col1.metric("Cars Available", len(affordable))
    col2.metric("Best Value Car", f"{best['make']} {best['model']}")
    col3.metric("Best Value Score", f"{best['value_for_money']}/10")

    st.caption(f"💡 {best['value_reasoning']}")
    
    st.subheader("⚠️ Cars to Avoid")

worst = df.sort_values("value_for_money").head(10)
st.dataframe(worst[["title", "make", "model", "year", "price", "mileage", "location", "value_for_money", "value_reasoning"]])