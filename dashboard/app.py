import streamlit as st
from pymongo import MongoClient
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(page_title="Hatla2ee Dashboard", layout="wide")

@st.cache_resource
def get_db():
    client = MongoClient(os.getenv("MONGO_URI"))
    return client["hatla2ee"]["listings"]

@st.cache_data
def load_data():
    collection = get_db()
    data = list(collection.find({}, {"_id": 0}))
    return pd.DataFrame(data)

df = load_data()

st.title("🚗 Hatla2ee Market Overview")

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Listings", len(df))
col2.metric("Avg Price (EGP)", f"{df['price'].mean():,.0f}")
col3.metric("Top Brand", df['make'].value_counts().idxmax())
col4.metric("Avg Value Score", f"{df['value_for_money'].mean():.1f}/10")

st.subheader("Price by Brand")
st.bar_chart(df.groupby("make")["price"].mean().sort_values(ascending=False))

col5, col6 = st.columns(2)
with col5:
    st.subheader("New vs Used")
    st.bar_chart(df["condition"].value_counts())
with col6:
    st.subheader("Fuel Types")
    st.bar_chart(df["fuel"].value_counts())
    
st.subheader("Listings by City")
st.bar_chart(df["city"].value_counts().head(10))