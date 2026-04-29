import streamlit as st
import pandas as pd
import joblib
import os
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(page_title="Price Predictor", layout="wide")

@st.cache_resource
def load_model():
    model_path = os.path.join("models", "price_predictor.pkl")
    if os.path.exists(model_path):
        return joblib.load(model_path)
    return None

@st.cache_data
def load_data():
    mongo_uri = os.getenv("MONGO_URI", "mongodb://root:example@localhost:27017/")
    client = MongoClient(mongo_uri)
    data = list(client["hatla2ee"]["listings"].find({}, {"_id": 0}))
    if not data:
        return pd.DataFrame()
    return pd.DataFrame(data)

st.title("🤖 AI Price Predictor")
st.markdown("Enter your car's details below to get an AI-estimated listing price based on current market data.")

model = load_model()
df = load_data()

if model is None:
    st.error("Model not found! Please run the `uv run python train_model.py` script first to train and save the model.")
    st.stop()

if df.empty:
    st.error("No data found in MongoDB. Cannot load options.")
    st.stop()

# Layout for inputs
col1, col2 = st.columns(2)

with col1:
    makes = ["Select Make"] + sorted([m for m in df['make'].dropna().unique() if m])
    make = st.selectbox("Make", makes)
    
    models = ["Select Model"]
    if make != "Select Make":
        models = ["Select Model"] + sorted([m for m in df[df['make'] == make]['model'].dropna().unique() if m])
    model_name = st.selectbox("Model", models)
    
    year = st.number_input("Year", min_value=1970, max_value=2025, value=2015)
    
    condition_options = ["Select Condition"] + sorted([str(c) for c in df['condition'].dropna().unique() if c])
    condition = st.selectbox("Condition", condition_options)

with col2:
    mileage = st.number_input("Mileage (KM)", min_value=0, max_value=1000000, value=50000, step=5000)
    
    body_types = ["Select Body Type"] + sorted([str(b) for b in df['body_type'].dropna().unique() if b])
    body_type = st.selectbox("Body Type", body_types)
    
    transmissions = ["Select Transmission"] + sorted([str(t) for t in df['transmission'].dropna().unique() if t])
    transmission = st.selectbox("Transmission", transmissions)
    
    fuels = ["Select Fuel"] + sorted([str(f) for f in df['fuel'].dropna().unique() if f])
    fuel = st.selectbox("Fuel", fuels)

if st.button("Predict Price", type="primary", use_container_width=True):
    if make == "Select Make" or model_name == "Select Model":
        st.warning("Please select at least a Make and Model.")
    else:
        # Create input dataframe
        input_data = pd.DataFrame([{
            'year': year,
            'mileage': mileage,
            'make': make,
            'model': model_name,
            'body_type': body_type if body_type != "Select Body Type" else "Unknown",
            'transmission': transmission if transmission != "Select Transmission" else "Unknown",
            'fuel': fuel if fuel != "Select Fuel" else "Unknown",
            'condition': condition if condition != "Select Condition" else "Unknown"
        }])
        
        # Predict
        try:
            predicted_price = model.predict(input_data)[0]
            st.success(f"### Estimated Price: {predicted_price:,.0f} EGP")
            st.markdown(f"**This is an AI-estimated price based on current market data. The actual price may vary.**")
        except Exception as e:
            st.error(f"Error making prediction: {str(e)}")
