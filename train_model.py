import os
import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, r2_score, classification_report
import joblib

def main():
    print("Loading environment variables...")
    load_dotenv()
    
    print("Connecting to MongoDB...")
    mongo_uri = os.getenv("MONGO_URI", "mongodb://root:example@localhost:27017/")
    client = MongoClient(mongo_uri)
    db = client["hatla2ee"]
    
    print("Fetching data from MongoDB...")
    cursor = db["listings"].find({}, {"_id": 0})
    df = pd.DataFrame(list(cursor))
    
    if df.empty:
        print("Error: No data found in the database. Please run the pipeline first.")
        return
        
    print(f"Loaded {len(df)} listings.")
    
    # 1. Clean and preprocess the data
    df = df.dropna(subset=['price'])
    
    df['mileage'] = df['mileage'].fillna(df['mileage'].median())
    df['year'] = df['year'].fillna(df['year'].median())
    
    cat_cols = ['make', 'model', 'body_type', 'transmission', 'fuel', 'condition']
    for col in cat_cols:
        if col in df.columns:
            df[col] = df[col].fillna('Unknown')
    
    features = ['year', 'mileage', 'make', 'model', 'body_type', 'transmission', 'fuel', 'condition']
    
    for col in features:
        if col not in df.columns:
            df[col] = 'Unknown'
            
    X = df[features]
    y = df['price']
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    # 2. Build the Pipeline
    print("Building model pipeline...")
    numeric_features = ['year', 'mileage']
    categorical_features = ['make', 'model', 'body_type', 'transmission', 'fuel', 'condition']
    
    preprocessor = ColumnTransformer(
        transformers=[
            ('num', StandardScaler(), numeric_features),
            ('cat', OneHotEncoder(handle_unknown='ignore'), categorical_features)
        ])
        
    pipeline = Pipeline(steps=[
        ('preprocessor', preprocessor),
        ('regressor', RandomForestRegressor(n_estimators=100, max_depth=None, min_samples_split=2, min_samples_leaf=1, random_state=42, n_jobs=-1))
    ])
    
    # 3. Hyperparameter Tuning with Grid Search
    # Best parameters: {'regressor__max_depth': None, 'regressor__min_samples_leaf': 1, 'regressor__min_samples_split': 2, 'regressor__n_estimators': 100}
    # print("Starting Grid Search for hyperparameter tuning...")
    # param_grid = {
    #     'regressor__n_estimators': [100, 200, 500],
    #     'regressor__max_depth': [None, 10, 20],
    #     'regressor__min_samples_split': [2, 5, 10],
    #     'regressor__min_samples_leaf': [1, 2, 4]
    # }
    
    # from sklearn.model_selection import GridSearchCV
    # grid_search = GridSearchCV(pipeline, param_grid, cv=5, scoring='r2', n_jobs=-1, verbose=1)
    
    # print("Training model (this will take longer due to Grid Search)...")
    pipeline.fit(X_train, y_train)
    
    best_model = pipeline
    # print(f"Best parameters: {grid_search.best_params_}")
    
    # 4. Evaluate
    y_pred = best_model.predict(X_test)
    mae = mean_absolute_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)
    
    print(f"Model Evaluation (Optimized):")
    print(f"Mean Absolute Error: {mae:,.0f} EGP")
    print(f"R-squared: {r2:.2f}")
    # 5. Save the model
    os.makedirs('models', exist_ok=True)
    model_path = os.path.join('models', 'price_predictor.pkl')
    print(f"Saving model to {model_path}...")
    joblib.dump(best_model, model_path)
    print("Done!")

if __name__ == "__main__":
    main()
