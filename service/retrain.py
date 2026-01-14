# retrain_model.py

import pandas as pd
import numpy as np
import joblib
from sklearn.neural_network import MLPRegressor
from sklearn.preprocessing import MinMaxScaler
from preprocessing import preprocess_workload

def retrain_mlp_model(csv_path, model_path="mlp_model.pkl", scaler_path="scaler.pkl"):
    try:
        # Load and preprocess dataset
        df = pd.read_csv(csv_path)
        df = preprocess_workload(df,training=True)

        # Split into features and target
        X = df.drop(columns=['duration_sum'])
        y = df['duration_sum']
        y_log = np.log1p(y)  # Apply log transform manually

        # Load existing scaler and transform features
        scaler = joblib.load(scaler_path)
        X_scaled = scaler.transform(X)

        # Load existing model
        mlp_model = joblib.load(model_path)

        # Continue training (requires warm_start=True during initial training)
        mlp_model.fit(X_scaled, y_log)

        # Save updated model
        joblib.dump(mlp_model, model_path)

        return True, "MLP model retrained and saved successfully."

    except Exception as e:
        return False, f"Error during MLP retraining: {e}"
