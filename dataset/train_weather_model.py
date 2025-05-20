import pandas as pd
import numpy as np
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import matplotlib.pyplot as plt
import pickle
import os
import sys
from datetime import datetime
from tqdm import tqdm
import warnings
warnings.filterwarnings('ignore')

# Import the standard province names
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from provinces import VIETNAM_PROVINCES

# Load the mapping for standardizing province names
def load_province_mapping():
    city_to_province = {
        "Ha Noi": "Hanoi",
        "Bien Hoa": "Dong Nai",
        "Buon Me Thuot": "Dak Lak",
        "Cam Pha": "Quang Ninh",
        "Cam Ranh": "Khanh Hoa",
        "Chau Doc": "An Giang",
        "Da Lat": "Lam Dong",
        "Hong Gai": "Quang Ninh",
        "Hue": "Thua Thien Hue",
        "Long Xuyen": "An Giang",
        "My Tho": "Tien Giang",
        "Nha Trang": "Khanh Hoa",
        "Phan Rang": "Ninh Thuan",
        "Phan Thiet": "Binh Thuan",
        "Play Cu": "Gia Lai",
        "Qui Nhon": "Binh Dinh",
        "Rach Gia": "Kien Giang",
        "Tam Ky": "Quang Nam",
        "Tan An": "Long An",
        "Tuy Hoa": "Phu Yen",
        "Uong Bi": "Quang Ninh",
        "Viet Tri": "Phu Tho",
        "Vinh": "Nghe An",
        "Vung Tau": "Ba Ria-Vung Tau"
    }
    return city_to_province

# Load and preprocess the data
def load_and_preprocess_data(file_path):
    print("Loading and preprocessing data...")
    df = pd.read_csv(file_path)
    
    # Standardize province names
    city_to_province = load_province_mapping()
    df['province'] = df['province'].replace(city_to_province)
    
    # Convert date to datetime
    df['date'] = pd.to_datetime(df['date'])
    
    # Sort by province and date
    df = df.sort_values(['province', 'date'])
    
    # Extract date features
    df['year'] = df['date'].dt.year
    df['month'] = df['date'].dt.month
    df['day'] = df['date'].dt.day
    df['dayofweek'] = df['date'].dt.dayofweek
    df['dayofyear'] = df['date'].dt.dayofyear
    df['is_weekend'] = df['dayofweek'].apply(lambda x: 1 if x >= 5 else 0)
    df['quarter'] = df['date'].dt.quarter
    df['season'] = df['month'].apply(lambda x: 1 if x in [12, 1, 2] else 
                                   (2 if x in [3, 4, 5] else 
                                    (3 if x in [6, 7, 8] else 4)))
    
    # Convert wind direction to numerical
    wind_dir_mapping = {}
    unique_wind_dirs = df['wind_d'].unique()
    for i, direction in enumerate(unique_wind_dirs):
        wind_dir_mapping[direction] = i
    df['wind_d_num'] = df['wind_d'].map(wind_dir_mapping)
    
    # One-hot encode province
    df_encoded = pd.get_dummies(df, columns=['province'], prefix='province')
    
    return df_encoded

# Create lag features for each province
def create_lag_features(df, target_cols, lag_days=7):
    print("Creating lag features...")
    # Get unique provinces from column names that start with 'province_'
    province_cols = [col for col in df.columns if col.startswith('province_')]
    provinces = [col.replace('province_', '') for col in province_cols]
    
    # Create a copy of the dataframe to avoid SettingWithCopyWarning
    df_with_lags = df.copy()
    
    # For each province and target column, create lag features
    for province_col in tqdm(province_cols):
        province_mask = df_with_lags[province_col] == 1
        province_data = df_with_lags[province_mask].copy()
        
        if len(province_data) > 0:
            for target in target_cols:
                for lag in range(1, lag_days + 1):
                    lag_col_name = f"{target}_lag_{lag}"
                    province_data[lag_col_name] = province_data[target].shift(lag)
            
            # Update the original dataframe with the lag features
            df_with_lags.loc[province_mask] = province_data
    
    # Drop rows with NaN values (due to lag features)
    df_with_lags = df_with_lags.dropna()
    
    return df_with_lags

# Create rolling statistics features
def create_rolling_features(df, target_cols, windows=[3, 7, 14, 30]):
    print("Creating rolling features...")
    # Get unique provinces from column names that start with 'province_'
    province_cols = [col for col in df.columns if col.startswith('province_')]
    
    # Create a copy of the dataframe to avoid SettingWithCopyWarning
    df_with_rolling = df.copy()
    
    # For each province and target column, create rolling features
    for province_col in tqdm(province_cols):
        province_mask = df_with_rolling[province_col] == 1
        province_data = df_with_rolling[province_mask].copy()
        
        if len(province_data) > 0:
            for target in target_cols:
                for window in windows:
                    # Rolling mean
                    province_data[f"{target}_rolling_mean_{window}"] = province_data[target].rolling(window=window, min_periods=1).mean()
                    # Rolling std
                    province_data[f"{target}_rolling_std_{window}"] = province_data[target].rolling(window=window, min_periods=1).std()
                    # Rolling min
                    province_data[f"{target}_rolling_min_{window}"] = province_data[target].rolling(window=window, min_periods=1).min()
                    # Rolling max
                    province_data[f"{target}_rolling_max_{window}"] = province_data[target].rolling(window=window, min_periods=1).max()
            
            # Update the original dataframe with the rolling features
            df_with_rolling.loc[province_mask] = province_data
    
    return df_with_rolling

# Create interaction features
def create_interaction_features(df):
    print("Creating interaction features...")
    df_with_interactions = df.copy()
    
    # Temperature range (max - min)
    df_with_interactions['temp_range'] = df_with_interactions['max'] - df_with_interactions['min']
    
    # Humidity and temperature interactions
    df_with_interactions['humidi_max_temp'] = df_with_interactions['humidi'] * df_with_interactions['max']
    df_with_interactions['humidi_min_temp'] = df_with_interactions['humidi'] * df_with_interactions['min']
    
    # Wind and temperature interactions
    df_with_interactions['wind_max_temp'] = df_with_interactions['wind'] * df_with_interactions['max']
    df_with_interactions['wind_min_temp'] = df_with_interactions['wind'] * df_with_interactions['min']
    
    # Pressure and temperature interactions
    df_with_interactions['pressure_max_temp'] = df_with_interactions['pressure'] * df_with_interactions['max']
    df_with_interactions['pressure_min_temp'] = df_with_interactions['pressure'] * df_with_interactions['min']
    
    # Cloud and humidity interactions
    df_with_interactions['cloud_humidi'] = df_with_interactions['cloud'] * df_with_interactions['humidi']
    
    return df_with_interactions

# Create cyclical features for month, day, etc.
def create_cyclical_features(df):
    print("Creating cyclical features...")
    df_with_cyclical = df.copy()
    
    # Month cyclical encoding
    df_with_cyclical['month_sin'] = np.sin(2 * np.pi * df_with_cyclical['month'] / 12)
    df_with_cyclical['month_cos'] = np.cos(2 * np.pi * df_with_cyclical['month'] / 12)
    
    # Day of month cyclical encoding
    df_with_cyclical['day_sin'] = np.sin(2 * np.pi * df_with_cyclical['day'] / 31)
    df_with_cyclical['day_cos'] = np.cos(2 * np.pi * df_with_cyclical['day'] / 31)
    
    # Day of year cyclical encoding
    df_with_cyclical['dayofyear_sin'] = np.sin(2 * np.pi * df_with_cyclical['dayofyear'] / 365)
    df_with_cyclical['dayofyear_cos'] = np.cos(2 * np.pi * df_with_cyclical['dayofyear'] / 365)
    
    # Day of week cyclical encoding
    df_with_cyclical['dayofweek_sin'] = np.sin(2 * np.pi * df_with_cyclical['dayofweek'] / 7)
    df_with_cyclical['dayofweek_cos'] = np.cos(2 * np.pi * df_with_cyclical['dayofweek'] / 7)
    
    return df_with_cyclical

# Train XGBoost models for each target
def train_xgboost_models(df, target_cols, test_size=0.2, random_state=42):
    print("Training XGBoost models...")
    # Drop columns not needed for training
    drop_cols = ['date', 'wind_d']
    df_model = df.drop(columns=drop_cols)
    
    # Dictionary to store models
    models = {}
    feature_importances = {}
    metrics = {}
    
    for target in target_cols:
        print(f"\nTraining model for {target}...")
        
        # Define features and target
        X = df_model.drop(columns=target_cols)
        y = df_model[target]
        
        # Split data into train and test sets
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=test_size, random_state=random_state)
        
        # Define XGBoost parameters
        params = {
            'objective': 'reg:squarederror',
            'eval_metric': 'rmse',
            'learning_rate': 0.05,
            'max_depth': 6,
            'subsample': 0.8,
            'colsample_bytree': 0.8,
            'n_estimators': 1000,
            'early_stopping_rounds': 50,
            'random_state': random_state
        }
        
        # Train model
        model = xgb.XGBRegressor(**params)
        model.fit(
            X_train, y_train,
            eval_set=[(X_train, y_train), (X_test, y_test)],
            verbose=100
        )
        
        # Make predictions
        y_pred = model.predict(X_test)
        
        # Calculate metrics
        mse = mean_squared_error(y_test, y_pred)
        rmse = np.sqrt(mse)
        mae = mean_absolute_error(y_test, y_pred)
        r2 = r2_score(y_test, y_pred)
        
        print(f"MSE: {mse:.4f}")
        print(f"RMSE: {rmse:.4f}")
        print(f"MAE: {mae:.4f}")
        print(f"R²: {r2:.4f}")
        
        # Store model and metrics
        models[target] = model
        metrics[target] = {
            'mse': mse,
            'rmse': rmse,
            'mae': mae,
            'r2': r2
        }
        
        # Get feature importance
        feature_importances[target] = {
            'feature': X.columns.tolist(),
            'importance': model.feature_importances_
        }
        
        # Plot feature importance
        plt.figure(figsize=(12, 8))
        xgb.plot_importance(model, max_num_features=20, height=0.8)
        plt.title(f"Feature Importance for {target}")
        plt.tight_layout()
        plt.savefig(f"feature_importance_{target}.png")
    
    return models, feature_importances, metrics

# Save models and metrics
def save_models_and_metrics(models, feature_importances, metrics, output_dir='models'):
    print("Saving models and metrics...")
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Save models
    for target, model in models.items():
        model_path = os.path.join(output_dir, f"{target}_model.pkl")
        with open(model_path, 'wb') as f:
            pickle.dump(model, f)
        print(f"Model for {target} saved to {model_path}")
    
    # Save feature importances
    feature_imp_path = os.path.join(output_dir, "feature_importances.pkl")
    with open(feature_imp_path, 'wb') as f:
        pickle.dump(feature_importances, f)
    print(f"Feature importances saved to {feature_imp_path}")
    
    # Save metrics
    metrics_path = os.path.join(output_dir, "metrics.pkl")
    with open(metrics_path, 'wb') as f:
        pickle.dump(metrics, f)
    print(f"Metrics saved to {metrics_path}")

# Main function
def main():
    # Set file paths
    data_file = "d:\\DATN\\Weather_RAG_Chatbot\\dataset\\weather2009-2021.csv"
    output_dir = "d:\\DATN\\Weather_RAG_Chatbot\\dataset\\models"
    
    # Define target columns
    target_cols = ['max', 'min', 'rain', 'humidi', 'cloud']
    
    # Load and preprocess data
    df = load_and_preprocess_data(data_file)
    
    # Create lag features
    df_with_lags = create_lag_features(df, target_cols, lag_days=7)
    
    # Create rolling features
    df_with_rolling = create_rolling_features(df_with_lags, target_cols)
    
    # Create interaction features
    df_with_interactions = create_interaction_features(df_with_rolling)
    
    # Create cyclical features
    df_final = create_cyclical_features(df_with_interactions)
    
    # Train models
    models, feature_importances, metrics = train_xgboost_models(df_final, target_cols)
    
    # Save models and metrics
    save_models_and_metrics(models, feature_importances, metrics, output_dir)
    
    print("\nTraining completed successfully!")

if __name__ == "__main__":
    import sys
    main()
