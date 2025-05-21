import pandas as pd
import numpy as np
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import matplotlib.pyplot as plt
import pickle
import os
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Load and preprocess data
def load_and_preprocess_data(file_path):
    print("Loading and preprocessing data...")
    df = pd.read_csv(file_path)
    
    # Convert date to datetime
    df['date'] = pd.to_datetime(df['date'])
    print(f"Date range: {df['date'].min()} to {df['date'].max()}")
    
    # Add month information
    df['month'] = df['date'].dt.month
    
    # Add time features
    df['year'] = df['date'].dt.year
    df['day'] = df['date'].dt.day
    df['dayofweek'] = df['date'].dt.dayofweek
    df['dayofyear'] = df['date'].dt.dayofyear
    
    # Add cyclical features for month and day to capture seasonality
    df['month_sin'] = np.sin(2 * np.pi * df['month'] / 12)
    df['month_cos'] = np.cos(2 * np.pi * df['month'] / 12)
    df['day_sin'] = np.sin(2 * np.pi * df['day'] / 31)
    df['day_cos'] = np.cos(2 * np.pi * df['day'] / 31)
    
    # Handle categorical variables
    if 'wind_d' in df.columns:
        # Convert wind direction to numerical and drop the original column
        wind_dir_mapping = {}
        unique_wind_dirs = df['wind_d'].unique()
        for i, direction in enumerate(unique_wind_dirs):
            wind_dir_mapping[direction] = i
        df['wind_d_num'] = df['wind_d'].map(wind_dir_mapping)
        df = df.drop(columns=['wind_d'])
    
    # One-hot encode province
    df_encoded = pd.get_dummies(df, columns=['province'], prefix='province')
    
    return df_encoded

# Feature engineering functions
def add_features(df, target_cols):
    print("Adding features...")
    df_copy = df.copy()
    
    # Ensure data is sorted chronologically for time-based features
    df_copy = df_copy.sort_values('date')
    
    # 1. Add lag features (previous days' values)
    print("  Creating lag features...")
    for target in target_cols:
        for lag in range(1, 8):  # Previous 7 days
            df_copy[f'{target}_lag_{lag}'] = df_copy.groupby('date')[target].shift(lag)
    
    # 2. Add rolling window statistics
    print("  Creating rolling features...")
    for target in target_cols:
        # Rolling means (3-day and 7-day)
        for window in [3, 7]:
            df_copy[f'{target}_rolling_mean_{window}'] = df_copy.groupby('date')[target].transform(
                lambda x: x.rolling(window=window, min_periods=1).mean())
        
        # For temperature, add min/max in the rolling window
        if target in ['max', 'min']:
            for window in [3, 7]:
                df_copy[f'{target}_rolling_min_{window}'] = df_copy.groupby('date')[target].transform(
                    lambda x: x.rolling(window=window, min_periods=1).min())
                df_copy[f'{target}_rolling_max_{window}'] = df_copy.groupby('date')[target].transform(
                    lambda x: x.rolling(window=window, min_periods=1).max())
    
    # 3. Add interaction features
    print("  Creating interaction features...")
    # Temperature range (difference between max and min)
    df_copy['temp_range'] = df_copy['max'] - df_copy['min']
    
    # Month-specific temperature interactions
    for month in range(1, 13):
        df_copy[f'max_month_{month}'] = (df_copy['month'] == month) * df_copy['max']
        df_copy[f'min_month_{month}'] = (df_copy['month'] == month) * df_copy['min']
    
    # Drop rows with NaN values (due to lag features)
    df_copy = df_copy.dropna()
    
    return df_copy

# Train models for weather prediction (without seasonal splitting)
def train_models(df, target_cols, test_size=0.2, random_state=42):
    print("Training province-specific weather models...")
    
    # Dictionary to store models and metrics
    models = {}
    metrics = {}
    
    # For each target variable (max, min, rain, etc.)
    for target in target_cols:
        print(f"\nTraining model for {target}...")
        
        # Prepare features and target
        X = df.drop(columns=['date'] + target_cols)
        y = df[target]
        
        # Use time-based split for proper validation
        # This simulates real-world forecasting where we train on past data
        # and predict future data
        sorted_indices = np.argsort(df['date'].values)
        split_idx = int(len(sorted_indices) * 0.8)  # 80% train, 20% test
        
        train_indices = sorted_indices[:split_idx]
        test_indices = sorted_indices[split_idx:]
        
        X_train, X_test = X.iloc[train_indices], X.iloc[test_indices]
        y_train, y_test = y.iloc[train_indices], y.iloc[test_indices]
        
        print(f"Train size: {len(X_train)}, Test size: {len(X_test)}")
        
        # Configure model parameters
        params = {
            'objective': 'reg:squarederror',  # Regression task
            'eval_metric': 'rmse',           # Root Mean Squared Error
            'learning_rate': 0.05,           # Learning rate
            'max_depth': 6,                  # Maximum tree depth
            'subsample': 0.8,                # Subsample ratio of training instances
            'colsample_bytree': 0.8,         # Subsample ratio of columns when building trees
            'n_estimators': 500,             # Number of trees
            'random_state': random_state     # For reproducibility
        }
        
        # Adjust parameters for temperature models to prevent overfitting
        if target in ['max', 'min']:
            params['max_depth'] = 5  # Less complex model
            params['gamma'] = 0.1     # Regularization parameter
        
        # Create and train the model
        model = xgb.XGBRegressor(**params)
        model.fit(
            X_train, y_train,
            eval_set=[(X_train, y_train), (X_test, y_test)],
            verbose=50  # Print progress every 50 iterations
        )
        
        # Make predictions on test set
        y_pred = model.predict(X_test)
        
        # Calculate performance metrics
        mse = mean_squared_error(y_test, y_pred)
        rmse = np.sqrt(mse)
        mae = mean_absolute_error(y_test, y_pred)
        r2 = r2_score(y_test, y_pred)
        
        print(f"RMSE: {rmse:.4f}, MAE: {mae:.4f}, R²: {r2:.4f}")
        
        # For temperature models, verify prediction ranges
        if target in ['max', 'min']:
            pred_min, pred_max = y_pred.min(), y_pred.max()
            actual_min, actual_max = y_test.min(), y_test.max()
            print(f"Predicted range: {pred_min:.1f}-{pred_max:.1f}°C, Actual range: {actual_min:.1f}-{actual_max:.1f}°C")
        
        # Plot feature importance
        plt.figure(figsize=(10, 6))
        xgb.plot_importance(model, max_num_features=15)
        plt.title(f'Feature Importance for {target}')
        plt.tight_layout()
        plt.savefig(f'feature_importance_{target}.png')
        plt.close()
        
        # Store model and performance metrics
        models[target] = model
        metrics[target] = {
            'rmse': rmse,  # Root Mean Squared Error
            'mae': mae,    # Mean Absolute Error
            'r2': r2       # Coefficient of determination
        }
    
    return models, metrics

# Save trained models and metrics
def save_models(models, metrics, output_dir):
    print("Saving models and metrics...")
    os.makedirs(output_dir, exist_ok=True)
    
    # Save models for each target variable
    for target, model in models.items():
        model_path = os.path.join(output_dir, f"{target}_model.pkl")
        with open(model_path, 'wb') as f:
            pickle.dump(model, f)
        print(f"Saved model for {target} to {model_path}")
    
    # Save metrics for later analysis
    metrics_path = os.path.join(output_dir, "model_metrics.pkl")
    with open(metrics_path, 'wb') as f:
        pickle.dump(metrics, f)
    print(f"Saved metrics to {metrics_path}")

# Test model predictions on sample data
def test_predictions(models, df, target_cols):
    print("\nTesting model predictions...")
    results = []
    
    # Sample data from different provinces and time periods
    # Get unique provinces
    province_columns = [col for col in df.columns if col.startswith('province_')]
    provinces = [col.replace('province_', '') for col in province_columns]
    
    # Sample data from each province
    for province in provinces[:5]:  # Test with first 5 provinces
        province_data = df[df[f'province_{province}'] == 1]
        if len(province_data) == 0:
            continue
            
        # Get samples from different time periods
        test_samples = province_data.sample(min(3, len(province_data)))
        
        for _, sample in test_samples.iterrows():
            # Get month name
            month_num = sample['month']
            month_name = {1: 'January', 2: 'February', 3: 'March', 4: 'April', 5: 'May', 6: 'June', 
                         7: 'July', 8: 'August', 9: 'September', 10: 'October', 11: 'November', 12: 'December'}[month_num]
            
            result = {
                'date': sample['date'],
                'province': province,
                'month': month_name,
                'actual': {},
                'predicted': {}
            }
            
            # Get actual values
            for target in target_cols:
                result['actual'][target] = sample[target]
            
            # Make predictions
            for target in target_cols:
                model = models[target]
                
                # Get features needed by this model
                features = model.feature_names_in_
                X = pd.DataFrame([sample.drop(target_cols + ['date'])], 
                                columns=sample.drop(target_cols + ['date']).index)[features]
                
                # Predict and store result
                prediction = model.predict(X)[0]
                result['predicted'][target] = prediction
            
            results.append(result)
    
    # Print sample predictions
    print("\nSample Predictions:")
    for result in results[:10]:  # Show only first 10 results
        print(f"Date: {result['date']}, Province: {result['province']}, Month: {result['month']}")
        for target in target_cols:
            if target in result['actual'] and target in result['predicted']:
                actual = result['actual'][target]
                predicted = result['predicted'][target]
                error = abs(actual - predicted)
                print(f"  {target}: Actual={actual:.1f}, Predicted={predicted:.1f}, Error={error:.1f}")
        print()
    
    return results

# Main function
def main():
    # Configuration
    data_file = "d:\\DATN\\Weather_RAG_Chatbot\\dataset\\weather2009-2021.csv"
    output_dir = "d:\\DATN\\Weather_RAG_Chatbot\\dataset\\unified_models"
    target_cols = ['max', 'min', 'rain', 'humidi', 'cloud']  # Weather variables to predict
    
    # Step 1: Load and preprocess data
    df = load_and_preprocess_data(data_file)
    
    # Step 2: Feature engineering
    df = add_features(df, target_cols)
    
    # Step 3: Train unified models (no seasonal splitting)
    models, metrics = train_models(df, target_cols)
    
    # Step 4: Save models
    save_models(models, metrics, output_dir)
    
    # Step 5: Test predictions
    test_predictions(models, df, target_cols)
    
    print("\nTraining completed successfully!")

if __name__ == "__main__":
    main()
