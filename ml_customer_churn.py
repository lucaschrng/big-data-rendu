"""
Machine Learning: Customer Churn Prediction & CLV Forecasting

This standalone ML module demonstrates predictive analytics on e-commerce data.

Use Cases:
1. Churn Prediction: Identify customers at risk of leaving
2. CLV Prediction: Forecast customer lifetime value

Business Value:
- Proactive retention campaigns for at-risk customers
- Prioritize high-value customers
- Optimize marketing budget allocation
- Reduce customer acquisition costs
"""

import json
from datetime import datetime
from io import BytesIO

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingClassifier, RandomForestRegressor
from sklearn.metrics import (
    accuracy_score,
    classification_report,
    mean_absolute_error,
    mean_squared_error,
    r2_score,
    roc_auc_score,
)
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

from flows.config import BUCKET_SILVER, get_minio_client


def load_silver_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Load cleaned data from Silver layer.
    
    Returns:
        Tuple of (clients DataFrame, purchases DataFrame)
    """
    print("Loading data from Silver layer...")
    client = get_minio_client()
    
    # Load clients
    response = client.get_object(BUCKET_SILVER, "clients.csv")
    clients_data = response.read()
    response.close()
    response.release_conn()
    clients_df = pd.read_csv(BytesIO(clients_data))
    
    # Load purchases
    response = client.get_object(BUCKET_SILVER, "purchases.csv")
    purchases_data = response.read()
    response.close()
    response.release_conn()
    purchases_df = pd.read_csv(BytesIO(purchases_data))
    
    print(f"✓ Loaded {len(clients_df)} clients and {len(purchases_df)} purchases")
    return clients_df, purchases_df


def engineer_features(clients_df: pd.DataFrame, purchases_df: pd.DataFrame) -> pd.DataFrame:
    """
    Engineer features for ML models from raw Silver data.
    
    Creates RFM (Recency, Frequency, Monetary) features and behavioral metrics.
    
    Args:
        clients_df: Clients from Silver layer
        purchases_df: Purchases from Silver layer
        
    Returns:
        DataFrame with engineered features per client
    """
    print("\nEngineering features...")
    
    # Convert dates
    clients_df['subscription_date'] = pd.to_datetime(clients_df['subscription_date'])
    purchases_df['purchase_date'] = pd.to_datetime(purchases_df['purchase_date'])
    
    # Aggregate purchase metrics per client
    client_features = purchases_df.groupby('id_client').agg({
        'id_purchase': 'count',  # Frequency
        'total': ['sum', 'mean', 'min', 'max'],  # Monetary
        'quantity': 'sum',
        'purchase_date': ['min', 'max']  # For recency
    }).reset_index()
    
    # Flatten column names
    client_features.columns = [
        'id_client', 'total_purchases', 'total_spent', 'avg_order_value',
        'min_order_value', 'max_order_value', 'total_quantity',
        'first_purchase_date', 'last_purchase_date'
    ]
    
    # Merge with client data
    features = clients_df.merge(client_features, on='id_client', how='left')
    
    # Fill NaN for clients with no purchases
    features['total_purchases'] = features['total_purchases'].fillna(0)
    features['total_spent'] = features['total_spent'].fillna(0)
    features['avg_order_value'] = features['avg_order_value'].fillna(0)
    features['total_quantity'] = features['total_quantity'].fillna(0)
    
    # Calculate temporal features
    today = pd.Timestamp.now()
    
    # Customer lifetime (days since subscription)
    features['customer_lifetime_days'] = (today - features['subscription_date']).dt.days
    
    # Recency (days since last purchase)
    features['days_since_last_purchase'] = (today - features['last_purchase_date']).dt.days
    features['days_since_last_purchase'] = features['days_since_last_purchase'].fillna(999)  # No purchase = high recency
    
    # Purchase frequency (purchases per month)
    features['purchase_frequency_per_month'] = (
        features['total_purchases'] / 
        (features['customer_lifetime_days'] / 30).clip(lower=1)
    )
    
    # Define churn (inactive for > 90 days)
    features['is_churned'] = (features['days_since_last_purchase'] > 90).astype(int)
    
    print(f"✓ Engineered features for {len(features)} clients")
    print(f"  - Churn rate: {features['is_churned'].mean() * 100:.1f}%")
    print(f"  - Avg purchases per client: {features['total_purchases'].mean():.1f}")
    print(f"  - Avg CLV: ${features['total_spent'].mean():.2f}")
    
    return features


def train_churn_model(features: pd.DataFrame) -> dict:
    """
    Train churn prediction model.
    
    Predicts whether a customer will churn (become inactive).
    
    Args:
        features: DataFrame with engineered features
        
    Returns:
        Dictionary with model, metrics, and predictions
    """
    print("\n" + "="*60)
    print("TRAINING CHURN PREDICTION MODEL")
    print("="*60)
    
    # Select features
    feature_cols = [
        'total_purchases',
        'total_spent',
        'avg_order_value',
        'total_quantity',
        'customer_lifetime_days',
        'purchase_frequency_per_month',
        'days_since_last_purchase'
    ]
    
    X = features[feature_cols].fillna(0)
    y = features['is_churned']
    
    # Split data
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.25, random_state=42, stratify=y
    )
    
    print(f"\nDataset split:")
    print(f"  - Training: {len(X_train)} samples")
    print(f"  - Testing: {len(X_test)} samples")
    print(f"  - Churn rate (train): {y_train.mean() * 100:.1f}%")
    print(f"  - Churn rate (test): {y_test.mean() * 100:.1f}%")
    
    # Scale features
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)
    
    # Train model
    print("\nTraining Gradient Boosting Classifier...")
    model = GradientBoostingClassifier(
        n_estimators=100,
        learning_rate=0.1,
        max_depth=5,
        random_state=42,
        verbose=0
    )
    model.fit(X_train_scaled, y_train)
    
    # Predictions
    y_pred = model.predict(X_test_scaled)
    y_pred_proba = model.predict_proba(X_test_scaled)[:, 1]
    
    # Metrics
    accuracy = accuracy_score(y_test, y_pred)
    roc_auc = roc_auc_score(y_test, y_pred_proba)
    
    # Feature importance
    feature_importance = pd.DataFrame({
        'feature': feature_cols,
        'importance': model.feature_importances_
    }).sort_values('importance', ascending=False)
    
    # Classification report
    report = classification_report(y_test, y_pred, output_dict=True)
    
    print(f"\n{'='*60}")
    print("MODEL PERFORMANCE")
    print(f"{'='*60}")
    print(f"Accuracy: {accuracy:.2%}")
    print(f"ROC-AUC Score: {roc_auc:.4f}")
    print(f"\nPrecision (churned): {report['1']['precision']:.2%}")
    print(f"Recall (churned): {report['1']['recall']:.2%}")
    print(f"F1-Score (churned): {report['1']['f1-score']:.2%}")
    
    print(f"\n{'='*60}")
    print("TOP 5 IMPORTANT FEATURES")
    print(f"{'='*60}")
    for idx, row in feature_importance.head(5).iterrows():
        print(f"{row['feature']:.<40} {row['importance']:.4f}")
    
    return {
        'model': model,
        'scaler': scaler,
        'feature_cols': feature_cols,
        'accuracy': accuracy,
        'roc_auc': roc_auc,
        'feature_importance': feature_importance,
        'classification_report': report
    }


def train_clv_model(features: pd.DataFrame) -> dict:
    """
    Train Customer Lifetime Value prediction model.
    
    Predicts total spending (CLV) based on customer behavior.
    
    Args:
        features: DataFrame with engineered features
        
    Returns:
        Dictionary with model, metrics, and predictions
    """
    print("\n" + "="*60)
    print("TRAINING CLV PREDICTION MODEL")
    print("="*60)
    
    # Select features (exclude total_spent which is our target)
    feature_cols = [
        'total_purchases',
        'avg_order_value',
        'total_quantity',
        'customer_lifetime_days',
        'purchase_frequency_per_month',
        'days_since_last_purchase'
    ]
    
    # Only train on customers with purchases
    active_customers = features[features['total_purchases'] > 0].copy()
    
    X = active_customers[feature_cols].fillna(0)
    y = active_customers['total_spent']
    
    # Split data
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.25, random_state=42
    )
    
    print(f"\nDataset split:")
    print(f"  - Training: {len(X_train)} samples")
    print(f"  - Testing: {len(X_test)} samples")
    print(f"  - Avg CLV (train): ${y_train.mean():.2f}")
    print(f"  - Avg CLV (test): ${y_test.mean():.2f}")
    
    # Scale features
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)
    
    # Train model
    print("\nTraining Random Forest Regressor...")
    model = RandomForestRegressor(
        n_estimators=100,
        max_depth=10,
        min_samples_split=5,
        random_state=42,
        verbose=0
    )
    model.fit(X_train_scaled, y_train)
    
    # Predictions
    y_pred = model.predict(X_test_scaled)
    
    # Metrics
    mae = mean_absolute_error(y_test, y_pred)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    r2 = r2_score(y_test, y_pred)
    
    # Feature importance
    feature_importance = pd.DataFrame({
        'feature': feature_cols,
        'importance': model.feature_importances_
    }).sort_values('importance', ascending=False)
    
    print(f"\n{'='*60}")
    print("MODEL PERFORMANCE")
    print(f"{'='*60}")
    print(f"R² Score: {r2:.4f}")
    print(f"MAE (Mean Absolute Error): ${mae:.2f}")
    print(f"RMSE (Root Mean Squared Error): ${rmse:.2f}")
    print(f"\nInterpretation:")
    print(f"  - Model explains {r2*100:.1f}% of CLV variance")
    print(f"  - Average prediction error: ${mae:.2f}")
    
    print(f"\n{'='*60}")
    print("TOP 5 IMPORTANT FEATURES")
    print(f"{'='*60}")
    for idx, row in feature_importance.head(5).iterrows():
        print(f"{row['feature']:.<40} {row['importance']:.4f}")
    
    return {
        'model': model,
        'scaler': scaler,
        'feature_cols': feature_cols,
        'mae': mae,
        'rmse': rmse,
        'r2_score': r2,
        'feature_importance': feature_importance
    }


def make_predictions(features: pd.DataFrame, churn_model: dict, clv_model: dict) -> pd.DataFrame:
    """
    Make predictions for all customers.
    
    Args:
        features: DataFrame with engineered features
        churn_model: Trained churn model
        clv_model: Trained CLV model
        
    Returns:
        DataFrame with predictions and segments
    """
    print("\n" + "="*60)
    print("MAKING PREDICTIONS FOR ALL CUSTOMERS")
    print("="*60)
    
    # Churn predictions
    X_churn = features[churn_model['feature_cols']].fillna(0)
    X_churn_scaled = churn_model['scaler'].transform(X_churn)
    churn_proba = churn_model['model'].predict_proba(X_churn_scaled)[:, 1]
    
    # CLV predictions (only for customers with purchases)
    has_purchases = features['total_purchases'] > 0
    predicted_clv = np.zeros(len(features))
    
    if has_purchases.sum() > 0:
        X_clv = features.loc[has_purchases, clv_model['feature_cols']].fillna(0)
        X_clv_scaled = clv_model['scaler'].transform(X_clv)
        predicted_clv[has_purchases] = clv_model['model'].predict(X_clv_scaled)
    
    # Create results DataFrame
    results = features[['id_client', 'name', 'email', 'country', 
                        'total_purchases', 'total_spent', 'days_since_last_purchase']].copy()
    
    results['churn_probability'] = churn_proba
    results['churn_risk'] = pd.cut(
        churn_proba,
        bins=[0, 0.3, 0.7, 1.0],
        labels=['Low', 'Medium', 'High']
    )
    results['predicted_clv'] = predicted_clv
    
    # Customer segmentation
    results['segment'] = 'Standard'
    
    # High Value at Risk: High churn risk + High CLV
    high_clv_threshold = results['predicted_clv'].quantile(0.5)
    results.loc[
        (results['churn_probability'] > 0.7) & (results['predicted_clv'] > high_clv_threshold),
        'segment'
    ] = 'High Value at Risk ⚠️'
    
    # Champions: Low churn risk + High CLV
    results.loc[
        (results['churn_probability'] < 0.3) & (results['predicted_clv'] > high_clv_threshold),
        'segment'
    ] = 'Champions 🏆'
    
    # At Risk: High churn risk + Low CLV
    results.loc[
        (results['churn_probability'] > 0.7) & (results['predicted_clv'] <= high_clv_threshold),
        'segment'
    ] = 'At Risk'
    
    # Loyal: Low churn risk + Low CLV
    results.loc[
        (results['churn_probability'] < 0.3) & (results['predicted_clv'] <= high_clv_threshold),
        'segment'
    ] = 'Loyal'
    
    print(f"\n{'='*60}")
    print("CUSTOMER SEGMENTATION")
    print(f"{'='*60}")
    print(results['segment'].value_counts().to_string())
    
    print(f"\n{'='*60}")
    print("KEY INSIGHTS")
    print(f"{'='*60}")
    high_risk = (results['segment'] == 'High Value at Risk ⚠️').sum()
    champions = (results['segment'] == 'Champions 🏆').sum()
    avg_churn_prob = results['churn_probability'].mean()
    
    print(f"High Value at Risk: {high_risk} customers (URGENT ACTION NEEDED)")
    print(f"Champions: {champions} customers (maintain relationship)")
    print(f"Average churn probability: {avg_churn_prob:.1%}")
    print(f"Total predicted CLV: ${results['predicted_clv'].sum():,.2f}")
    
    return results


def save_results(predictions: pd.DataFrame, churn_model: dict, clv_model: dict):
    """
    Save predictions and models.
    
    Args:
        predictions: DataFrame with predictions
        churn_model: Trained churn model
        clv_model: Trained CLV model
    """
    print("\n" + "="*60)
    print("SAVING RESULTS")
    print("="*60)
    
    # Save predictions to CSV
    output_file = "ml_predictions.csv"
    predictions.to_csv(output_file, index=False)
    print(f"✓ Predictions saved to: {output_file}")
    
    # Save models
    joblib.dump(churn_model, "churn_model.joblib")
    print(f"✓ Churn model saved to: churn_model.joblib")
    
    joblib.dump(clv_model, "clv_model.joblib")
    print(f"✓ CLV model saved to: clv_model.joblib")
    
    # Save report
    report = {
        'generated_at': datetime.now().isoformat(),
        'churn_model': {
            'accuracy': float(churn_model['accuracy']),
            'roc_auc': float(churn_model['roc_auc']),
            'top_features': churn_model['feature_importance'].head(5).to_dict('records')
        },
        'clv_model': {
            'r2_score': float(clv_model['r2_score']),
            'mae': float(clv_model['mae']),
            'rmse': float(clv_model['rmse']),
            'top_features': clv_model['feature_importance'].head(5).to_dict('records')
        },
        'business_insights': {
            'total_customers': len(predictions),
            'high_value_at_risk': int((predictions['segment'] == 'High Value at Risk ⚠️').sum()),
            'champions': int((predictions['segment'] == 'Champions 🏆').sum()),
            'at_risk': int((predictions['segment'] == 'At Risk').sum()),
            'loyal': int((predictions['segment'] == 'Loyal').sum()),
            'avg_churn_probability': float(predictions['churn_probability'].mean()),
            'total_predicted_clv': float(predictions['predicted_clv'].sum())
        }
    }
    
    with open('ml_report.json', 'w') as f:
        json.dump(report, f, indent=2)
    print(f"✓ Report saved to: ml_report.json")
    
    # Print top 10 high-risk customers
    print(f"\n{'='*60}")
    print("TOP 10 HIGH VALUE AT RISK CUSTOMERS")
    print(f"{'='*60}")
    high_risk = predictions[predictions['segment'] == 'High Value at Risk ⚠️'].nlargest(10, 'predicted_clv')
    
    if len(high_risk) > 0:
        for idx, row in high_risk.iterrows():
            print(f"\n{row['name']} ({row['email']})")
            print(f"  Country: {row['country']}")
            print(f"  Churn Probability: {row['churn_probability']:.1%}")
            print(f"  Predicted CLV: ${row['predicted_clv']:.2f}")
            print(f"  Current Spent: ${row['total_spent']:.2f}")
    else:
        print("No high-risk customers identified.")


def main():
    """
    Main execution function.
    """
    print("\n" + "="*60)
    print("ML CUSTOMER ANALYTICS")
    print("Churn Prediction & CLV Forecasting")
    print("="*60)
    
    # Load data from Silver layer
    clients_df, purchases_df = load_silver_data()
    
    # Engineer features
    features = engineer_features(clients_df, purchases_df)
    
    # Train churn model
    churn_model = train_churn_model(features)
    
    # Train CLV model
    clv_model = train_clv_model(features)
    
    # Make predictions
    predictions = make_predictions(features, churn_model, clv_model)
    
    # Save results
    save_results(predictions, churn_model, clv_model)
    
    print("\n" + "="*60)
    print("✅ ML ANALYTICS COMPLETE")
    print("="*60)
    print("\nFiles created:")
    print("  - ml_predictions.csv (customer predictions)")
    print("  - churn_model.joblib (trained churn model)")
    print("  - clv_model.joblib (trained CLV model)")
    print("  - ml_report.json (performance metrics)")
    print("\n" + "="*60)


if __name__ == "__main__":
    main()
