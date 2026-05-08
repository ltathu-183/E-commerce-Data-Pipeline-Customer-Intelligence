"""
MLflow Tracking for ETL Pipeline
=================================

Tracks ETL pipeline runs with:
- Parameters (config settings)
- Metrics (processing time, data quality scores)
- Artifacts (processed data samples, logs)

Usage:
    python src/mlflow_tracking.py
"""

import json
import time
from datetime import datetime
from pathlib import Path

import mlflow
import mlflow.sklearn
import pandas as pd

from src.etl_pipeline import Config, DatabaseConfig, ETLPipeline


def setup_mlflow():
    """Setup MLflow tracking"""
    # Use local MLflow server or set tracking URI
    mlflow.set_tracking_uri("file://" + str(Path(__file__).parent.parent / "mlruns"))

    # Set experiment
    mlflow.set_experiment("E-Commerce ETL Pipeline")

def log_etl_run():
    """Log complete ETL pipeline run"""

    with mlflow.start_run(run_name=f"ETL_{datetime.now().strftime('%Y%m%d_%H%M%S')}"):

        start_time = time.time()

        # Log parameters
        mlflow.log_param("db_host", DatabaseConfig.DB_HOST)
        mlflow.log_param("db_name", DatabaseConfig.DB_NAME)
        mlflow.log_param("use_database", DatabaseConfig.USE_DATABASE)
        mlflow.log_param("enable_nlp", DatabaseConfig.ENABLE_NLP)
        mlflow.log_param("data_raw_path", str(Config.DATA_RAW))
        mlflow.log_param("data_processed_path", str(Config.DATA_PROCESSED))

        try:
            # Run ETL pipeline
            datasets, dimensions, aggregates, fact_table = ETLPipeline.run()

            processing_time = time.time() - start_time

            # Log metrics
            mlflow.log_metric("processing_time_seconds", processing_time)
            mlflow.log_metric("total_datasets_loaded", len(datasets))
            mlflow.log_metric("fact_table_rows", len(fact_table) if fact_table is not None else 0)

            # Calculate data quality metrics
            if fact_table is not None:
                null_percentage = fact_table.isnull().sum().sum() / (len(fact_table) * len(fact_table.columns)) * 100
                mlflow.log_metric("fact_table_null_percentage", null_percentage)

                # Log revenue metrics
                if 'total_value' in fact_table.columns:
                    total_revenue = fact_table['total_value'].sum()
                    avg_order_value = fact_table['total_value'].mean()
                    mlflow.log_metric("total_revenue", total_revenue)
                    mlflow.log_metric("avg_order_value", avg_order_value)

            # Log artifacts
            # Save sample of processed data
            if fact_table is not None:
                sample_file = Config.DATA_PROCESSED / "fact_table_sample.csv"
                fact_table.head(1000).to_csv(sample_file, index=False)
                mlflow.log_artifact(str(sample_file))

            # Log dataset info
            dataset_info = {}
            for name, df in datasets.items():
                dataset_info[name] = {
                    "rows": len(df),
                    "columns": len(df.columns),
                    "null_percentage": (df.isnull().sum().sum() / (len(df) * len(df.columns)) * 100)
                }

            info_file = Config.DATA_PROCESSED / "dataset_info.json"
            with open(info_file, 'w') as f:
                json.dump(dataset_info, f, indent=2)
            mlflow.log_artifact(str(info_file))

            # Log success
            mlflow.log_param("pipeline_status", "success")

            print("✅ ETL pipeline completed successfully")
            print(".2f")
            print(f"📊 Logged to MLflow run: {mlflow.active_run().info.run_id}")

        except Exception as e:
            # Log failure
            mlflow.log_param("pipeline_status", "failed")
            mlflow.log_param("error_message", str(e))

            processing_time = time.time() - start_time
            mlflow.log_metric("processing_time_seconds", processing_time)

            print(f"❌ ETL pipeline failed: {str(e)}")
            raise

def log_data_quality_metrics():
    """Log additional data quality metrics"""

    with mlflow.start_run(run_name=f"Data_Quality_{datetime.now().strftime('%Y%m%d_%H%M%S')}"):

        try:
            # Load fact table
            fact_file = Config.DATA_PROCESSED / "dwh" / "fact_order_items.csv"
            if fact_file.exists():
                fact_df = pd.read_csv(fact_file)

                # Data quality checks
                total_rows = len(fact_df)
                mlflow.log_metric("total_rows", total_rows)

                # Null checks
                for col in fact_df.columns:
                    null_count = fact_df[col].isnull().sum()
                    null_pct = (null_count / total_rows) * 100
                    mlflow.log_metric(f"null_pct_{col}", null_pct)

                # Business rule checks
                if 'price' in fact_df.columns:
                    negative_prices = (fact_df['price'] < 0).sum()
                    mlflow.log_metric("negative_prices_count", negative_prices)

                if 'order_status' in fact_df.columns:
                    valid_statuses = ['delivered', 'shipped', 'canceled', 'processing', 'invoiced', 'approved']
                    invalid_statuses = (~fact_df['order_status'].isin(valid_statuses)).sum()
                    mlflow.log_metric("invalid_order_statuses", invalid_statuses)

                print("✅ Data quality metrics logged to MLflow")

        except Exception as e:
            print(f"❌ Failed to log data quality metrics: {str(e)}")
            mlflow.log_param("quality_check_status", "failed")
            mlflow.log_param("quality_error", str(e))

if __name__ == "__main__":
    setup_mlflow()

    # Log ETL run
    log_etl_run()

    # Log data quality
    log_data_quality_metrics()

    print("🎯 MLflow tracking complete!")
    print("📈 View results with: mlflow ui")

