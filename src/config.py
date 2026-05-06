"""
Configuration Management
========================

SECURITY NOTE:
  - NEVER commit credentials to Git
  - Use environment variables or .env files
  - Keep config.py out of version control

USAGE:
  from config import Config
  cfg = Config()
  print(cfg.db_host)
"""

import os
from typing import Dict


class Config:
    """
    ✅ PRODUCTION: Centralized configuration management

    Features:
    - Environment variables for deployment flexibility
    - Validation thresholds for quality gates
    - Database credentials (from env)
    - Feature engineering parameters
    """

    # ========================================================================
    # DATABASE CONNECTION
    # ========================================================================

    # Get from environment variables (for production deployment)
    db_host: str = os.getenv("DB_HOST", "localhost")
    db_port: int = int(os.getenv("DB_PORT", "5432"))
    db_name: str = os.getenv("DB_NAME", "ecommerce_dw")
    db_user: str = os.getenv("DB_USER", "postgres")
    db_password: str = os.getenv("DB_PASSWORD", "postgres")
    db_schema: str = "ecommerce"

    @property
    def database_url(self) -> str:
        """PostgreSQL connection string"""
        return f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"

    # ========================================================================
    # DATA VALIDATION THRESHOLDS
    # ========================================================================

    # Critical field null threshold (fail if exceeded)
    max_null_percent_critical = 0.05  # 5% - FAIL_FAST
    max_null_percent_warning = 0.15  # 15% - WARNING

    # Duplicate record threshold
    max_duplicate_percent = 0.01  # 1% - FAIL_FAST

    # Foreign key violation threshold
    max_fk_violation_percent = 0.02  # 2% - FAIL_FAST

    # Negative price / invalid value threshold
    max_invalid_percent = 0.01  # 1% - FAIL_FAST

    # ========================================================================
    # FEATURE ENGINEERING PARAMETERS
    # ========================================================================

    # Delivery performance
    delivery_days_threshold = 30  # Days to mark as "delayed"
    estimated_delivery_days_threshold = 60  # Max reasonable estimate

    # Order size classification
    min_order_value = 0
    max_order_value = 10000  # Flag as unusual if exceeded

    # Time-based features
    look_back_days = 365  # For RFM (Recency = last N days)

    # ========================================================================
    # RFM SEGMENTATION PARAMETERS
    # ========================================================================

    rfm_segments = {
        "champions": {
            "recency_quantile": 0.75,
            "frequency_quantile": 0.75,
            "monetary_quantile": 0.75,
            "description": "Best customers (recent, frequent, high value)",
        },
        "loyal": {
            "recency_quantile": 0.50,
            "frequency_quantile": 0.50,
            "monetary_quantile": 0.50,
            "description": "Good customers (medium on all metrics)",
        },
        "at_risk": {
            "recency_quantile": 0.25,
            "frequency_quantile": 0.50,
            "monetary_quantile": 0.50,
            "description": "Purchases declining (old but good history)",
        },
        "need_attention": {
            "recency_quantile": 0.25,
            "frequency_quantile": 0.25,
            "monetary_quantile": 0.50,
            "description": "Low recent activity",
        },
        "lost": {
            "recency_quantile": 0.0,
            "frequency_quantile": 0.0,
            "monetary_quantile": 0.0,
            "description": "No recent purchases",
        },
    }

    # ========================================================================
    # PIPELINE EXECUTION PARAMETERS
    # ========================================================================

    # Data load mode
    load_mode = "replace"  # "replace" (full refresh) or "append" (incremental)

    # Batch size for large datasets
    batch_size = 5000

    # Data source tracking
    data_source_system = "olist"
    data_source_version = "1.0"

    # ========================================================================
    # LOGGING & MONITORING
    # ========================================================================

    log_level = os.getenv("LOG_LEVEL", "INFO")
    log_file = "logs/pipeline_execution.log"
    quality_report_file = "reports/data_quality_report.html"

    # ========================================================================
    # AGGREGATION REFRESH STRATEGY
    # ========================================================================

    # Full refresh vs incremental
    aggregation_refresh_mode = "full"  # "full" (daily) or "incremental" (hourly)
    aggregation_batch_size = 1000

    # ========================================================================
    # VALIDATION RULES BY TABLE
    # ========================================================================

    validation_rules = {
        "fact_order_items": {
            "critical_fields": ["order_id", "product_id", "customer_id", "price"],
            "max_null_percent": 0.05,
            "check_duplicates": True,
            "check_foreign_keys": True,
        },
        "dim_customers": {
            "critical_fields": ["customer_id", "customer_city", "customer_state"],
            "max_null_percent": 0.10,
            "check_duplicates": True,
            "check_foreign_keys": False,
        },
        "dim_products": {
            "critical_fields": ["product_id"],
            "max_null_percent": 0.20,  # Products have optional descriptions
            "check_duplicates": True,
            "check_foreign_keys": False,
        },
        "dim_sellers": {
            "critical_fields": ["seller_id"],
            "max_null_percent": 0.10,
            "check_duplicates": True,
            "check_foreign_keys": False,
        },
    }

    # ========================================================================
    # METHOD: Validate Configuration
    # ========================================================================

    @classmethod
    def validate(cls) -> Dict[str, bool]:
        """
        Validate that configuration is complete and reasonable

        Returns:
            Dict[str, bool]: Validation results
        """
        checks = {
            "db_host": bool(cls.db_host),
            "db_user": bool(cls.db_user),
            "db_name": bool(cls.db_name),
            "delivery_threshold_valid": 0 < cls.delivery_days_threshold < 100,
            "null_threshold_valid": 0 < cls.max_null_percent_critical < 1,
            "look_back_days_valid": cls.look_back_days > 0,
        }

        if not all(checks.values()):
            failed = [k for k, v in checks.items() if not v]
            raise ValueError(f"❌ Configuration validation failed: {', '.join(failed)}")

        print("✅ Configuration validated successfully")
        return checks

    # ========================================================================
    # METHOD: Print Configuration Summary
    # ========================================================================

    @classmethod
    def print_summary(cls):
        """Print configuration summary for debugging"""
        print("\n" + "=" * 80)
        print("🔧 CONFIGURATION SUMMARY")
        print("=" * 80)
        print(f"\n📊 DATABASE:")
        print(f"  Host: {cls.db_host}:{cls.db_port}")
        print(f"  Database: {cls.db_name}")
        print(f"  Schema: {cls.db_schema}")

        print(f"\n✅ VALIDATION THRESHOLDS:")
        print(f"  Critical Null %: {cls.max_null_percent_critical * 100}%")
        print(f"  Max Duplicates: {cls.max_duplicate_percent * 100}%")
        print(f"  Max FK Violations: {cls.max_fk_violation_percent * 100}%")

        print(f"\n🎯 FEATURE ENGINEERING:")
        print(f"  Delivery Threshold: {cls.delivery_days_threshold} days")
        print(f"  RFM Look-back: {cls.look_back_days} days")
        print(f"  Load Mode: {cls.load_mode}")

        print(f"\n📝 PIPELINE:")
        print(f"  Batch Size: {cls.batch_size}")
        print(f"  Data Source: {cls.data_source_system} v{cls.data_source_version}")
        print(f"  Aggregation Refresh: {cls.aggregation_refresh_mode}")
        print("\n" + "=" * 80 + "\n")


# ============================================================================
# QUICK VALIDATION
# ============================================================================

if __name__ == "__main__":
    try:
        Config.validate()
        Config.print_summary()
        print("\n✨ Configuration is ready for pipeline execution!")
    except Exception as e:
        print(f"\n❌ Configuration error: {e}")
        print("\n📋 FIX: Set environment variables before running pipeline:")
        print("   export DB_HOST='your_host'")
        print("   export DB_USER='your_user'")
        print("   export DB_PASSWORD='your_password'")
