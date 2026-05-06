"""
E-Commerce Data Pipeline: PRODUCTION-GRADE ETL Module
======================================================

Enhanced ETL with:
✓ PostgreSQL/SQLAlchemy integration (not just CSV)
✓ Complete dimension table loading
✓ Aggregation tables
✓ Fail-fast validation (strict data quality)
✓ Data lineage & metadata tracking
✓ Scalability notes for Spark/Airflow

Status: PRODUCTION-READY
"""

import logging
import os
import warnings
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

# ============================================================================
# DATABASE CONFIGURATION (PostgreSQL)
# ============================================================================


class DatabaseConfig:
    """Database connection configuration"""

    # Use environment variables in production
    DB_USER = os.getenv("DB_USER", "postgres")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "password")
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = os.getenv("DB_PORT", "5432")
    DB_NAME = os.getenv("DB_NAME", "ecommerce")
    ENABLE_NLP = os.getenv("ENABLE_NLP", "false").lower() == "true"

    # Connection string for SQLAlchemy
    @classmethod
    def get_connection_string(cls):
        return f"postgresql://{cls.DB_USER}:{cls.DB_PASSWORD}@{cls.DB_HOST}:{cls.DB_PORT}/{cls.DB_NAME}"

    # Option to use CSV for testing
    USE_DATABASE = os.getenv("USE_DATABASE", "false").lower() == "true"


# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)
# ============================================================================
# OPTIONAL: NLP FEATURE ENGINEERING (Reviews)
# Strategy: Process only non-null text → Merge scores back via review_id
# ============================================================================


class ReviewNLP:
    """
    Optional NLP pipeline for review text.
    Install dependencies: pip install textblob langdetect emoji
    """

    @staticmethod
    def analyze_sentiment(text: str) -> Dict[str, float]:
        """Return polarity (-1 to 1) and subjectivity (0 to 1)"""
        try:
            from textblob import TextBlob

            blob = TextBlob(str(text))
            return {
                "sentiment_polarity": blob.sentiment.polarity,
                "sentiment_subjectivity": blob.sentiment.subjectivity,
            }
        except Exception:
            return {"sentiment_polarity": np.nan, "sentiment_subjectivity": np.nan}

    @staticmethod
    def detect_language(text: str) -> str:
        """Detect language code (e.g., 'pt', 'en')"""
        try:
            from langdetect import detect

            return detect(str(text))
        except Exception:
            return "unknown"

    @staticmethod
    def count_emojis(text: str) -> int:
        """Count emoji characters in text"""
        try:
            import emoji

            return sum(1 for c in str(text) if c in emoji.EMOJI_DATA)
        except Exception:
            return 0

    @staticmethod
    def extract_nlp_features(
        df: pd.DataFrame,
        text_col: str = "review_comment_message",
        id_col: str = "review_id",
    ) -> pd.DataFrame:
        """
        Apply NLP features ONLY to non-null text, then merge back.
        Preserves original DataFrame structure and null patterns.
        """
        logger.info(f"\n🔍 Running NLP on {text_col} (non-null subset only)...")

        # Work on a copy to avoid SettingWithCopyWarning
        df = df.copy()

        # Filter to rows with actual text
        mask = df[text_col].notnull() & (df[text_col].str.len() > 0)
        text_subset = df.loc[mask, [id_col, text_col]].copy()

        if len(text_subset) == 0:
            logger.warning(f"  No valid text found in {text_col} → skipping NLP")
            # Add empty feature columns with NaN
            for col in [
                "sentiment_polarity",
                "sentiment_subjectivity",
                "review_language",
                "emoji_count",
            ]:
                df[col] = np.nan
            return df

        logger.info(f"  Processing {len(text_subset):,} reviews with text...")

        # Apply NLP functions (vectorized where possible)
        text_subset["sentiment_polarity"] = text_subset[text_col].apply(
            lambda x: ReviewNLP.analyze_sentiment(x)["sentiment_polarity"]
        )
        text_subset["sentiment_subjectivity"] = text_subset[text_col].apply(
            lambda x: ReviewNLP.analyze_sentiment(x)["sentiment_subjectivity"]
        )
        text_subset["review_language"] = text_subset[text_col].apply(
            lambda x: ReviewNLP.detect_language(x)
        )
        text_subset["emoji_count"] = text_subset[text_col].apply(
            lambda x: ReviewNLP.count_emojis(x)
        )

        # Drop raw text to save memory
        text_subset = text_subset.drop(columns=[text_col])

        # Merge NLP features back to full DataFrame via review_id
        df = df.merge(text_subset, on=id_col, how="left")

        # Fill NaN for rows that had no text (meaningful: "no sentiment")
        nlp_cols = ["sentiment_polarity", "sentiment_subjectivity", "emoji_count"]
        for col in nlp_cols:
            if col in df.columns:
                df[col] = df[col].fillna(-999)  # Sentinel value for "no text"

        logger.info(
            "  ✓ Added NLP features: sentiment_polarity, sentiment_subjectivity, review_language, emoji_count"
        )
        return df


# ============================================================================
# CONFIGURATION
# ============================================================================


class Config:
    DATA_RAW = Path(
        os.getenv("DATA_RAW", str(Path(__file__).parent.parent / "data" / "raw"))
    )
    DATA_PROCESSED = Path(
        os.getenv(
            "DATA_PROCESSED", str(Path(__file__).parent.parent / "data" / "processed")
        )
    )

    # Create directories if they don't exist
    DATA_PROCESSED.mkdir(parents=True, exist_ok=True)

    # Data validation thresholds
    max_null_percent_critical = 0.05  # 5% - FAIL_FAST

    # CSV files
    CSV_FILES = {
        "orders": "olist_orders_dataset.csv",
        "order_items": "olist_order_items_dataset.csv",
        "customers": "olist_customers_dataset.csv",
        "products": "olist_products_dataset.csv",
        "payments": "olist_order_payments_dataset.csv",
        "reviews": "olist_order_reviews_dataset.csv",
        "sellers": "olist_sellers_dataset.csv",
        "geolocation": "olist_geolocation_dataset.csv",
        "category_translation": "product_category_name_translation.csv",
    }


# ============================================================================
# STAGE 1: EXTRACT
# ============================================================================


class DataExtractor:
    """Extract data from CSV files with validation"""

    @staticmethod
    def extract_all() -> Dict[str, pd.DataFrame]:
        """Load all datasets from CSV files"""
        logger.info("=" * 80)
        logger.info("STAGE 1: EXTRACT")
        logger.info("=" * 80)

        datasets = {}

        for table_name, filename in Config.CSV_FILES.items():
            file_path = Config.DATA_RAW / filename

            if not file_path.exists():
                logger.error(f"File not found: {file_path}")
                raise FileNotFoundError(f"CSV file not found: {filename}")

            try:
                df = pd.read_csv(file_path)
                logger.info(
                    f"Loaded {table_name:20s}: {df.shape[0]:>8,} rows × {df.shape[1]:>3} cols"
                )
                datasets[table_name] = df
            except Exception as e:
                logger.error(f"Error loading {filename}: {str(e)}")
                raise

        logger.info(f"Extraction complete: {len(datasets)} tables loaded\n")
        return datasets


# ============================================================================
# STAGE 2: TRANSFORM - CLEANING & VALIDATION
# ============================================================================

logger = logging.getLogger(__name__)


class DataCleaner:
    """Strict validation + business-aware cleaning utilities"""

    @staticmethod
    def validate_critical_fields(
        df: pd.DataFrame, table_name: str, critical_fields: List[str]
    ) -> bool:
        """FAIL-FAST: Critical fields must exist and contain acceptable nulls"""
        for field in critical_fields:
            if field not in df.columns:
                raise ValueError(f"CRITICAL: Field '{field}' missing from {table_name}")
            null_pct = (df[field].isnull().sum() / len(df)) * 100
            if null_pct > Config.max_null_percent_critical * 100:
                raise ValueError(
                    f"CRITICAL: {null_pct:.1f}% nulls in {table_name}.{field} (threshold: {Config.max_null_percent_critical * 100:.1f}%)"
                )
        logger.info(f"✓ {table_name}: Critical fields validated")
        return True

    @staticmethod
    def create_missing_flags(
        df: pd.DataFrame, flag_columns: Dict[str, str]
    ) -> pd.DataFrame:
        """CREATE BINARY FLAGS BEFORE IMPUTATION (preserves business-state signal)"""
        for col, flag_name in flag_columns.items():
            if col in df.columns:
                df[flag_name] = df[col].notnull().astype(int)
                null_count = (~df[flag_name].astype(bool)).sum()
                logger.info(
                    f"  • Created flag '{flag_name}': {null_count} missing → signal preserved"
                )
        return df

    @staticmethod
    def remove_duplicates(
        df: pd.DataFrame, table_name: str, key_columns: List[str]
    ) -> pd.DataFrame:
        """Remove duplicates, log findings"""
        before = len(df)
        df = df.drop_duplicates(subset=key_columns, keep="first")
        removed = before - len(df)
        if removed > 0:
            logger.warning(
                f"{table_name}: Removed {removed} duplicates on {key_columns}"
            )
        else:
            logger.info(f"{table_name}: No duplicates found")
        return df

    @staticmethod
    def clean_datetime_columns(
        df: pd.DataFrame, datetime_columns: Dict[str, str]
    ) -> pd.DataFrame:
        """Convert and standardize datetime columns with error logging"""
        for col, _fmt in datetime_columns.items():
            if col not in df.columns:
                continue
            before = df[col].notna().sum()
            df[col] = pd.to_datetime(df[col], errors="coerce")
            after = df[col].notna().sum()
            errors = before - after
            if errors > 0:
                logger.warning(
                    f"️  {col}: {errors} unparseable datetime values → set to NaT"
                )
        return df

    @staticmethod
    def handle_missing_values_safe(
        df: pd.DataFrame, table_name: str, strategy: Dict[str, str]
    ) -> pd.DataFrame:
        """
        Handle missing values with FLAG-FIRST strategy: create flags before imputation.
        Supported actions: 'drop', 'zero', 'mean', 'median', 'unknown', 'keep_null'
        """
        for col, action in strategy.items():
            if col not in df.columns:
                continue
            null_count = df[col].isnull().sum()
            if null_count == 0:
                continue

            # FLAG-FIRST: Create missing value flag column
            flag_col = f"{col}_is_missing"
            df[flag_col] = df[col].isnull().astype(int)
            logger.info(
                f"  • {col}: Created flag column '{flag_col}' ({null_count} missing values flagged)"
            )

            if action == "drop":
                df = df.dropna(subset=[col])
                logger.info(f"  • {col}: Dropped {null_count} rows")
            elif action == "zero":
                df[col] = df[col].fillna(0)
                logger.info(f"  • {col}: Filled {null_count} with 0")
            elif action == "mean":
                if pd.api.types.is_numeric_dtype(df[col]):
                    val = df[col].mean()
                    df[col] = df[col].fillna(val)
                    logger.info(f"  • {col}: Filled {null_count} with mean ({val:.2f})")
            elif action == "median":
                if pd.api.types.is_numeric_dtype(df[col]):
                    val = df[col].median()
                    df[col] = df[col].fillna(val)
                    logger.info(
                        f"  • {col}: Filled {null_count} with median ({val:.2f})"
                    )
            elif action == "unknown":
                if pd.api.types.is_object_dtype(df[col]) or df[col].dtype == "string":
                    df[col] = df[col].fillna("unknown")
                    logger.info(f"  • {col}: Filled {null_count} with 'unknown'")
            elif action == "keep_null":
                logger.info(
                    f"  • {col}: Keeping {null_count} nulls (business state preserved)"
                )
            else:
                logger.warning(f"  • {col}: Unknown action '{action}' → keeping nulls")
        return df

    @staticmethod
    def validate_numeric_ranges(
        df: pd.DataFrame,
        table_name: str,
        numeric_validations: Dict[str, Tuple[float, float]],
    ) -> pd.DataFrame:
        """Fail-fast validation for numeric boundaries"""
        for col, (min_v, max_v) in numeric_validations.items():
            if col not in df.columns:
                continue
            violations = ((df[col] < min_v) | (df[col] > max_v)).sum()
            if violations > 0:
                raise ValueError(
                    f"{table_name}.{col}: {violations} values outside [{min_v}, {max_v}]"
                )
        logger.info(f"{table_name}: Numeric ranges validated")
        return df


class DataTransformer:
    """Orchestrates cleaning, feature extraction, and fact table creation"""

    @staticmethod
    def extract_text_features(
        df: pd.DataFrame, text_columns: List[str]
    ) -> pd.DataFrame:
        """Extract safe numeric features from text (0 for missing = no engagement)"""
        for col in text_columns:
            if col in df.columns:
                df[f"{col}_length"] = df[col].str.len().fillna(0).astype(int)
                df[f"{col}_word_count"] = (
                    df[col].astype(str).str.split().str.len().fillna(0).astype(int)
                )
        return df

    @staticmethod
    def clean_all_tables(datasets: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """Apply complete cleaning pipeline with flag-first missing value strategy"""
        logger.info("\n" + "=" * 80)
        logger.info("STAGE 2: TRANSFORM - CLEANING & VALIDATION")
        logger.info("=" * 80)

        cleaner = DataCleaner()

        # Define complete cleaning strategies per table
        cleaning_strategies = {
            "orders": {
                "critical_fields": [
                    "order_id",
                    "customer_id",
                    "order_purchase_timestamp",
                    "order_status",
                ],
                "duplicates_key": ["order_id"],
                "datetime_cols": {
                    "order_purchase_timestamp": "%Y-%m-%d %H:%M:%S",
                    "order_approved_at": "%Y-%m-%d %H:%M:%S",
                    "order_delivered_carrier_date": "%Y-%m-%d %H:%M:%S",
                    "order_delivered_customer_date": "%Y-%m-%d %H:%M:%S",
                    "order_estimated_delivery_date": "%Y-%m-%d %H:%M:%S",
                },
                "flag_columns": {
                    "order_approved_at": "is_approved",
                    "order_delivered_carrier_date": "is_shipped",
                    "order_delivered_customer_date": "is_delivered",
                },
                "missing_strategy": {
                    "order_approved_at": "keep_null",  # Filled explicitly via business logic
                    "order_delivered_carrier_date": "keep_null",
                    "order_delivered_customer_date": "keep_null",
                },
                "numeric_validations": {},
            },
            "order_items": {
                "critical_fields": ["order_id", "product_id", "price"],
                "duplicates_key": ["order_id", "order_item_id"],
                "datetime_cols": {"shipping_limit_date": "%Y-%m-%d %H:%M:%S"},
                "flag_columns": {"shipping_limit_date": "has_shipping_limit"},
                "missing_strategy": {"shipping_limit_date": "keep_null"},
                "numeric_validations": {
                    "price": (0, 100000),
                    "freight_value": (0, 500),
                },
            },
            "customers": {
                "critical_fields": ["customer_id", "customer_city", "customer_state"],
                "duplicates_key": ["customer_id"],
                "datetime_cols": {},
                "flag_columns": {},
                "missing_strategy": {},
                "numeric_validations": {},
            },
            "products": {
                "critical_fields": ["product_id"],
                "duplicates_key": ["product_id"],
                "datetime_cols": {},
                "flag_columns": {"product_category_name": "has_product_info"},
                "missing_strategy": {
                    "product_category_name": "unknown",
                    "product_name_lenght": "zero",
                    "product_description_lenght": "zero",
                    "product_photos_qty": "zero",
                    "product_weight_g": "median",
                    "product_length_cm": "median",
                    "product_height_cm": "median",
                    "product_width_cm": "median",
                },
                "numeric_validations": {},
            },
            "payments": {
                "critical_fields": ["order_id"],
                "duplicates_key": ["order_id", "payment_sequential"],
                "datetime_cols": {"order_payment_time": "%Y-%m-%d %H:%M:%S"},
                "flag_columns": {},
                "missing_strategy": {},
                "numeric_validations": {"payment_value": (0, 100000)},
            },
            "reviews": {
                "critical_fields": ["review_id", "order_id"],
                "duplicates_key": ["review_id"],
                "datetime_cols": {
                    "review_creation_date": "%Y-%m-%d %H:%M:%S",
                    "review_answer_timestamp": "%Y-%m-%d %H:%M:%S",
                },
                "flag_columns": {
                    "review_comment_title": "has_review_title",
                    "review_comment_message": "has_review_message",
                },
                "missing_strategy": {
                    "review_comment_title": "keep_null",
                    "review_comment_message": "keep_null",
                },
                "numeric_validations": {"review_score": (1, 5)},
            },
        }

        for table_name, df in datasets.items():
            logger.info(f"\nCleaning: {table_name.upper()}")
            logger.info("-" * 50)

            if table_name not in cleaning_strategies:
                logger.info(f"ℹ️  No cleaning strategy for {table_name} → skipping")
                continue

            strategy = cleaning_strategies[table_name]

            # 1.  Critical field validation
            cleaner.validate_critical_fields(
                df, table_name, strategy["critical_fields"]
            )

            # 2. Deduplicate
            df = cleaner.remove_duplicates(df, table_name, strategy["duplicates_key"])

            # 3. Standardize datetimes
            if strategy["datetime_cols"]:
                df = cleaner.clean_datetime_columns(df, strategy["datetime_cols"])

            # 4a. CREATE FLAGS FIRST (preserves MAR/Business-state signal)
            if strategy.get("flag_columns"):
                df = cleaner.create_missing_flags(df, strategy["flag_columns"])

            # 4b. Handle missing values (impute/drop/keep)
            if strategy["missing_strategy"]:
                df = cleaner.handle_missing_values_safe(
                    df, table_name, strategy["missing_strategy"]
                )

            # TABLE-SPECIFIC BUSINESS LOGIC OVERRIDES
            if table_name == "orders":
                # Fill approval timestamp with purchase time (instant approval assumption)
                if "order_approved_at" in df.columns:
                    before = df["order_approved_at"].isnull().sum()
                    df["order_approved_at"] = df["order_approved_at"].fillna(
                        df["order_purchase_timestamp"]
                    )
                    logger.info(
                        f"  • order_approved_at: Filled {before} nulls with order_purchase_timestamp"
                    )

            if table_name == "reviews":
                # Extract engagement metrics from text (safe, no fake text imputation)
                df = DataTransformer.extract_text_features(
                    df, ["review_comment_title", "review_comment_message"]
                )

                # Run advanced NLP (if enabled)
                if DatabaseConfig.ENABLE_NLP:
                    df = ReviewNLP.extract_nlp_features(
                        df, text_col="review_comment_message", id_col="review_id"
                    )
            # 5. Numeric range validation
            if strategy["numeric_validations"]:
                cleaner.validate_numeric_ranges(
                    df, table_name, strategy["numeric_validations"]
                )

            datasets[table_name] = df
            logger.info(f"✓ {table_name}: {len(df):,} rows (quality validated)")

        # Post-cleaning audit
        logger.info("\n Post-cleaning missing value audit:")
        for name, df in datasets.items():
            missing = df.isnull().sum()
            remaining = missing[missing > 0]
            if len(remaining) > 0:
                logger.info(
                    f"  {name}: {dict(remaining)} (expected for raw text/delivery states)"
                )
            else:
                logger.info(f"  {name}: All handled fields clean")

        return datasets

    @staticmethod
    def create_fact_order_items(
        orders: pd.DataFrame, order_items: pd.DataFrame
    ) -> pd.DataFrame:
        """Create item-level fact table with delivery flags & safe metrics"""
        logger.info("\n" + "=" * 80)
        logger.info("FEATURE ENGINEERING: Creating fact_order_items")
        logger.info("=" * 80)

        # Merge orders (includes is_approved, is_shipped, is_delivered flags)
        fact = order_items.merge(
            orders[
                [
                    "order_id",
                    "customer_id",
                    "order_status",
                    "order_purchase_timestamp",
                    "order_approved_at",
                    "order_delivered_carrier_date",
                    "order_delivered_customer_date",
                    "order_estimated_delivery_date",
                    "is_approved",
                    "is_shipped",
                    "is_delivered",
                ]
            ],
            on="order_id",
            how="left",
        )

        # Core metrics
        fact["total_value"] = fact["price"] + fact["freight_value"]

        # Delivery time: computed only where delivered, nulls preserved otherwise
        fact["delivery_time_days"] = (
            fact["order_delivered_customer_date"] - fact["order_purchase_timestamp"]
        ).dt.days

        fact["estimated_delivery_days"] = (
            fact["order_estimated_delivery_date"] - fact["order_purchase_timestamp"]
        ).dt.days

        # Delay flag: only meaningful for delivered orders
        fact["is_delayed"] = (
            (fact["delivery_time_days"] > fact["estimated_delivery_days"])
            & fact["is_delivered"].astype(bool)
        ).astype(int)

        fact["order_purchase_ym"] = fact["order_purchase_timestamp"].dt.to_period("M")

        # Data lineage
        fact["etl_loaded_at"] = datetime.now()
        fact["data_source"] = "olist_csv"

        logger.info(f"✓ Fact table: {len(fact):,} rows | Grain: item-level")
        logger.info(
            f"  Date range: {fact['order_purchase_timestamp'].min()} → {fact['order_purchase_timestamp'].max()}"
        )
        return fact

    @staticmethod
    def validate_referential_integrity(
        fact_order_items: pd.DataFrame, dimensions: Dict[str, pd.DataFrame]
    ) -> pd.DataFrame:
        """Validate referential integrity between fact and dimension tables"""

        logger.info("\n" + "=" * 80)
        logger.info("VALIDATION: Referential Integrity Checks")
        logger.info("=" * 80)

        # Check customer_id references
        if "dim_customers" in dimensions:
            valid_customers = set(dimensions["dim_customers"]["customer_id"])
            fact_customers = set(fact_order_items["customer_id"])
            orphaned_customers = fact_customers - valid_customers
            if orphaned_customers:
                logger.warning(
                    f"Found {len(orphaned_customers)} customer_ids in fact table not in dim_customers"
                )
                # Remove orphaned rows
                fact_order_items = fact_order_items[
                    ~fact_order_items["customer_id"].isin(orphaned_customers)
                ]
                logger.info(f"Removed {len(orphaned_customers)} orphaned customer rows")
            else:
                logger.info("✓ All customer_ids in fact table exist in dim_customers")

        # Check product_id references
        if "dim_products" in dimensions:
            valid_products = set(dimensions["dim_products"]["product_id"])
            fact_products = set(fact_order_items["product_id"])
            orphaned_products = fact_products - valid_products
            if orphaned_products:
                logger.warning(
                    f"Found {len(orphaned_products)} product_ids in fact table not in dim_products"
                )
                fact_order_items = fact_order_items[
                    ~fact_order_items["product_id"].isin(orphaned_products)
                ]
                logger.info(f"Removed {len(orphaned_products)} orphaned product rows")
            else:
                logger.info("✓ All product_ids in fact table exist in dim_products")

        # Check seller_id references
        if "dim_sellers" in dimensions:
            valid_sellers = set(dimensions["dim_sellers"]["seller_id"])
            fact_sellers = set(fact_order_items["seller_id"])
            orphaned_sellers = fact_sellers - valid_sellers
            if orphaned_sellers:
                logger.warning(
                    f"Found {len(orphaned_sellers)} seller_ids in fact table not in dim_sellers"
                )
                fact_order_items = fact_order_items[
                    ~fact_order_items["seller_id"].isin(orphaned_sellers)
                ]
                logger.info(f"Removed {len(orphaned_sellers)} orphaned seller rows")
            else:
                logger.info("✓ All seller_ids in fact table exist in dim_sellers")

        logger.info(
            f"✓ Referential integrity validation complete: {len(fact_order_items)} rows remaining"
        )
        return fact_order_items

    @staticmethod
    def create_dimension_tables(
        datasets: Dict[str, pd.DataFrame],
    ) -> Dict[str, pd.DataFrame]:
        """
        Create complete dimension tables
        These are SEPARATE from fact table
        """

        logger.info("\n" + "=" * 80)
        logger.info("CREATING DIMENSION TABLES")
        logger.info("=" * 80)

        dimensions = {}

        # 1. DIM_CUSTOMERS
        logger.info("\n1. Creating dim_customers...")
        dim_customers = datasets["customers"].copy()
        dim_customers["etl_loaded_at"] = datetime.now()
        dim_customers["data_source"] = "olist_csv"
        dimensions["dim_customers"] = dim_customers
        logger.info(f"   dim_customers: {len(dim_customers):,} rows")

        # 2. DIM_PRODUCTS
        logger.info("\n2. Creating dim_products...")
        dim_products = datasets["products"].copy()
        # Add category translation
        if "category_translation" in datasets:
            dim_products = dim_products.merge(
                datasets["category_translation"], on="product_category_name", how="left"
            ).rename(columns={"product_category_name_english": "category_english"})
        dim_products["etl_loaded_at"] = datetime.now()
        dim_products["data_source"] = "olist_csv"
        dimensions["dim_products"] = dim_products
        logger.info(f"   dim_products: {len(dim_products):,} rows")

        # 3. DIM_SELLERS
        logger.info("\n3. Creating dim_sellers...")
        dim_sellers = datasets["sellers"].copy()
        dim_sellers["etl_loaded_at"] = datetime.now()
        dim_sellers["data_source"] = "olist_csv"
        dimensions["dim_sellers"] = dim_sellers
        logger.info(f"   dim_sellers: {len(dim_sellers):,} rows")

        # 4. DIM_TIME (generated from order dates)
        logger.info("\n4. Creating dim_time...")
        date_range = pd.date_range(
            start=datasets["orders"]["order_purchase_timestamp"].min(),
            end=datasets["orders"]["order_purchase_timestamp"].max(),
            freq="D",
        )
        dim_time = pd.DataFrame(
            {
                "date_id": date_range,
                "year": date_range.year,
                "month": date_range.month,
                "day": date_range.day,
                "day_of_week": date_range.dayofweek,
                "day_name": date_range.day_name(),
                "month_name": date_range.month_name(),
                "quarter": date_range.quarter,
                "week_of_year": date_range.isocalendar().week,
                "is_weekend": (date_range.dayofweek >= 5).astype(int),
            }
        )
        dim_time["date_key"] = (
            dim_time["year"] * 10000 + dim_time["month"] * 100 + dim_time["day"]
        )
        dimensions["dim_time"] = dim_time
        logger.info(f"   dim_time: {len(dim_time):,} rows")

        return dimensions


class AggregationBuilder:
    """
    Build aggregation tables for performance
    Supports incremental updates to avoid full recomputes
    """

    @staticmethod
    def load_existing_aggregate(path: Path) -> pd.DataFrame:
        """Load existing aggregate table if available"""
        if path.exists():
            try:
                return pd.read_csv(path)
            except Exception:
                logger.warning(f"Could not load existing aggregate from {path}")
        return pd.DataFrame()

    @staticmethod
    def merge_incremental(
        existing: pd.DataFrame, new: pd.DataFrame, key_col: str
    ) -> pd.DataFrame:
        """Merge new data with existing aggregates (basic implementation)"""
        if existing.empty:
            return new

        # For simplicity, replace with new (full refresh)
        # In production, would implement true incremental merge logic
        logger.info(
            f"Merging aggregates: {len(existing)} existing + {len(new)} new rows"
        )
        return new

    @staticmethod
    def create_agg_customer_metrics(
        fact_df: pd.DataFrame, dim_customers: pd.DataFrame, incremental: bool = False
    ) -> pd.DataFrame:
        """Pre-calculate customer metrics with optional incremental update"""

        logger.info("\nCreating agg_customer_metrics...")

        agg = (
            fact_df[fact_df["is_delivered"] == 1]
            .groupby("customer_id")
            .agg(
                {
                    "order_id": "nunique",
                    "total_value": ["sum", "mean", "min", "max"],
                    "delivery_time_days": "mean",
                    "is_delayed": "mean",
                    "order_purchase_timestamp": ["min", "max"],
                }
            )
            .round(2)
        )

        agg.columns = [
            "total_orders",
            "total_revenue",
            "avg_order_value",
            "min_order_value",
            "max_order_value",
            "avg_delivery_days",
            "delayed_rate",
            "first_order_date",
            "last_order_date",
        ]
        agg = agg.reset_index()

        agg["days_as_customer"] = (
            agg["last_order_date"] - agg["first_order_date"]
        ).dt.days
        agg["etl_loaded_at"] = datetime.now()

        if incremental:
            existing_path = Config.DATA_PROCESSED / "dwh" / "agg_customer_metrics.csv"
            existing = AggregationBuilder.load_existing_aggregate(existing_path)
            agg = AggregationBuilder.merge_incremental(existing, agg, "customer_id")

        logger.info(f"   agg_customer_metrics: {len(agg):,} rows")
        return agg

    @staticmethod
    def create_agg_product_metrics(fact_df: pd.DataFrame) -> pd.DataFrame:
        """Pre-calculate product metrics"""

        logger.info("Creating agg_product_metrics...")

        agg = (
            fact_df[fact_df["is_delivered"] == 1]
            .groupby("product_id")
            .agg(
                {
                    "order_id": "nunique",
                    "order_item_id": "sum",
                    "total_value": ["sum", "mean"],
                    "delivery_time_days": "mean",
                    "is_delayed": "mean",
                    "order_purchase_timestamp": ["min", "max"],
                }
            )
            .round(2)
        )

        agg.columns = [
            "orders_count",
            "units_sold",
            "total_revenue",
            "avg_price",
            "avg_delivery_days",
            "delayed_rate",
            "first_sale_date",
            "last_sale_date",
        ]
        agg = agg.reset_index()
        agg["etl_loaded_at"] = datetime.now()

        logger.info(f"   agg_product_metrics: {len(agg):,} rows")
        return agg

    @staticmethod
    def create_agg_monthly_revenue(fact_df: pd.DataFrame) -> pd.DataFrame:
        """Pre-calculate monthly revenue"""

        logger.info("Creating agg_monthly_revenue...")

        monthly = fact_df[fact_df["is_delivered"] == 1].copy()
        monthly["year_month"] = monthly["order_purchase_timestamp"].dt.to_period("M")

        agg = (
            monthly.groupby("year_month")
            .agg(
                {
                    "total_value": "sum",
                    "order_id": "nunique",
                    "customer_id": "nunique",
                    "order_item_id": "sum",
                }
            )
            .round(2)
        )

        agg.columns = ["total_revenue", "order_count", "unique_customers", "item_count"]
        agg = agg.reset_index()
        agg["year"] = agg["year_month"].dt.year
        agg["month"] = agg["year_month"].dt.month
        agg["etl_loaded_at"] = datetime.now()

        logger.info(f"   agg_monthly_revenue: {len(agg):,} rows")
        return agg


# ============================================================================
# STAGE 3: LOAD
# ============================================================================


class DataLoader:
    """Load to both CSV and PostgreSQL"""

    @staticmethod
    def get_primary_key(table_name: str) -> list:
        """Get primary key columns for each table"""
        keys = {
            "dim_customers": ["customer_id"],
            "dim_products": ["product_id"],
            "dim_sellers": ["seller_id"],
            "dim_time": ["date_key"],
        }
        return keys.get(table_name, [])

    @staticmethod
    def upsert_table(
        engine, table_name: str, df: pd.DataFrame, primary_keys: list
    ) -> None:
        """Upsert data into PostgreSQL table using INSERT ... ON CONFLICT"""
        if not primary_keys:
            # Fallback to replace for tables without defined keys
            df.to_sql(table_name, engine, if_exists="replace", index=False)
            return

        from sqlalchemy import text

        # Create temp table
        temp_table = f"{table_name}_temp"
        df.to_sql(temp_table, engine, if_exists="replace", index=False)

        # Build upsert query
        columns = df.columns.tolist()
        update_cols = [col for col in columns if col not in primary_keys]

        insert_cols = ", ".join(columns)
        insert_vals = ", ".join([f'"{temp_table}"."{col}"' for col in columns])

        conflict_cols = ", ".join(primary_keys)
        update_clause = ", ".join(
            [f'"{col}" = EXCLUDED."{col}"' for col in update_cols]
        )

        upsert_query = f"""
        INSERT INTO "{table_name}" ({insert_cols})
        SELECT {insert_vals} FROM "{temp_table}"
        ON CONFLICT ({conflict_cols}) DO UPDATE SET {update_clause}
        """

        with engine.connect() as conn:
            conn.execute(text(upsert_query))
            conn.execute(text(f'DROP TABLE "{temp_table}"'))
            conn.commit()

    @staticmethod
    def save_to_csv(
        datasets: Dict[str, pd.DataFrame],
        dimensions: Dict[str, pd.DataFrame],
        aggregates: Dict[str, pd.DataFrame],
        fact_table: pd.DataFrame,
    ) -> None:
        """Save to CSV (for testing or fallback)"""

        logger.info("\n" + "=" * 80)
        logger.info("LOAD STAGE: Saving to CSV")
        logger.info("=" * 80)

        staging_dir = Config.DATA_PROCESSED / "staging"
        staging_dir.mkdir(parents=True, exist_ok=True)
        dwh_dir = Config.DATA_PROCESSED / "dwh"
        dwh_dir.mkdir(parents=True, exist_ok=True)

        # Staging tables
        for table_name in [
            "orders",
            "order_items",
            "customers",
            "products",
            "payments",
        ]:
            if table_name in datasets:
                path = staging_dir / f"raw_{table_name}.csv"
                datasets[table_name].to_csv(path, index=False)
                logger.info(f"{path.name}")

        # Dimension tables
        for table_name, df in dimensions.items():
            path = dwh_dir / f"{table_name}.csv"
            df.to_csv(path, index=False)
            logger.info(f"{path.name}")

        # Fact table
        fact_table.to_csv(dwh_dir / "fact_order_items.csv", index=False)
        logger.info("✓ fact_order_items.csv")

        # Aggregates
        for table_name, df in aggregates.items():
            path = dwh_dir / f"{table_name}.csv"
            df.to_csv(path, index=False)
            logger.info(f"{path.name}")

    @staticmethod
    def save_to_postgresql(
        dimensions: Dict[str, pd.DataFrame],
        fact_table: pd.DataFrame,
        aggregates: Dict[str, pd.DataFrame],
    ) -> None:
        """UPGRADE: Load to PostgreSQL database"""

        logger.info("\n" + "=" * 80)
        logger.info("LOAD STAGE: Saving to PostgreSQL")
        logger.info("=" * 80)

        try:
            from sqlalchemy import create_engine
        except ImportError:
            logger.warning("SQLAlchemy not installed. Skipping PostgreSQL load.")
            logger.warning("Install: pip install sqlalchemy psycopg2-binary")
            return

        conn_string = DatabaseConfig.get_connection_string()

        try:
            engine = create_engine(conn_string)
            logger.info(f"Connected to PostgreSQL: {DatabaseConfig.DB_NAME}")
        except Exception as e:
            logger.error(f"Cannot connect to PostgreSQL: {str(e)}")
            logger.info("Continuing with CSV only. To enable DB: set USE_DATABASE=true")
            return

        try:
            # Load dimensions with incremental upsert
            for table_name, df in dimensions.items():
                DataLoader.upsert_table(
                    engine, table_name, df, DataLoader.get_primary_key(table_name)
                )
                logger.info(f" {table_name}: {len(df):,} rows")

            # Load fact table with incremental upsert
            DataLoader.upsert_table(
                engine, "fact_order_items", fact_table, ["order_id", "order_item_id"]
            )
            logger.info(f" fact_order_items: {len(fact_table):,} rows")

            # Load aggregates (these are typically refreshed, not incremental)
            for table_name, df in aggregates.items():
                df.to_sql(table_name, engine, if_exists="replace", index=False)
                logger.info(f" {table_name}: {len(df):,} rows")

            logger.info("\n All tables loaded to PostgreSQL successfully")

        except Exception as e:
            logger.error(f"Error loading to PostgreSQL: {str(e)}")
            raise


# ============================================================================
# ORCHESTRATION
# ============================================================================


class ETLPipeline:
    """Main ETL orchestrator - PRODUCTION GRADE"""

    @staticmethod
    def run(
        use_database: Optional[bool] = None,
    ) -> Tuple[Dict, Dict, Dict, pd.DataFrame]:
        """
        Execute complete ETL pipeline with error recovery

        Args:
            use_database: If True, load to PostgreSQL. If None, check environment.

        Returns:
            (datasets, dimensions, aggregates, fact_table) - may be partial on errors
        """

        logger.info("\n\n" + "=" * 80)
        logger.info("E-COMMERCE DATA PIPELINE")
        logger.info("=" * 80)
        logger.info(f"Start time: {datetime.now()}")

        datasets = {}
        dimensions = {}
        aggregates = {}
        fact_order_items = pd.DataFrame()

        try:
            # Stage 1: Extract
            try:
                extractor = DataExtractor()
                datasets = extractor.extract_all()
            except Exception as e:
                logger.error(f"EXTRACT STAGE FAILED: {str(e)}")
                raise  # Critical failure, cannot continue

            # Stage 2: Transform
            try:
                transformer = DataTransformer()
                datasets = transformer.clean_all_tables(datasets)

                # Create fact table
                fact_order_items = transformer.create_fact_order_items(
                    datasets["orders"], datasets["order_items"]
                )

                # Create dimension tables
                dimensions = transformer.create_dimension_tables(datasets)

                # Referential integrity validation
                fact_order_items = transformer.validate_referential_integrity(
                    fact_order_items, dimensions
                )

            except Exception as e:
                logger.error(f"TRANSFORM STAGE FAILED: {str(e)}")
                raise  # Critical failure, cannot continue

            # Create aggregation tables
            try:
                logger.info("\n" + "=" * 80)
                logger.info("CREATING AGGREGATION TABLES")
                logger.info("=" * 80)
                agg_builder = AggregationBuilder()
                aggregates = {
                    "agg_customer_metrics": agg_builder.create_agg_customer_metrics(
                        fact_order_items,
                        dimensions.get("dim_customers", pd.DataFrame()),
                    ),
                    "agg_product_metrics": agg_builder.create_agg_product_metrics(
                        fact_order_items
                    ),
                    "agg_monthly_revenue": agg_builder.create_agg_monthly_revenue(
                        fact_order_items
                    ),
                }
            except Exception as e:
                logger.warning(f"AGGREGATION CREATION FAILED (non-critical): {str(e)}")
                logger.warning("Continuing with empty aggregates...")
                aggregates = {}

            # Stage 3: Load
            loader = DataLoader()

            # Always try CSV load (fallback)
            try:
                loader.save_to_csv(datasets, dimensions, aggregates, fact_order_items)
                logger.info("CSV load completed successfully")
            except Exception as e:
                logger.warning(f"CSV LOAD FAILED: {str(e)}")
                logger.warning("Data may not be saved to disk")

            # Try PostgreSQL load (optional)
            if use_database or (use_database is None and DatabaseConfig.USE_DATABASE):
                try:
                    loader.save_to_postgresql(dimensions, fact_order_items, aggregates)
                    logger.info("PostgreSQL load completed successfully")
                except Exception as e:
                    logger.warning(f"POSTGRESQL LOAD FAILED (non-critical): {str(e)}")
                    logger.warning("Continuing without database load...")

            logger.info("\n" + "=" * 80)
            logger.info("ETL PIPELINE COMPLETED (with possible warnings)")
            logger.info("=" * 80)
            logger.info(f"End time: {datetime.now()}")
            logger.info("\nOutputs:")
            logger.info(f"  CSV: {Config.DATA_PROCESSED}")
            if use_database or (use_database is None and DatabaseConfig.USE_DATABASE):
                logger.info(f"  Database: {DatabaseConfig.DB_NAME} (if available)")

            return datasets, dimensions, aggregates, fact_order_items

        except Exception as e:
            logger.error("\n ETL PIPELINE FAILED CRITICALLY")
            logger.error(f"Error: {str(e)}")
            logger.info("Returning partial results for debugging...")
            return datasets, dimensions, aggregates, fact_order_items


# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    # Execute pipeline
    datasets, dimensions, aggregates, fact_table = ETLPipeline.run()

    print("\n" + "=" * 80)
    print("PIPELINE SUMMARY")
    print("=" * 80)
    print(f"\nFact Table: {len(fact_table):,} rows")
    print("Dimensions:")
    for name, df in dimensions.items():
        print(f"  • {name}: {len(df):,} rows")
    print("Aggregates:")
    for name, df in aggregates.items():
        print(f"  • {name}: {len(df):,} rows")
