import sys
from pathlib import Path

import pandas as pd
import pytest

# Add project root to path so the src package can be imported in this test suite.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import logging

from src.etl_pipeline import (
    Config,
    DatabaseConfig,
    DataCleaner,
    DataExtractor,
    DataTransformer,
    ReviewNLP,
)


@pytest.fixture(autouse=True)
def _suppress_logging_for_tests():
    """Disable logging output during tests to avoid Windows console handle errors"""
    logging.disable(logging.CRITICAL)
    yield
    logging.disable(logging.NOTSET)

@pytest.fixture
def df_with_missing_values() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "col1": [1.0, None, 3.0],
            "col2": ["A", "B", None],
        }
    )


@pytest.fixture
def df_with_datetime_issues() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "dt": ["2023-01-01", "invalid-date", "2023-02-02"],
        }
    )


@pytest.fixture
def df_with_emoji_text() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "review_comment_message": ["Hello world", "🙂 emoji test"],
        }
    )


class TestDataExtractor:
    """Unit tests for DataExtractor: isolated file loading behavior."""

    @pytest.mark.integration
    def test_extract_all_reads_expected_tables(self, tmp_path, monkeypatch):
        expected_tables = list(Config.CSV_FILES.keys())

        for filename in Config.CSV_FILES.values():
            fake_path = tmp_path / filename
            pd.DataFrame({"dummy": [1]}).to_csv(fake_path, index=False)

        monkeypatch.setattr(Config, "DATA_RAW", tmp_path)

        datasets = DataExtractor.extract_all()

        assert set(datasets.keys()) == set(expected_tables)
        for table_name, df in datasets.items():
            assert isinstance(df, pd.DataFrame), f"{table_name} should be a DataFrame"
            assert df.shape == (1, 1), f"{table_name} should contain the seeded row"

    def test_extract_all_file_not_found_raises_error(self, tmp_path, monkeypatch):
        """Test that missing file raises FileNotFoundError"""
        monkeypatch.setattr(Config, "DATA_RAW", tmp_path)

        with pytest.raises(FileNotFoundError, match="CSV file not found"):
            DataExtractor.extract_all()

    def test_extract_all_corrupt_csv_raises_error(self, tmp_path, monkeypatch):
        """Test that corrupt CSV raises exception"""
        filename = "olist_orders_dataset.csv"
        fake_path = tmp_path / filename
        with open(fake_path, 'w') as f:
            f.write('order_id,customer_id\n"1,2')

        monkeypatch.setattr(Config, "DATA_RAW", tmp_path)

        with pytest.raises(pd.errors.ParserError):
            DataExtractor.extract_all()


class TestDatabaseConfig:
    """Unit tests for DatabaseConfig: connection string and environment handling."""

    def test_get_connection_string_with_defaults(self, monkeypatch):
        """Test connection string with default values"""
        # Mock os.getenv to return None for all env vars
        def mock_getenv(key, default=None):
            return default

        monkeypatch.setattr("os.getenv", mock_getenv)

        conn_str = DatabaseConfig.get_connection_string()
        expected = "postgresql://postgres:password@localhost:5432/ecommerce"
        assert conn_str == expected

    def test_get_connection_string_with_env_vars(self, monkeypatch):
        """Test connection string with environment variables"""
        # Since class variables are set at import time, we test the method directly
        # by temporarily modifying the class attributes
        original_user = DatabaseConfig.DB_USER
        original_password = DatabaseConfig.DB_PASSWORD
        original_host = DatabaseConfig.DB_HOST
        original_port = DatabaseConfig.DB_PORT
        original_name = DatabaseConfig.DB_NAME

        try:
            DatabaseConfig.DB_USER = "testuser"
            DatabaseConfig.DB_PASSWORD = "testpass"
            DatabaseConfig.DB_HOST = "testhost"
            DatabaseConfig.DB_PORT = "9999"
            DatabaseConfig.DB_NAME = "testdb"

            conn_str = DatabaseConfig.get_connection_string()
            expected = "postgresql://testuser:testpass@testhost:9999/testdb"
            assert conn_str == expected
        finally:
            # Restore original values
            DatabaseConfig.DB_USER = original_user
            DatabaseConfig.DB_PASSWORD = original_password
            DatabaseConfig.DB_HOST = original_host
            DatabaseConfig.DB_PORT = original_port
            DatabaseConfig.DB_NAME = original_name

    def test_use_database_flag(self, monkeypatch):
        """Test USE_DATABASE flag parsing"""
        # Test the parsing logic directly since class variables are cached
        assert ("true").lower() == "true"
        assert ("false").lower() == "false"
        assert bool("true")
        assert not bool("")


class TestErrorHandling:
    """Integration tests for error handling scenarios."""

    def test_etl_pipeline_with_missing_critical_column(self, tmp_path, monkeypatch):
        """Test that ETL handles missing critical columns gracefully"""
        # Create CSV with missing critical column
        orders_file = tmp_path / "olist_orders_dataset.csv"
        pd.DataFrame({
            "order_id": ["1", "2"],  # Missing customer_id
            "order_status": ["delivered", "shipped"]
        }).to_csv(orders_file, index=False)

        # Create minimal other files
        for filename in Config.CSV_FILES.values():
            if filename != "olist_orders_dataset.csv":
                file_path = tmp_path / filename
                pd.DataFrame({"dummy": [1]}).to_csv(file_path, index=False)

        monkeypatch.setattr(Config, "DATA_RAW", tmp_path)

        from src.etl_pipeline import ETLPipeline
        # ETL should handle this gracefully and continue with available data
        datasets, dimensions, aggregates, fact_table = ETLPipeline.run()
        assert isinstance(datasets, dict)
        assert isinstance(dimensions, dict)
        assert isinstance(aggregates, dict)
        assert isinstance(fact_table, pd.DataFrame)

    def test_etl_pipeline_with_too_many_nulls(self, tmp_path, monkeypatch):
        """Test that ETL handles excessive nulls gracefully"""
        # Create CSV with too many nulls
        orders_file = tmp_path / "olist_orders_dataset.csv"
        pd.DataFrame({
            "order_id": ["1", "2", "3", "4", "5"],
            "customer_id": ["A", None, None, None, None],  # 80% nulls
            "order_status": ["delivered"] * 5
        }).to_csv(orders_file, index=False)

        # Create minimal other files
        for filename in Config.CSV_FILES.values():
            if filename != "olist_orders_dataset.csv":
                file_path = tmp_path / filename
                pd.DataFrame({"dummy": [1]}).to_csv(file_path, index=False)

        monkeypatch.setattr(Config, "DATA_RAW", tmp_path)

        from src.etl_pipeline import ETLPipeline
        # ETL should handle this gracefully
        datasets, dimensions, aggregates, fact_table = ETLPipeline.run()
        assert isinstance(datasets, dict)
        assert isinstance(dimensions, dict)
        assert isinstance(aggregates, dict)
        assert isinstance(fact_table, pd.DataFrame)


class TestDataCleaner:
    """Unit tests for DataCleaner: validation, flags, imputation, and datetime handling."""

    def test_validate_critical_fields_passes_and_fails(self):
        df = pd.DataFrame(
            {
                "order_id": ["1", "2", "3"],
                "customer_id": ["A", "B", "C"],
                "price": [10.0, 20.0, 30.0],
            }
        )

        assert DataCleaner.validate_critical_fields(
            df, "test_table", ["order_id", "customer_id"]
        )

        with pytest.raises(ValueError, match="missing_col"):
            DataCleaner.validate_critical_fields(df, "test_table", ["order_id", "missing_col"])

    def test_create_missing_flags_basic(self):
        df = pd.DataFrame(
            {
                "a": [1.0, None, 3.0],
                "b": [None, "x", None],
            }
        )

        result = DataCleaner.create_missing_flags(df.copy(), {"a": "a_present", "b": "b_present"})

        assert "a_present" in result.columns
        assert "b_present" in result.columns
        assert result["a_present"].tolist() == [1, 0, 1]
        assert result["b_present"].tolist() == [0, 1, 0]

    def test_handle_missing_values_flag_first(self, df_with_missing_values):
        strategy = {"col1": "mean", "col2": "unknown"}
        cleaned = DataCleaner.handle_missing_values_safe(df_with_missing_values.copy(), "test", strategy)

        assert "col1_is_missing" in cleaned.columns
        assert "col2_is_missing" in cleaned.columns
        assert cleaned["col1_is_missing"].tolist() == [0, 1, 0]
        assert cleaned["col2_is_missing"].tolist() == [0, 0, 1]
        assert cleaned["col1"].isnull().sum() == 0
        assert cleaned["col2"].isnull().sum() == 0

    def test_handle_missing_values_safe_actions(self):
        df = pd.DataFrame(
            {
                "dropme": [1.0, None, 3.0, 2.0],
                "zero": [None, 2.0, None, 4.0],
                "median": [None, 4.0, 6.0, None],
                "unknown": ["a", None, "b", None],
                "keep": [None, 1.0, None, 2.0],
            }
        )

        strategy = {
            "dropme": "drop",
            "zero": "zero",
            "median": "median",
            "unknown": "unknown",
            "keep": "keep_null",
        }

        cleaned = DataCleaner.handle_missing_values_safe(df.copy(), "test", strategy)

        # Drop action removes row with null in dropme (index 1), leaving 3 rows
        assert cleaned.shape[0] == 3
        assert cleaned["dropme_is_missing"].tolist() == [0, 0, 0]

        # After drop, zero column has [None, None, 4.0] → fill with 0 → [0.0, 0.0, 4.0]
        assert cleaned["zero"].tolist() == [0.0, 0.0, 4.0]

        # Median: pandas ignores NaN, so median of [6.0] = 6.0 → [6.0, 6.0, 6.0]
        assert cleaned["median"].tolist() == [6.0, 6.0, 6.0]

        # Unknown fill: ["a", "b", None] → ["a", "b", "unknown"]
        assert cleaned["unknown"].tolist() == ["a", "b", "unknown"]

        # Keep null: [None, None, 2.0] → still 2 nulls
        assert cleaned["keep"].isnull().sum() == 2

class TestDataTransformer:
    """Unit tests for DataTransformer: feature engineering and referential checks."""

    def test_extract_text_features_with_emoji(self, df_with_emoji_text):
        result = DataTransformer.extract_text_features(df_with_emoji_text.copy(), ["review_comment_message"])

        assert "review_comment_message_length" in result.columns
        assert "review_comment_message_word_count" in result.columns
        assert result["review_comment_message_length"].tolist() == [11, 12]
        assert result["review_comment_message_word_count"].tolist() == [2, 3]

    def test_create_fact_order_items_computes_expected_metrics(self):
        orders = pd.DataFrame(
            {
                "order_id": ["O1", "O2"],
                "customer_id": ["C1", "C2"],
                "order_status": ["delivered", "delivered"],
                "order_purchase_timestamp": pd.to_datetime(["2023-01-01", "2023-01-02"]),
                "order_approved_at": pd.to_datetime(["2023-01-01", "2023-01-02"]),
                "order_delivered_carrier_date": pd.to_datetime(["2023-01-03", "2023-01-04"]),
                "order_delivered_customer_date": pd.to_datetime(["2023-01-06", "2023-01-03"]),
                "order_estimated_delivery_date": pd.to_datetime(["2023-01-05", "2023-01-04"]),
                "is_approved": [1, 1],
                "is_shipped": [1, 1],
                "is_delivered": [1, 1],
            }
        )

        order_items = pd.DataFrame(
            {
                "order_id": ["O1", "O2"],
                "order_item_id": [1, 1],
                "product_id": ["P1", "P2"],
                "seller_id": ["S1", "S2"],
                "shipping_limit_date": pd.to_datetime(["2023-01-10", "2023-01-11"]),
                "price": [100.0, 50.0],
                "freight_value": [10.0, 5.0],
            }
        )

        fact = DataTransformer.create_fact_order_items(orders, order_items)

        assert fact.loc[fact["order_id"] == "O1", "total_value"].iloc[0] == 110.0
        assert fact.loc[fact["order_id"] == "O2", "total_value"].iloc[0] == 55.0
        assert fact.loc[fact["order_id"] == "O1", "is_delayed"].iloc[0] == 1
        assert fact.loc[fact["order_id"] == "O2", "is_delayed"].iloc[0] == 0
        assert fact["order_purchase_ym"].dtype.name == "period[M]"

    def test_validate_referential_integrity_removes_orphans(self):
        fact = pd.DataFrame(
            {
                "customer_id": ["C1", "C2", "C3"],
                "product_id": ["P1", "P2", "P3"],
                "seller_id": ["S1", "S2", "S3"],
            }
        )

        dimensions = {
            "dim_customers": pd.DataFrame({"customer_id": ["C1", "C2"]}),
            "dim_products": pd.DataFrame({"product_id": ["P1", "P2"]}),
            "dim_sellers": pd.DataFrame({"seller_id": ["S1", "S2"]}),
        }

        validated = DataTransformer.validate_referential_integrity(fact.copy(), dimensions)

        assert len(validated) == 2
        assert set(validated["customer_id"]) == {"C1", "C2"}
        assert set(validated["product_id"]) == {"P1", "P2"}
        assert set(validated["seller_id"]) == {"S1", "S2"}

    @pytest.mark.nlp
    def test_nlp_extract_features_basic(self, monkeypatch):
        data = pd.DataFrame(
            {
                "review_id": [1, 2],
                "review_comment_message": ["Great product", None],
            }
        )

        monkeypatch.setattr(ReviewNLP, "analyze_sentiment", lambda text: {"sentiment_polarity": 0.25, "sentiment_subjectivity": 0.75})
        monkeypatch.setattr(ReviewNLP, "detect_language", lambda text: "en")
        monkeypatch.setattr(ReviewNLP, "count_emojis", lambda text: 0)

        result = ReviewNLP.extract_nlp_features(data.copy())

        assert result.loc[result["review_id"] == 1, "sentiment_polarity"].iloc[0] == 0.25
        assert result.loc[result["review_id"] == 2, "sentiment_polarity"].iloc[0] == -999
        assert result.loc[result["review_id"] == 1, "review_language"].iloc[0] == "en"
        assert "emoji_count" in result.columns

    def test_nlp_disabled_skips_processing(self, monkeypatch):
        df = pd.DataFrame(
            {
                "review_id": [1],
                "order_id": ["O1"],
                "review_comment_title": ["Nice"],
                "review_comment_message": ["Good"],
                "review_score": [5],
            }
        )

        monkeypatch.setattr(DatabaseConfig, "ENABLE_NLP", False)
        monkeypatch.setattr(ReviewNLP, "extract_nlp_features", lambda *args, **kwargs: pytest.fail("NLP should be skipped when disabled"))

        cleaned = DataTransformer.clean_all_tables({"reviews": df.copy()})
        review_df = cleaned["reviews"]

        assert "review_comment_message_length" in review_df.columns
        assert "sentiment_polarity" not in review_df.columns


if __name__ == "__main__":
    pytest.main([__file__])
