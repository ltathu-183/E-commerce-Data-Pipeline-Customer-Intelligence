"""
Great Expectations Data Quality Tests for Olist E-commerce Data
================================================================

This module defines data quality expectations for the ETL pipeline.
"""

import great_expectations as ge
from great_expectations.core.expectation_configuration import ExpectationConfiguration

def create_expectations_suite():
    """Create expectations suite for Olist data"""

    # Create context
    context = ge.get_context()

    # Create suite
    suite = context.add_expectation_suite("olist_data_quality")

    # Expectations for customers
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_to_exist",
            kwargs={"column": "customer_id"}
        )
    )

    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": "customer_id"}
        )
    )

    # Expectations for orders
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={
                "column": "order_status",
                "value_set": ["delivered", "shipped", "canceled", "processing", "invoiced", "approved"]
            }
        )
    )

    # Expectations for payments
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={
                "column": "payment_value",
                "min_value": 0,
                "max_value": 10000
            }
        )
    )

    # Expectations for products
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_of_type",
            kwargs={
                "column": "product_weight_g",
                "type_": "INTEGER"
            }
        )
    )

    return suite

def validate_data(df, suite_name="olist_data_quality"):
    """Validate dataframe against expectations"""

    # Convert to GE dataframe
    ge_df = ge.from_pandas(df)

    # Run validation
    results = ge_df.validate(expectation_suite=suite_name)

    return results