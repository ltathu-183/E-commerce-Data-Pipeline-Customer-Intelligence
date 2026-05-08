"""
Great Expectations Data Quality Tests for Olist E-commerce Data
================================================================

This module defines data quality expectations for the ETL pipeline.
"""

from great_expectations.expectations.expectation_configuration import (
    ExpectationConfiguration,
)

import great_expectations as ge


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
    """Validate dataframe against basic expectations (simplified for testing)"""

    results = {
        "success": True,
        "statistics": {},
        "expectations": []
    }

    # Basic validations for customers dataset
    if "customer_id" in df.columns:
        # Check column exists
        results["expectations"].append({
            "expectation_type": "expect_column_to_exist",
            "column": "customer_id",
            "success": True,
            "result": "Column exists"
        })

        # Check for nulls
        null_count = df["customer_id"].isnull().sum()
        results["expectations"].append({
            "expectation_type": "expect_column_values_to_not_be_null",
            "column": "customer_id",
            "success": null_count == 0,
            "result": f"{null_count} null values found"
        })

    # Basic statistics
    results["statistics"] = {
        "row_count": len(df),
        "column_count": len(df.columns),
        "columns": list(df.columns)
    }

    # Check if any expectation failed
    for exp in results["expectations"]:
        if not exp["success"]:
            results["success"] = False
            break

    return results

