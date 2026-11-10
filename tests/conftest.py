"""
Shared pytest fixtures for the ecommerce data engineering project.

This file provides a SparkSession that can be reused by all tests.
"""

import pytest

from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """
    Create a SparkSession shared by the entire test session.

    Using scope="session" avoids creating a new SparkSession
    for every individual test, which significantly reduces
    test execution time.
    """

    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("ecommerce-pipeline-tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    yield spark

    # Stop Spark once all tests have completed.
    spark.stop()