"""Shared pytest fixtures."""

import os
import shutil

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """A small local Spark session shared by all Spark tests.
    """
    if "DATABRICKS_RUNTIME_VERSION" in os.environ:
        pytest.skip("Spark tests use a local session; run them locally or in CI.")
    if shutil.which("java") is None:
        if os.environ.get("CI"):
            pytest.fail("Java is required for the Spark tests in CI.")
        pytest.skip("Java not found; Spark tests are skipped.")

    session = SparkSession.builder.master("local[1]").appName("tests").getOrCreate()
    yield session
    session.stop()