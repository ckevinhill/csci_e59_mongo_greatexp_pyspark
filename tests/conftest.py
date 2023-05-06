import pytest
import pandas as pd
from pyspark.sql import SparkSession

@pytest.fixture(autouse=True)
def correct_test_data_frame():
    """Create a Spark DataFrame to use for testing."""
    spark = SparkSession.builder.appName("UnitTestApplication").getOrCreate()
    data = [
        [1, "A"], 
        [2, "B"], 
        [3, "C"],
        [4, "D"]
    ]
    return spark.createDataFrame( pd.DataFrame(data, columns=["id", "letter"]), schema="id LONG, letter STRING" )

@pytest.fixture(autouse=True)
def incorrect_test_data_frame():
    """Create a Spark DataFrame to use for testing."""
    spark = SparkSession.builder.appName("UnitTestApplication").getOrCreate()
    data = [
        [1, "A"], 
        [2, "B"], 
        [3, "C"],
        [3, "C"],
        [4, "D"]
    ]
    return spark.createDataFrame( pd.DataFrame(data, columns=["id", "letter"]), schema="id LONG, letter STRING" )
