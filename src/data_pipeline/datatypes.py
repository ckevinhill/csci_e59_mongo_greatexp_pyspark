from gdh.pipeline.datatype import BaseValidatedDataFrame
import pandas as pd
from pyspark.sql import SparkSession
import numpy as np
import string


class ExampleInputDataFrame(BaseValidatedDataFrame):
    def add_validation_rules(self) -> None:
        """Add validation rules for ExampleDataFrame"""
        self.expect_column_to_exist("id")
        self.expect_column_to_exist("color")
        self.expect_column_to_exist("letter")

        self.expect_column_values_to_be_unique("id")
        self.expect_column_values_to_be_unique("letter")

        self.expect_column_mean_to_be_between(column="id", min_value=-3, max_value=3)

    @classmethod
    def load_dataframe(cls):
        """Implement logic to load dataframe from source"""
        spark = SparkSession.getActiveSession()

        # Generate Fake Data:
        colors = ["Red", "White", "Green", "Blue", "Purple", "Gold"]
        color_data = [str(np.random.choice(colors)) for x in range(5)]
        number_data = (
            np.random.normal(loc=0, scale=10, size=5).round(0).astype(np.int16).tolist()
        )
        letter_data = [
            str(np.random.choice(list(string.ascii_lowercase))) for x in range(5)
        ]

        data = [list(a) for a in zip(number_data, color_data, letter_data)]

        df = spark.createDataFrame(
            pd.DataFrame(data, columns=["id", "color", "letter"]),
            schema="id LONG, color STRING, letter STRING",
        )

        return df


class ExampleOutputDataFrame(BaseValidatedDataFrame):
    def add_validation_rules(self) -> None:
        self.expect_column_to_exist("cnts")
        self.expect_table_row_count_to_be_between(min_value=1)
        self.expect_column_sum_to_be_between(column="cnts", min_value=5)
