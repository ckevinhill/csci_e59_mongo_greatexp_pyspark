from gdh.pipeline.datatype import BaseValidatedDataFrame
import pandas as pd
from pyspark.sql import SparkSession


class ExampleInputDataFrame(BaseValidatedDataFrame):
    def add_validation_rules(self) -> None:
        """Add validation rules for ExampleDataFrame"""
        self.expect_column_to_exist("id")
        self.expect_column_values_to_be_unique("id")

    @classmethod
    def load_dataframe(cls):
        """Implement logic to load dataframe from source"""
        spark = SparkSession.getActiveSession()
        data = [[1, "A"], [2, "B"], [3, "C"], [4, "D"]]
        return spark.createDataFrame(
            pd.DataFrame(data, columns=["id", "letter"]),
            schema="id LONG, letter STRING",
        )


class ExampleOutputDataFrame(BaseValidatedDataFrame):
    def add_validation_rules(self) -> None:
        self.expect_table_row_count_to_be_between(min_value=1)
