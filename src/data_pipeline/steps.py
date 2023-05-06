from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, concat
from gdh.pipeline.step import BaseStep


class ConcatenateColumnsStep(BaseStep):
    """Example of a simple Step that adds a Column."""

    def __init__(self, col_1="", col_2="", col_name="new_col") -> None:
        self.col_1 = col_1
        self.col_2 = col_2
        self.col_name = col_name

    def run(self, input_df: DataFrame, **kwargs) -> DataFrame:
        """Step run implementation."""
        return input_df.withColumn(
            self.col_name, concat(col(self.col_1), col(self.col_2))
        )


class UniqueCountStep(BaseStep):
    """Example of a simple Step that uniquely counts a Column."""

    def __init__(self, col="", cnt_col_name="unique_count") -> None:
        self.col = col
        self.cnt_col_name = cnt_col_name

    def run(self, input_df: DataFrame, **kwargs) -> DataFrame:
        """Step run implementation."""
        spark = SparkSession.getActiveSession()

        input_df.createOrReplaceTempView("df")
        sql = f"Select {self.col}, count(*) as {self.cnt_col_name} from df group by {self.col}"
        return spark.sql(sql)
