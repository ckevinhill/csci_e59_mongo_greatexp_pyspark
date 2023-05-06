"""TestCase for DataType"""
from gdh_hello_world_pipeline.datatypes import (
    ExampleInputDataFrame,
    ExampleOutputDataFrame,
)


def test_example_input_dataframe(correct_test_data_frame, incorrect_test_data_frame):
    """Test ExampleInputDataFrame."""

    df = ExampleInputDataFrame(correct_test_data_frame)
    validation = df.validate()
    assert validation.success is True

    df = ExampleInputDataFrame(incorrect_test_data_frame)
    validation = df.validate()
    assert validation.success is False


def test_example_output_dataframe(correct_test_data_frame, incorrect_test_data_frame):
    """Test ExampleOutputDataFrame."""

    df = ExampleOutputDataFrame(correct_test_data_frame)
    validation = df.validate()
    assert validation.success is True
