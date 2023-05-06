"""TestCase for Pipeline"""
from gdh_hello_world_pipeline.steps import ConcatenateColumnsStep
from gdh.pipeline.pipeline import Pipeline, PipelineMetadata
from gdh_hello_world_pipeline.datatypes import ExampleInputDataFrame


def test_pipeline(correct_test_data_frame):
    """Test Pipeline."""

    metadata = {PipelineMetadata.VERIFY_INPUT_DATAFRAME: False}

    steps = [ConcatenateColumnsStep("id", "letter", "new_col")]

    pipeline = Pipeline(
        input_df=ExampleInputDataFrame(correct_test_data_frame),
        steps=steps,
        metadata=metadata,
    )

    # Returns as SparkDF since now output_type specified:
    result_df = pipeline.run_pipeline()

    assert "new_col" in result_df.columns
