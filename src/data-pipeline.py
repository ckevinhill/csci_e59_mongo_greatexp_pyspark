from gdh.pipeline.pipeline import PipelineMetadata, Builder
from gdh_hello_world_pipeline.datatypes import (
    ExampleInputDataFrame,
    ExampleOutputDataFrame,
)
from gdh_hello_world_pipeline.steps import ConcatenateColumnsStep
from gdh_hello_world_pipeline.steps import UniqueCountStep
from datetime import datetime
from pyspark.sql import SparkSession
from gdh.validation.filestorage import LocalFileStorage

# Properties associated with Pipeline:
metadata = {
    PipelineMetadata.PIPELINE_NAME: "GDH-Hello-World-Pipeline",
    PipelineMetadata.EXECUTION_DATE: round(datetime.utcnow().timestamp()),  # default
    PipelineMetadata.VERIFY_INPUT_DATAFRAME: True,  # default
    PipelineMetadata.VERIFICATION_SERIALIZER: LocalFileStorage(path="."),
}

# Create SparkSession:
app_name = metadata.get(PipelineMetadata.PIPELINE_NAME)
SparkSession.builder.appName(app_name).getOrCreate()

# Define Pipeline Steps:
steps = [
    ConcatenateColumnsStep("id", "letter", "new_col"),
    UniqueCountStep("new_col", "cnts"),
]

# Create Pipeline:
pipeline = Builder.build_with_input_type(
    input_type=ExampleInputDataFrame,
    steps=steps,
    output_type=ExampleOutputDataFrame,
    metadata=metadata,
)

# Execute Pipeline:
output_df = pipeline.run_pipeline()
output_df.spark_df.show()
