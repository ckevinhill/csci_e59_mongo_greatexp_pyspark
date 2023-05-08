from data_pipeline.datatypes import (
    ExampleInputDataFrame,
    ExampleOutputDataFrame,
)
from data_pipeline.steps import ConcatenateColumnsStep
from data_pipeline.steps import UniqueCountStep
from data_pipeline.serializer import MongoDBFileStorage
from data_pipeline.db import get_mongo_database_collection

from gdh.pipeline.pipeline import PipelineMetadata, Builder
from gdh.pipeline.datatype import ValidationException

import os
from datetime import datetime
from pyspark.sql import SparkSession
from dotenv import load_dotenv
import random

load_dotenv()

# Create SparkSession:
SparkSession.builder.appName("Validation_Data_Generator").getOrCreate()


# Execute Pipeline:
for x in range(1500):
    try:
        # Properties associated with Pipeline:
        pipeline_names = [
            "CSCI-E58-Pipeline",
            "Final-Project-Pipeline",
            "Data-Generation-Pipeline",
            "Example-Pipeline",
            "Data-Ingestion-Pipeline",
        ]

        pipeline_name = random.choice(pipeline_names)

        metadata = {
            PipelineMetadata.PIPELINE_NAME: pipeline_name,
            PipelineMetadata.EXECUTION_DATE: round(
                datetime.utcnow().timestamp()
            ),  # default
            PipelineMetadata.VERIFY_INPUT_DATAFRAME: True,  # default
            PipelineMetadata.VERIFICATION_SERIALIZER: MongoDBFileStorage(
                path=get_mongo_database_collection(
                    os.getenv("VERIFICATION_MONGO_CONNECTION_STR"),
                    os.getenv("VERIFICATION_MONGO_DB_NAME"),
                    "pipeline",
                )
            ),
        }

        # Define Pipeline Steps:
        steps = [
            ConcatenateColumnsStep("id", "color", "new_col"),
            UniqueCountStep("new_col", "cnts"),
        ]

        # Create Pipeline:
        pipeline = Builder.build_with_input_type(
            input_type=ExampleInputDataFrame,
            steps=steps,
            output_type=ExampleOutputDataFrame,
            metadata=metadata,
        )

        output_df = pipeline.run_pipeline()
        print("Output DF:")
        output_df.spark_df.show()

    except ValidationException:
        print("Data validation error thrown.")
