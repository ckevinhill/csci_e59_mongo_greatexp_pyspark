from gdh.validation.filestorage import FileStorage
from great_expectations.core import ExpectationSuiteValidationResult
from pymongo.collection import Collection


class MongoDBFileStorage(FileStorage):
    """Serializer interface for MongoDB."""

    db_collection: Collection = None

    def __init__(self, path, **kwargs) -> None:
        """Initialize Mongodb Serializer.

        Parameters:
        - path (positional): The connection to MongDB for use.
        """
        self.db_collection = path

    def save_validation_results(
        self, pipeline_name: str, results: ExpectationSuiteValidationResult
    ) -> None:
        """Save validation results to MongoDB."""

        document_id = self.get_serialization_filename(pipeline_name, results)

        # Convert to JSON Dict for MongoDB storage:
        if isinstance(results, ExpectationSuiteValidationResult):
            results = results.to_json_dict()

        results["_id"] = document_id
        results["pipeline_name"] = pipeline_name

        self.db_collection.insert_one(results)
