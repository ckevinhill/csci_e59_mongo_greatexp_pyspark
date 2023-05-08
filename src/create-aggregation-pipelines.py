from pymongo import MongoClient
from pprint import pprint
import os
from dotenv import load_dotenv

CREATE_VIEW = True


def get_database():
    # Provide the mongodb atlas url to connect python to mongodb using pymongo
    CONNECTION_STRING = os.getenv("VERIFICATION_MONGO_CONNECTION_STR")

    # Create a connection using MongoClient. You can import MongoClient or use
    # pymongo.MongoClient
    client = MongoClient(CONNECTION_STRING)

    # Create the database for our example (we will use the same database
    # through out the tutorial
    db_name = os.getenv("VERIFICATION_MONGO_DB_NAME")
    return client[db_name]


def create_view(dbname, view_name, aggregation_pipeline):
    # Create Collection-View:
    dbname.command(
        {
            "create": view_name,
            "viewOn": "pipeline",
            "pipeline": aggregation_pipeline,
        }
    )


def get_statistics_aggregation_pipeline():
    # Aggregation for High-level Reporting:
    aggregation_pipeline = [
        {
            "$group": {
                "_id": "$pipeline_name",
                "ttl_expectations": {"$sum": "$statistics.evaluated_expectations"},
                "successful_expectations": {
                    "$sum": "$statistics.successful_expectations"
                },
                "unsuccessful_expectations": {
                    "$sum": "$statistics.unsuccessful_expectations"
                },
            },
        },
    ]

    return aggregation_pipeline


def get_pipeline_failures_view():
    # Aggregation for Failure Reporting:
    aggregation_pipeline = [
        {"$match": {"success": False}},  # Filter to batches that contain a failure
        {"$unwind": "$results"},  # Unwind specific expectations
        {
            "$addFields": {
                "success": "$results.success",
                "timestamp": "$meta.validation_time",
                "expected": "$results.expectation_config.expectation_type",
                "column": "$results.expectation_config.kwargs.column",
                "min": "$results.expectation_config.kwargs.min_value",
                "max": "$results.expectation_config.kwargs.max_value",
            }
        },
        {"$match": {"success": False}},  # Filter expectations that failed
        {
            "$project": {
                "_id": 1,
                "pipeline_name": 1,
                "success": 1,
                "timestamp": 1,
                "expected": 1,
                "column": 1,
                "min": 1,
                "max": 1,
            }
        },
    ]

    return aggregation_pipeline


def get_pipeline_common_failures_view():
    # Group by pipeline, expected & colummn
    aggregation_pipeline = get_pipeline_failures_view()
    aggregation_pipeline.append(
        {
            "$group": {  # Group total failures by pipeilne, expectation & column
                "_id": {
                    "pipeline_name": "$pipeline_name",
                    "expected": "$expected",
                    "column": "$column",
                },
                "unsuccessful_expectations": {"$count": {}},
            },
        }
    )

    return aggregation_pipeline


# This is added so that many files can reuse the function get_database()
if __name__ == "__main__":
    # Load environmental variables:
    load_dotenv()

    # Create database/collections connection:
    dbname = get_database()
    collection = dbname["pipeline"]

    # aggregation_pipeline = get_statistics_aggregation_pipeline()
    # aggregation_pipeline = get_pipeline_failures_view()
    aggregation_pipeline = get_pipeline_common_failures_view()

    # Run pipeline for validation/check:
    results = collection.aggregate(aggregation_pipeline)
    pprint(list(results))

    explain_output = dbname.command(
        "explain",
        {"aggregate": "pipeline", "pipeline": aggregation_pipeline, "cursor": {}},
        verbosity="executionStats",
    )
    pprint(explain_output)

    # create_view(dbname, "pipeline_summary_statistics_view", aggregation_pipeline)
    # create_view(dbname, "pipeline_failures_view", aggregation_pipeline)
    # create_view(dbname, "pipeline_common_failures_view", aggregation_pipeline)
    print("done")
