# CSCI-E59 Final Project Repository

This provides code and examples for CSCSI-E59: Designing and Developing Relational and NoSQL Databases.

## Project Objectives

A Data Pipeline (ETL, etc) is only useful if you can guarantee the QUALITY of the resulting data.  To that end this project will explore the application and collection of validation data generated via the Great Expectations Data Quality management framework.

Learning Objectives:

* Implementation and configuration of the Great Expectations validation framework
* Setup and use of MongoDB for storage of JSON documents
* JSON collection aggregation and summarization for reporting
* Integration of MongoDB with Spark for downstream processing

## Key Components

* MongoDB environment stand-up via [docker-compose](docker-compose.yml)
* [Data Generation](src/data-pipeline.py) via Great Expectations
* MongoDB [Aggregation Pipeline creation](src/create-aggregation-pipelines.py) via PyMongo
* [PySpark DataFrame creation](src/pyspark-reporting.py) via Mongo-Spark Connector

Environmental Variable File (.env) will need to be created with the following variables, examples below:

```bash
MONGO_ROOT_USER=root
MONGO_ROOT_PASSWORD=password
VERIFICATION_MONGO_CONNECTION_STR=mongodb://root:password@localhost:27017
VERIFICATION_MONGO_DB_NAME=verification_results
```

## Presentation Material

Final presentation material (high-level) can be found [here](presentation_material/Final%20Presentation.pptx).
