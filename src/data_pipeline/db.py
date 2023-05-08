from pymongo import MongoClient


def get_mongo_database_collection(connection_str, db_name, collection_name):
    # Create a connection using MongoClient. You can import MongoClient or use
    # pymongo.MongoClient
    client = MongoClient(connection_str)
    db_name = client[db_name]
    return db_name[collection_name]
