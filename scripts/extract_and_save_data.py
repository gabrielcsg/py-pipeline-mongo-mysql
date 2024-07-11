from pymongo.mongo_client import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection
from pymongo.server_api import ServerApi
import os
import requests
from dotenv import load_dotenv

load_dotenv()


def connect_mongo(uri: str) -> MongoClient:
    client = MongoClient(uri, server_api=ServerApi('1'))
    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(e)

    return client


def create_connect_db(client: MongoClient, db_name: str) -> Database:
    return client[db_name]


def create_connect_collection(db: Database, col_name: str) -> Collection:
    return db[col_name]


def extract_api_data(uri: str) -> list[dict]:
    return requests.get(uri).json()


def insert_data(col: Collection, data: list[dict]) -> int:
    docs = col.insert_many(data)
    return len(docs.inserted_ids)


if __name__ == "__main__":

    # Create Database and Collection
    mongo_uri = os.getenv('MONGO_URI')
    client = connect_mongo(mongo_uri)
    db = create_connect_db(client, 'db_produtos')
    collection = create_connect_collection(db, 'produtos')

    # Extract API Data
    api_url = 'https://labdados.com/produtos'
    api_data = extract_api_data(api_url)
    print(f'Dados extraidos: {len(api_data)}')

    # Saving data
    n_docs = insert_data(collection, api_data)
    print(f'Dadoso inseridos: {n_docs}')

    # Close connection
    client.close()
