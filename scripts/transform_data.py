from pymongo.collection import Collection
import pandas as pd
from extract_and_save_data import connect_mongo, create_connect_db, create_connect_collection
import os
from dotenv import load_dotenv

load_dotenv()


def visualize_collection(col: Collection) -> None:
    for doc in col.find():
        print(doc)


def rename_column(col: Collection, col_name: str, new_name: str) -> None:
    col.update_many({}, {
        "$rename": {f"{col_name}": f"{new_name}"}
    })
    print("Column renamed successful")


def select_category(col: Collection, category: str) -> list[dict]:
    query = {"Categoria do Produto": f"{category}"}

    category_list = [doc for doc in col.find(query)]

    return category_list


def make_regex(col: Collection, regex: str) -> list[dict]:
    query = {"Data da Compra": {"$regex": f"{regex}"}}

    regex_list = [doc for doc in col.find(query)]

    return regex_list


def create_dataframe(data_list: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(data_list)
    return df


def format_date(df: pd.DataFrame) -> None:
    df['Data da Compra'] = pd.to_datetime(
        df['Data da Compra'], format='%d/%m/%Y')
    df['Data da Compra'] = df['Data da Compra'].dt.strftime('%Y-%m-%d')


def save_csv(df: pd.DataFrame, path: str) -> None:
    df.to_csv(path, index=False)


if __name__ == "__main__":
    # create database connection
    mongo_uri = os.getenv("MONGO_URI")
    client = connect_mongo(mongo_uri)
    db = create_connect_db(client, "db_produtos")
    col = create_connect_collection(db, "produtos")

    # rename colmuns: latitude e longitude
    rename_column(col, "lat", "Latitude")
    rename_column(col, "lon", "Longitude")

    # save books category
    book_list = select_category(col, "livros")
    df_books = create_dataframe(book_list)
    format_date(df_books)
    save_csv(df_books, "data/book_table.csv")

    # save products purchased in 2021
    product_list = make_regex(col, "/202[1-9]")
    df_products = create_dataframe(product_list)
    format_date(df_products)
    save_csv(df_products, "data/table_2021_onwards.csv")
