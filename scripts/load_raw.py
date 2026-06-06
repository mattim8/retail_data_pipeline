import os
from pathlib import PurePosixPath

import psycopg2


def connect_postgres():
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(
            dbname=os.environ["RETAIL_DB"],
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
            host=os.environ["POSTGRES_HOST"],
            port=os.environ["POSTGRES_PORT"],
        )
        print("Connected to PostgreSQL database successfully")
        cur = conn.cursor()
        data_path = PurePosixPath(os.getenv("DATA_PATH", "/data/sample"))
        cur.execute(
            """
            TRUNCATE TABLE
                raw.customers,
                raw.orders,
                raw.products,
                raw.order_items;
            """
        )
        sql1 = f"""COPY raw.customers
                 FROM '{data_path / "olist_customers_dataset.csv"}'
                 DELIMITER ','
                 CSV HEADER;"""
        sql2 = f"""COPY raw.orders
                 FROM '{data_path / "olist_orders_dataset.csv"}'
                 DELIMITER ','
                 CSV HEADER;"""
        sql3 = f"""COPY raw.products
                 FROM '{data_path / "olist_products_dataset.csv"}'
                 DELIMITER ','
                 CSV HEADER;"""
        sql4 = f"""COPY raw.order_items
                 FROM '{data_path / "olist_order_items_dataset.csv"}'
                 DELIMITER ','
                 CSV HEADER;"""
        cur.execute(sql1)
        cur.execute(sql2)
        cur.execute(sql3)
        cur.execute(sql4)
        conn.commit()
        print("Data loaded into raw tables successfully")
    except Exception as e:
        print(f"An error occurred: {e}")
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
            print("PostgreSQL connection closed")


if __name__ == "__main__":
    connect_postgres()
