import os

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
        sql1 = """COPY raw.customers
                 FROM '/data/full/olist_customers_dataset.csv'
                 DELIMITER ','
                 CSV HEADER;"""
        sql2 = """COPY raw.orders
                 FROM '/data/full/olist_orders_dataset.csv'
                 DELIMITER ','
                 CSV HEADER;"""
        sql3 = """COPY raw.products
                 FROM '/data/full/olist_products_dataset.csv'
                 DELIMITER ','
                 CSV HEADER;"""
        sql4 = """COPY raw.order_items
                 FROM '/data/full/olist_order_items_dataset.csv'
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
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
            print("PostgreSQL connection closed")


if __name__ == "__main__":
    connect_postgres()
