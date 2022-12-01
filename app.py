import pandas as pd
import os

BASE_DIR = r'/Users/fallout/research/data/retail_db_json'
table_name = 'orders'
query = 'SELECT * FROM departments'
"""
Connection to the database
"""
conn = 'postgresql://retail_user:itversity@localhost:5432/retail_db'


def main():
    """
    Dynamically read files
    """
    file_name = os.listdir(f'{BASE_DIR}/{table_name}')[0]
    fp = f'{BASE_DIR}/{table_name}/{file_name}'
    df = pd.read_json(fp, lines=True)
    print(df.shape)

    """
    Reading Data in Chunks using Pandas
    """
    json_reader = pd.read_json(fp, lines=True, chunksize=1000)
    for idx, df in enumerate(json_reader):
        print(f'Number of records in chunk with index {idx} is {df.shape[0]}')


def load_table():
    pd.read_sql('SELECT * FROM departments', conn)

    file_name = os.listdir(f'{BASE_DIR}/{table_name}')[0]
    fp = f'{BASE_DIR}/{table_name}/{file_name}'
    df = pd.read_json(fp, lines=True)
    df.to_sql(table_name, conn, if_exists='append', index=False)


def read_table():
    df = pd.read_sql(query, conn)
    print(df.head())
    df = pd.read_sql(
    'SELECT order_status, count(1) AS order_count FROM orders GROUP BY order_status',
    conn)
    print(df)




def p_orders_table():
    """
    Read data from files using read_json with chunksize set to 1000.
    Iterate through JSON Reader object.
    Load Dataframe related to each chunk into the target table.
    """
    file_name = os.listdir(f'{BASE_DIR}/{table_name}')[0]
    fp = f'{BASE_DIR}/{table_name}/{file_name}'
    json_reader = pd.read_json(fp, lines=True, chunksize=1000)

    for df in json_reader:
        min_key = df['order_id'].min()
        max_key = df['order_id'].max()
        df.to_sql(table_name, conn, if_exists='append', index=False)
        print(f'Processed {table_name} with in the range of {min_key} and {max_key}')


if __name__ == '__main__':
    # main()
    # sql_table()
    read_table()
    #p_orders_table()
