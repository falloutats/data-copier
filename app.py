import pandas as pd
import os

BASE_DIR = r'/Users/fallout/research/data/retail_db_json'
table_name = 'order_items'


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


def sql_table():
    users_list = [
        {'user_first_name': 'Scott', 'user_last_name': 'Tiger'},
        {'user_first_name': 'Donald', 'user_last_name': 'Duck'}
    ]
    df = pd.DataFrame(users_list)
    conn = 'postgresql://retail_user:itversity@localhost:5432/retail_db'
    df.to_sql('users', conn, if_exists='append', index=False)
    print(pd.read_sql('SELECT * FROM users', conn))


if __name__ == '__main__':
    main()
    sql_table()
