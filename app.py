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


if __name__ == '__main__':
    main()
