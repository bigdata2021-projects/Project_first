import pandas as pd
import os
import opendatasets as od

DIMENSIONS = [10000, 100000, 1000000, 10000000]
DATA_DIR = './data/'
DATASET_URL = 'https://www.kaggle.com/datasets/ehallmar/daily-historical-stock-prices-1970-2018/download?datasetVersionNumber=1'
DATASET_NAME = 'daily-historical-stock-prices-1970-2018'


def create_files_with_distinct_dimension(path_to_filename: str):
    df = pd.read_csv(path_to_filename)
    sorted_df = df.sort_values(by=['date'], ascending=True)
    for dim in DIMENSIONS:
        filename = "historical_stock_prices_" + str(dim)
        path = DATA_DIR + DATASET_NAME + '/' + filename + ".csv"
        new_df = sorted_df.head(dim)
        new_df.to_csv(path_or_buf=path, columns=df.columns, index=False)


def remove_commas_from_sector_names(path_to_filename: str):
    df = pd.read_csv(path_to_filename)
    df_app = df["name"]
    df_app = df_app.apply(lambda name: str(name).replace(",", ""))
    df["name"] = df_app
    df = df[["ticker", "exchange", "name", "sector", "industry"]]
    df.to_csv(path_to_filename, index=False)

def download_dataset():
    os.mkdir(DATA_DIR)
    od.download(dataset_id_or_url=DATASET_URL, data_dir=DATA_DIR)
    


if __name__ == '__main__':
    if not os.path.exists(DATA_DIR):
        download_dataset(DATA_DIR)
    remove_commas_from_sector_names(DATA_DIR + DATASET_NAME + "/historical_stocks.csv")
    create_files_with_distinct_dimension(DATA_DIR + DATASET_NAME + "/historical_stock_prices.csv")
