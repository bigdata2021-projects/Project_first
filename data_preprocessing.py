import pandas as pd

DIMENSIONS = [10000, 100000, 1000000, 10000000]



def create_files_with_distinct_dimension(path_to_filename: str):
    df = pd.read_csv(path_to_filename)
    sorted_df = df.sort_values(by=['date'], ascending=True)
    for dim in DIMENSIONS:
        filename = "historical_stock_prices_" + str(dim)
        path = "insert/path/" + filename + ".csv"
        new_df = sorted_df.head(dim)
        new_df.to_csv(path_or_buf=path, columns=df.columns, index=False)


def remove_commas_from_sector_names(path_to_filename: str):
    df = pd.read_csv(path_to_filename)
    df_app = df["name"]
    df_app = df_app.apply(lambda name: str(name).replace(",", ""))
    df["name"] = df_app
    df = df[["ticker", "exchange", "name", "sector", "industry"]]
    df.to_csv(path_to_filename, index=False)


if __name__ == '__main__':
    remove_commas_from_sector_names("historical_stocks")
    create_files_with_distict_dimension("historical_stock_prices.csv")
