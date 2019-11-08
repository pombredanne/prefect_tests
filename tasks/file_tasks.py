import pandas as pd
from prefect import task


@task
def csv_to_df(file_path: str, sep: str = ',') -> pd.DataFrame:
    return pd.read_csv(file_path, sep=sep)
