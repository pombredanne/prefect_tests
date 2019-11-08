from typing import List, Union, Dict

import pandas as pd
from prefect import task


@task
def project_columns(df: pd.DataFrame, column_list: List[str]) -> pd.DataFrame:
    return df[column_list]


@task
def count_rows(df: pd.DataFrame) -> None:
    print(df.count())


@task
def sum_rows(df: pd.DataFrame) -> pd.DataFrame:
    return df.sum()


@task
def add_column_to_df(df: pd.DataFrame, column: str, base_value: Union[str, int, float, bool, None]) -> pd.DataFrame:
    df[column] = base_value

    return df


@task
def create_empty_df() -> pd.DataFrame:
    return pd.DataFrame()


@task
def create_df(column_data: Dict[str, list]) -> pd.DataFrame:
    return pd.DataFrame(column_data)


@task
def print_df(df: pd.DataFrame):
    print(df)
