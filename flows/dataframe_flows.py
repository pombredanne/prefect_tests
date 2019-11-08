from prefect import Flow, Parameter, unmapped

from tasks.dataframe_tasks import project_columns, count_rows, add_column_to_df, create_df, print_df, sum_rows
from tasks.file_tasks import csv_to_df


# Flow definitions
with Flow('Create simple data frame') as simple_df_flow:
    initial_data = Parameter('initial_data')
    df = create_df(initial_data)

    df = add_column_to_df(df, 'column_a', 0)

    df = add_column_to_df(df, 'column_b', 1)

    count_rows(df)

with Flow('Read file and sum columns') as simple_csv_df_flow:
    file_path = Parameter('file_path')
    csv_separator = Parameter('separator')
    df = csv_to_df(file_path=file_path, sep=csv_separator)

    # If we don't add the unmapped call to the column_list parameter
    # prefect will automatically create two separate tasks for each item
    df = project_columns(df=df, column_list=unmapped(['price', 'volume']))

    df = sum_rows(df)

    print_df(df)

# Entry test point

if __name__ == '__main__':
    # This flow builds a dataframe programmatically and counts the number of rows in the end
    simple_df_flow.visualize()
    # simple_df_flow.run(initial_data=dict(column_c=[1, 2, 3, 4]))

    # This flow reads the csv file found at the project root, projects the
    # price column, sums it and then prints the dataframe
    simple_csv_df_flow.visualize()
    # simple_csv_df_flow.run(file_path='../mock_data.csv', separator='|')
