from utils.bigQueryClient import BigQueryClient
from utils.data_io import load_yaml


def execute_sql_scripts(
    client: BigQueryClient,
    folder_path,
    sql_file=None,
    env_variables: dict = None,
    return_df: bool = False,
):

    if sql_file:
        folder_path = folder_path / sql_file
    with open(folder_path, "r") as f:
        query = f.read()
    if env_variables:
        query = fill_query_with_variables(query, env_variables)
    if return_df:
        df = client.run_query(query=query, return_df=True)
        return df
    client.run_query(query=query)


def fill_query_with_variables(query, variables):
    for key, value in variables.items():
        placeholder = f"{{{key}}}"
        if placeholder in query:
            query = query.replace(placeholder, value)
    return query


def extract_whole_table_from_gcp(sql_file, env_variables, query_path, limit=0):
    client = BigQueryClient()
    if limit > 0: 
        env_variables["limit"] = f"LIMIT {limit}"
    else: 
        env_variables["limit"] = ''

    df = execute_sql_scripts(
        client=client,
        folder_path=query_path,
        env_variables=env_variables,
        sql_file=sql_file,
        return_df=True,
    )

    return df