import boto3  # type: ignore
import time
import re
import pandas as pd  # type: ignore
from io import StringIO

def get_table_data(query, database_name='data_validation_db'):
    outputputpath = 's3://your-bucket/athenaoutput/'  # s3 output path and folder relative path to the query athena output
    global ddl

    athena_client = boto3.client('athena', region_name='us-east-1')

    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database_name
        },
        ResultConfiguration={
            'OutputLocation': outputputpath
        },
        WorkGroup='athena-query-workgroup'
    )

    query_execution_id = response['QueryExecutionId']
    print(f"Query_execution_id: {query_execution_id}")

    status = None
    while status not in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
        response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        status = response['QueryExecution']['Status']['State']
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        else:
            time.sleep(2)

    if status == 'SUCCEEDED':
        print("Execution success")
        s3_client = boto3.client('s3')
        s3_path = 'athenaoutput/' + query_execution_id + '.csv'
        ddl = s3_client.get_object(Bucket='your-bucket', Key=s3_path)
        data = ddl['Body'].read().decode('utf-8')
        df_data = pd.read_csv(StringIO(data), index_col=False)
        return df_data
    elif status in ['FAILED', 'CANCELLED']:
        print('Failed')
        return ''

def get_table_ddl(table_name, database_name):
    outputputpath = 's3://your-bucket/athenaoutput/'
    global ddl

    athena_client = boto3.client('athena', region_name='your region')

    query = f"desc {database_name}.{table_name}"

    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database_name
        },
        ResultConfiguration={
            'OutputLocation': outputputpath
        },
        WorkGroup='athena-query-workgroup'
    )

    query_execution_id = response['QueryExecutionId']
    print(f"Query_execution_id: {query_execution_id}")

    status = None
    while status not in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
        response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        status = response['QueryExecution']['Status']['State']
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        else:
            time.sleep(2)

    response = athena_client.get_query_results(QueryExecutionId=query_execution_id)
    data = {}

    for row in response['ResultSet']['Rows'][1:]:
        row_data = [field.get('VarCharValue').upper() for field in row['Data']]
        for i in row_data:
            if '' not in i.split('\t')[0] and i.split('\t')[0] and i.split('\t')[1] != 'VOID':
                data[i.split('\t')[0]] = {
                    'data_type': i.split('\t')[1].split('(')[0],
                    'Length': re.findall(r'\((\d+(?:,\s+\d+)?)\)', i.split('\t')[1])[0].replace('', '') if len(
                        re.findall(r'\((\d+(?:,\s+\d+)?)\)', i.split('\t')[1])) > 0 else i.split('\t')[1].split('(')[0]
                }

if __name__ == "__main__":
    data = get_table_ddl('your_table_name', 'data_validation_db')

    for dt, val in data.items():
        print(dt, val)

    # Uncomment the following lines if needed
    # print(data)
    # ib_queries = sql_generate(data, "data_validation_db.ARRANGEMENT_T_AR")
    # print([key for key in ib_queries['Iceberg_queries'].keys()])
    # print(ib_queries['Iceberg_queries']['Count'])
    # data_det_table = ib_queries['Iceberg_queries']['Count'], 'data_validation_db'    