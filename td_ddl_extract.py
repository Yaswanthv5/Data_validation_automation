import re
import os
from io import StringIO
import pandas as pd #type: ignore
from pyspark.sql import SparkSession #type: ignore
from pyspark.sql.functions import * #type: ignore

decipher_dt = {
    'A1': 'ARRAY', 'AN': 'MULTI-DIMENSIONAL ARRAY', 'AT': 'TIME', 'BF': 'BYTE', 'BO': 'BLOB', 'BV': 'VARBYTE',
    'CF': 'CHAR', 'CO': 'CLOB', 'CV': 'VARCHAR', 'D': 'DECIMAL',
    'DA': 'DATE', 'DH': 'DATE INTERVAL TO HOUR', 'DS': 'INTERVAL DAY TO SECOND', 'DY': 'INTERVAL DAY',
    'F': 'FLOAT', 'HS': 'INTERVAL HOUR TO SECOND', 'I': 'INT', '11': 'BYTEINT',
    '12': 'SMALLINT', '18': 'BIGINT', 'JN': 'JSON', 'MI': 'INTERVAL MINUTE', 'MO': 'INTERVAL MONTH',
    'N': 'NUMBER', 'PD': 'DATE', 'SZ': 'TIMESTAMP WITH TIME ZONE', 'TS': 'TIMESTAMP',
    'TZ': 'TIME WITH TIME ZONE', 'UT': 'UDT TYPE', 'YR': 'INTERVAL YEAR'
}

os.environ["HADOOP_HOME"] = "I:\\hadoop\\hadoop-3.2.0"
os.environ["hadoop.home.dir"] = "I:\\hadoop\\hadoop-3.2.0"
os.environ["PYSPARK_DRIVER_PYTHON"] = "C:\\example\\DEV\\TMP\\ds\\data\\python3.12\\site-packages\\pyspark\\bin\\.."

spark = (SparkSession.builder.config("spark.jars",
    "terajdbc4-16.10.jar, tdgssconfig-16.10.jar, spark-core_2.13-3.3.2.jar,"
    "hadoop-core-1.2.1.jar")
    .config("spark.executorEnv.PYSPARK_PYTHON",
    "C:\\example\\DEV\\TMP\\ds\\tools\\python3.12\\latest\\python.exe")
    .config("spark.pyspark.python", "C:\\example\\DEV\\TMP\\ds\\tools\\python3.12\\latest\\python.exe").getOrCreate())

# spark.conf.set("spark.sql.session.timeZone", "CST")
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

def create_dict(df):
    c_c, c_dt, c_n = df['ColumnName'], df['ColumnType'], df['Nullable']
    column_dic = {
        c_c[i].rstrip(): {'data_type': c_dt[i].split('(')[0], 'Nullable': c_n[i],
        'Length': re.findall(r'\((\d+(?:\d+)?)\)', c_dt[i])[0]} if len(
        re.findall(r'\((\d+(?:\d+)?)\)', c_dt[i])) > 0 else {'data_type': c_dt[i].split('(')[0],
        'Nullable': c_n[i]} for i in range(len(c_c))}
    return column_dic

def convert_timestamp(df):
    timestamp_columns = [col_name for col_name, data_type in df.dtypes if data_type.startswith("timestamp")]
    for column in timestamp_columns:
        df = df.withColumn(column, df[column].cast('string'))
    return df

def td_ddl_query(table_name, url, userid, pwd):
    query = f"(select * from dbc.columns where databasename = '{table_name.split('.')[0]}' and tablename = '{table_name.split('.')[1]}') a"
    print(query)
    df_data = spark.read.format("jdbc").options(
        driver="com.teradata.jdbc.TeraDriver",
        url=url,
        user=userid,
        password=pwd,
        dbtable=query).load()
    df_data.select(df_data.columns[:14])
    dfp = df_data.select(df_data.columns[:14]).toPandas()
    for i in range(len(dfp)):
        dfp.loc[i, 'ColumnType'] = decipher_dt[dfp.loc[i, 'ColumnType'].strip()]
        if dfp.loc[i, 'ColumnType'] == 'DECIMAL':
            dfp.loc[i, 'ColumnType'] = str(dfp.loc[i, 'ColumnType']) + '(' + str(
                dfp.loc[i, 'DecimalTotalDigits']).replace('.', '') + ',' + str(
                dfp.loc[i, 'DecimalFractionalDigits']).replace('.', '') + ')'
    q_d = dfp.to_dict('list')
    return create_dict(q_d)

def query_executor_length(query_d, url, userid, pwd):
    df_dt = spark.read.format("jdbc").options(
        driver="com.teradata.jdbc.TeraDriver",
        url=url,
        user=userid,
        password=pwd,
        dbtable=query_d).load()
    decimal_l = [col_name for col_name, data_type in df_dt.dtypes if data_type.startswith("decimal")]
    for column in decimal_l:
        df_dt = df_dt.withColumn(column, df_dt[column].cast('integer'))
    converted_df = convert_timestamp(df_dt).toPandas()
    df_csv = converted_df.to_csv()
    df_data = pd.read_csv(StringIO(df_csv), index_col=False).drop(labels='Unnamed: 0', axis=1)
    return df_data

def query_executor(query_d, url, userid, pwd):
    df_dt = spark.read.format("jdbc").options(
        driver="com.teradata.jdbc.TeraDriver",
        url=url,
        user=userid,
        password=pwd,
        dbtable=query_d).load()
    decimal_l = [col_name for col_name, data_type in df_dt.dtypes if data_type.startswith("decimal")]
    for column in decimal_l:
        df_dt = df_dt.withColumn(column, df_dt[column].cast('string'))
    converted_df = convert_timestamp(df_dt).toPandas()
    df_csv = converted_df.to_csv()
    df_data = pd.read_csv(StringIO(df_csv), index_col=False).drop(labels='Unnamed: 0', axis=1)
    return df_data

def pandas_to_spark(df):
    converted_df = spark.createDataFrame(df)
    return converted_df

if __name__ == "__main__":
    # Example usage
    q = "(select * from example_db.example_table) a"
    # df_q = query_executor(q, "jdbc:teradata://example.uat/DATABASE='',LOGMECH-LDAP,DBS_PORT=1025", "example_user", "example_password")
    # df_p = pandas_to_spark(df_q)
    # print(df_q)
    # df_p.show(10, False)