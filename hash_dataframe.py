import pyspark
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, BooleanType, DateType, TimestampType

def profile_dataset(df):
    df = df.select([col(column).alias(column.lower()) for column in df.columns])
    alpha_numeric_columns = filter(lambda f: (isinstance(f.dataType, StringType)
                                              or isinstance(f.dataType, BooleanType)
                                              or isinstance(f.dataType, DateType)
                                              or isinstance(f.dataType, TimestampType)), df.schema.fields)
    alphanum_cols_list = list(alpha_numeric_columns)
    hashed_dataset = df
    for col_name in alphanum_cols_list:
        hashed_dataset = hashed_dataset.withColumn(col_name, pyspark.sql.functions.hash(col_name).alias(col_name))
    avg_ = {}
    col_list = list(df.schema.fields)
    for col_name in col_list:
        avg_[col_name] = 'avg'
    pf_dataset = hashed_dataset.agg(avg_)
    pf_dataset = pf_dataset.select([col(c).alias(
        c.replace('(', '').replace(')', '')) for c in pf_dataset.columns])
    return pf_dataset