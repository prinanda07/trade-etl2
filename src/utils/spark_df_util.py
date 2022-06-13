import re

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_date

from src.trade_transform.postgres_config import *


def read_postgres(table_name, env):
    read_data = spark.read.format("jdbc") \
        .option("url", get_postgres_url(env)) \
        .option("dbtable", table_name) \
        .option("user", get_postgres_user(env)) \
        .option("password", get_postgres_password(env)) \
        .option("fetchsize", 1000) \
        .option("driver", get_postgres_driver()).load()
    return read_data


def write_postgres(data_frame, table_name, env, modes="append"):
    data_frame.write.format("jdbc").mode(modes) \
        .option("url", get_postgres_url(env)) \
        .option("dbtable", table_name) \
        .option("user", get_postgres_user(env)) \
        .option("password", get_postgres_password(env)) \
        .option("driver", get_postgres_driver()).save()


def read_csv_spark(input_path, sep="|", header="true"):
    df = spark.read \
        .option("sep", sep) \
        .option("header", header) \
        .option("inferschema", "true") \
        .option("quote", "\"") \
        .option("nullValue", None)\
        .option("escape", "\"") \
        .csv(input_path)
    return df


def rename_cols(df, mappings):
    if isinstance(mappings, list):
        for index, cols in enumerate(df.columns):
            df = df.withColumnRenamed(cols, mappings[index].lower())
    elif isinstance(mappings, dict):
        for cols in df.columns:
            value = mappings.get(cols.strip())
            df = df.withColumnRenamed(cols, value.lower())
    return df


def trim_cols(df):
    for cols in df.columns:
        df = df.withColumnRenamed(cols, cols.strip())
    return df


def drop_matching_regex_column(input_df: DataFrame, pattern):
    r = re.compile(pattern)
    to_drop_cols_list = list(filter(r.match, input_df.columns))
    dropped_col_df = input_df.drop(*to_drop_cols_list)
    return dropped_col_df


def cast_date_column(input_df: DataFrame, pattern):
    r = re.compile(pattern)
    casting_col_list = list(filter(r.match, input_df.columns))
    for date_col in casting_col_list:
        input_df = input_df.withColumn(date_col, to_date(col(date_col), 'MM/dd/yyyy'))
    return input_df
