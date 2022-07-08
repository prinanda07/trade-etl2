import json
import re

from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import StringType

from src.trade_transform import postgres_config
from src.trade_transform.postgres_config import *
from src.utils.logger_builder import LoggerBuilder

logger = LoggerBuilder().build()


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
        .option("nullValue", None) \
        .option("escape", "\"") \
        .option("quote", "\"") \
        .csv(input_path)
    return df
    # .option("multiline", "true")


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


# def cast_date_column(input_df: DataFrame, pattern):
#     r = re.compile(pattern)
#     casting_col_list = list(filter(r.match, input_df.columns))
#     for date_col in casting_col_list:
#         input_df = input_df.withColumn(date_col, to_date(col(date_col).cast(StringType()), 'MM/dd/yyyy'))
#     return input_df

def cast_date_column(input_df: DataFrame, pattern):
    r = re.compile(pattern)
    casting_col_list = list(filter(r.match, input_df.columns))
    for date_col in casting_col_list:
        input_df = input_df.withColumn(date_col, col(date_col).cast(StringType()))
    return input_df


# for index, cols in enumerate(data_df.columns):
#     for map_cols in schema_names_list:
#         if cols in map_cols:
#             data_df = data_df.withColumnRenamed(cols, map_cols.lower())
def create_spark_df(src_file_path, src_file_extension, separator, header_row_number):
    postgres_config.spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    if header_row_number > 0:
        actual_header_no = header_row_number - 1
    else:
        actual_header_no = None
    if 'xls' in src_file_extension:
        input_df = read_excel_pandas(src_file_path, engine_name='xlrd', header_row_no=actual_header_no)
    elif 'xlsx' in src_file_extension:
        input_df = read_excel_pandas(src_file_path, engine_name='openpyxl', header_row_no=actual_header_no)
    elif 'csv' in src_file_extension:
        input_df = read_csv_pandas(src_file_path, separator=separator, header_row_no=actual_header_no)
    elif 'dat' in src_file_extension:
        input_df = read_csv_pandas(src_file_path, separator=separator, row_skip=1)
    else:
        logger.info("Not a valid source file extension")
    spark_data_df = spark.createDataFrame(input_df)
    postgres_config.spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
    return spark_data_df


def get_fixedwidth_details(fixedwidth_mapping_tbl_name, filter_value: int, env):
    logger.info(f"Reading postgres {fixedwidth_mapping_tbl_name} table to get the fixed width details")
    mappings_df = read_postgres(fixedwidth_mapping_tbl_name, env)
    sorted_mappings_df = mappings_df.filter(col("client_file_config_id") == filter_value) \
        .sort(col("fixedwidth_field_mapping_id").asc()) \
        .withColumn("start_pos", col("starting_position") - 2).withColumn("end_pos", (
            col("starting_position") + col("length")) - 2)
    selected_mappings_df = sorted_mappings_df.select(col("start_pos"), col('end_pos'), col("field_mapped_to"))
    mappings_json = selected_mappings_df.toJSON().map(lambda j: json.loads(j)).collect()
    return mappings_json
