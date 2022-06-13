import ast
import json
import sys
from datetime import datetime
from functools import reduce

from pyspark.sql import Window, DataFrame
from pyspark.sql.functions import regexp_replace, col, row_number, monotonically_increasing_id

from src.trade_transform import constants
from src.utils.aws_util import create_s3_files_list_with_matching_pattern, get_files_metadata_from_s3, file_movement
from src.utils.logger_builder import LoggerBuilder
from src.utils.params import Params
from src.utils.postgresDB_util import Database
from src.utils.spark_df_util import read_csv_spark, trim_cols, rename_cols, spark, read_postgres, \
    drop_matching_regex_column, \
    write_postgres, cast_date_column

logger = LoggerBuilder().build()


def update_file_table(status, file_name, env):
    update_sql = 'UPDATE public."Files" SET processstatus = %s where filename = %s'
    up_values = (status, file_name,)
    Database(env).execute_query(update_sql, up_values)


def get_client_config(postgres_tb_name, env):
    logger.info(f"Reading postgres config table to get the client config for staging load")
    config_df = read_postgres(postgres_tb_name, env)
    config_df_with_req_details = config_df.orderBy(col("clientfileconfigid")) \
        .withColumn("srcfilepath", regexp_replace(col("srcfilepath"), '\\\\', '/')) \
        .select(col("srcfilepath"), col("filemask"), col("columnstring"), col("hasheader"), col("clientfileconfigid"),
                col("clientid"), col("filetypeid")) \
        .repartition(20)
    config_df_json = config_df_with_req_details.toJSON().map(lambda j: json.loads(j)).collect()
    return config_df_json


def main():
    logger.info(f"Trade DB ETL job has started...")
    params = Params(sys.argv)
    env_name = params.get('ENV')
    client_config_json = get_client_config('pubic."ClientFileConfig_shree"', env_name)
    try:
        start_time = datetime.now()
        for item in client_config_json:
            src_dir, src_filename_pattern, schema_names_dict, \
            header_flag, src_clientfileconfigid, src_clientid, src_filetypeid = item['srcfilepath'], \
                                                                                item['filemask'], \
                                                                                ast.literal_eval(
                                                                                    item['columnstring']), \
                                                                                item['hasheader'], item[
                                                                                    'clientfileconfigid'], item[
                                                                                    'clientid'], item['filetypeid']

            files_list = create_s3_files_list_with_matching_pattern(src_dir, src_filename_pattern)
            data_append = []
            processed_file_list = []
            for file_path in files_list:
                data_df = read_csv_spark(file_path, ",", header_flag)
                schema_names_list = list(dict(sorted(schema_names_dict.items())).values())
                data_col_names_list = trim_cols(data_df).columns
                file_created_by, file_last_modified_date, created_datetime, file_name = get_files_metadata_from_s3(
                    file_path)
                insert_query = 'INSERT INTO public."Files" (clientid, filetypeid, filename, filelocation, ' \
                               'filecreatedby, filelastmodifieddate, createddatetime, ' \
                               'lastupdateddatetime, cleintconfigid) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)'
                ins_values = (
                    src_clientid, src_filetypeid, file_name, src_dir, file_created_by, file_last_modified_date,
                    created_datetime, datetime.today().strftime('%Y-%m-%d %H:%M:%S'), src_clientfileconfigid,)
                Database(env_name).execute_query(insert_query, ins_values)

                if len(data_col_names_list) == len(schema_names_list):
                    if header_flag:
                        renamed_with_schema_df = rename_cols(data_df, schema_names_dict)
                        drop_unknown_field_df = drop_matching_regex_column(renamed_with_schema_df, ".*unknownfield*")
                        casted_df = cast_date_column(drop_unknown_field_df, ".*date*")
                        data_append.append(casted_df)

                    elif not header_flag:
                        data_with_schema_df = rename_cols(data_df, schema_names_list)
                        drop_unknown_field_df = drop_matching_regex_column(data_with_schema_df, ".*unknownfield*")
                        casted_df = cast_date_column(drop_unknown_field_df, ".*date*")
                        data_append.append(casted_df)

                    update_file_table("InStage", file_name, env_name)
                    processed_file_list.append(file_name)
                else:
                    update_file_table("Rejected", file_name, env_name)
                    file_movement(file_path, "Rejected")
            if bool(data_append):
                final_df = reduce(DataFrame.unionByName, data_append).withColumn("rownum", row_number()
                                                                                 .over(
                    Window.orderBy(monotonically_increasing_id())))
                write_postgres(final_df, constants.POSTGRES_TABLES_DICT.get(src_filetypeid), env_name)
                for filenames in processed_file_list:
                    file_movement(f"{src_dir}/{filenames}", "Processed")
            else:
                logger.info(f"Data appended list is empty because all the files within {files_list} "
                            f"are being rejected due schema mismatch")
        logger.info("Trade DB ETL job has completed. " + "Time taken: {} seconds".format(datetime.now() - start_time))
    except Exception as ex:
        print(str(ex))
        raise Exception(str(ex))

    finally:
        spark.stop()


if __name__ == '__main__':
    main()
