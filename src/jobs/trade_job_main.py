import json
import json
import sys
from datetime import datetime

from pyspark.sql.functions import regexp_replace, col, when

from src.trade_transform import postgres_config
from src.trade_transform.tradeDB_fw_ingestion import TradeDBFWIngestion
from src.trade_transform.tradeDB_ingestion import TradeDBIngestion
from src.utils.logger_builder import LoggerBuilder
from src.utils.params import Params
from src.utils.postgresDB_util import Database
from src.utils.python_util import read_excel_pandas, read_csv_pandas
from src.utils.spark_df_util import spark, read_postgres

logger = LoggerBuilder().build()


def update_file_table(status, file_name, env):
    update_sql = 'UPDATE public."Files" SET processstatus = %s where filename = %s'
    up_values = (status, file_name,)
    Database(env).execute_query(update_sql, up_values)


def get_client_config(postgres_tb_name, env):
    logger.info(f"Reading postgres config table to get the client config for staging load")
    config_df = read_postgres(postgres_tb_name, env)
    config_df_with_req_details = config_df.orderBy(col("clientfileconfigid")) \
        .withColumn("hasheader", when((col("hasheader") == False) & (col("headerrownumber") == 0), False)
                    .when((col("hasheader") == False) & (col("headerrownumber") != 0), True).otherwise(True)) \
        .withColumn("srcfilepath", regexp_replace(col("srcfilepath"), '\\\\', '/'))
    config_without_fw_df = config_df_with_req_details.filter(~(col("textdelimiter").isin('fixedwidth', 'FIXEDWIDTH', 'FixedWidth')))\
        .select(col("srcfilepath"), col("filemask"), col("columnstring"), col("hasheader"), col("clientfileconfigid"),
                col("clientid"), col("filetypeid"), col("headerrownumber"), col("textdelimiter"), col("fileextension")) \
        .repartition(20)
    config_with_fw_df = config_df_with_req_details.filter(col("textdelimiter").isin('fixedwidth', 'FIXEDWIDTH', 'FixedWidth'))\
        .select(col("srcfilepath"), col("filemask"), col("hasheader"), col("clientfileconfigid"),
                col("clientid"), col("filetypeid"), col("headerrownumber"), col("datastartingrownumber"),
                col("textdelimiter"), col("fileextension")) \
        .repartition(20)
    config_without_fw_json = config_without_fw_df.toJSON().map(lambda j: json.loads(j)).collect()
    config_with_fw_json = config_with_fw_df.toJSON().map(lambda j: json.loads(j)).collect()
    return config_without_fw_json, config_with_fw_json


def main():
    logger.info(f"Trade DB ETL job has started...")
    # postgres_config.spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
    params = Params(sys.argv)
    env_name = params.get('ENV')
    client_config_json, client_config_fw_json = get_client_config('public."ClientFileConfig_shree"', env_name)
    mapping_tbl_name = params.get('FIXED_WIDTH_TABLE_NAME')
    try:
        start_time = datetime.now()
        trade_s3_processed = TradeDBIngestion(client_config_json)
        trade_s3_processed.process_json(env_name)
        trade_fw_s3_processed = TradeDBFWIngestion(client_config_fw_json, mapping_tbl_name)

        #     for item in client_config_json:
        #         src_dir, src_filename_pattern, schema_names_dict, header_flag, \
        #         src_clientfileconfigid, src_clientid, src_filetypeid, src_header_row_nos, src_delim, src_file_ext = \
        #             item['srcfilepath'], \
        #             item['filemask'], \
        #             ast.literal_eval(
        #                 item['columnstring']), \
        #             item['hasheader'], item[
        #                 'clientfileconfigid'], item[
        #                 'clientid'], item['filetypeid'], \
        #             int(item['headerrownumber']), item['textdelimiter'].strip(), \
        #             item['fileextension']
        #
        #         files_list = create_s3_files_list_with_matching_pattern(src_dir, src_filename_pattern)
        #         data_append = []
        #         processed_file_list = []
        #         for file_path in files_list:
        #             data_df = create_spark_df(file_path, src_file_ext, src_delim, src_header_row_nos)
        #             file_created_by, file_last_modified_date, created_datetime, file_name = get_files_metadata_from_s3(
        #                 file_path)
        #             if 'dat' in src_file_ext:
        #                 as_of_date = extract_as_of_date(file_name)
        #                 data_df = data_df.withColumn("asofdate", lit(as_of_date))
        #                 schema_names_list = list(dict(schema_names_dict.items()).values())
        #                 schema_names_list.append("ASOFDATE")
        #             else:
        #                 schema_names_list = list(dict(sorted(schema_names_dict.items())).values())
        #             data_col_names_list = trim_cols(data_df).columns
        #             insert_query = 'INSERT INTO public."Files" (clientid, filetypeid, filename, filelocation, ' \
        #                            'filecreatedby, filelastmodifieddate, createddatetime, ' \
        #                            'lastupdateddatetime, cleintconfigid) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)'
        #             ins_values = (
        #                 src_clientid, src_filetypeid, file_name, src_dir, file_created_by, file_last_modified_date,
        #                 created_datetime, datetime.today().strftime('%Y-%m-%d %H:%M:%S'), src_clientfileconfigid,)
        #             Database(env_name).execute_query(insert_query, ins_values)
        #
        #             if len(data_col_names_list) == len(schema_names_list):
        #                 if header_flag:
        #                     renamed_with_schema_df = rename_cols(data_df, schema_names_dict)
        #                     drop_unknown_field_df = drop_matching_regex_column(renamed_with_schema_df, ".*unknown*")
        #                     casted_df = cast_date_column(drop_unknown_field_df, ".*date*")
        #                     data_append.append(casted_df)
        #
        #                 elif not header_flag:
        #                     data_with_schema_df = rename_cols(data_df, schema_names_list)
        #                     drop_unknown_field_df = drop_matching_regex_column(data_with_schema_df, ".*unknown*")
        #                     casted_df = cast_date_column(drop_unknown_field_df, ".*date*")
        #                     data_append.append(casted_df)
        #
        #                 update_file_table("InStage", file_name, env_name)
        #                 processed_file_list.append(file_name)
        #             else:
        #                 update_file_table("Rejected", file_name, env_name)
        #                 file_movement(file_path, "Rejected")
        #         if bool(data_append):
        #             final_df = reduce(DataFrame.unionByName, data_append).withColumn("rownum", row_number()
        #                                                                              .over(
        #                 Window.orderBy(monotonically_increasing_id())))
        #             write_postgres(final_df, constants.POSTGRES_TABLES_DICT.get(src_filetypeid), env_name)
        #             for filenames in processed_file_list:
        #                 file_movement(f"{src_dir}{filenames}", "Processed")
        #         else:
        #             logger.info(f"Data appended list is empty because all the files within {files_list} "
        #                         f"are being rejected due schema mismatch")
        logger.info("Trade DB ETL job has completed. " + "Time taken: {} seconds".format(datetime.now() - start_time))
    except Exception as ex:
        print(str(ex))
        raise Exception(str(ex))

    finally:
        spark.stop()


if __name__ == '__main__':
    main()
