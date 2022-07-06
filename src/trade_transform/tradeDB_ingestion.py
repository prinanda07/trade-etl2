import ast
import json
from datetime import datetime
from functools import reduce

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import lit, row_number, monotonically_increasing_id

from src.jobs.trade_job_main import create_spark_df, update_file_table, logger
from src.trade_transform import constants
from src.utils.aws_util import create_s3_files_list_with_matching_pattern, file_movement, get_files_metadata_from_s3
from src.utils.postgresDB_util import Database
from src.utils.python_util import extract_as_of_date
from src.utils.spark_df_util import trim_cols, rename_cols, drop_matching_regex_column, cast_date_column, write_postgres


class TradeDBIngestion(object):
    def __init__(self, config_json: json):
        self.client_conf_json = config_json

    def process_json(self, env_name):
        try:
            for item in self.client_conf_json:
                src_dir, src_filename_pattern, schema_names_dict, header_flag, \
                src_clientfileconfigid, src_clientid, src_filetypeid, src_header_row_nos, src_delim, src_file_ext = \
                    item['srcfilepath'], \
                    item['filemask'], \
                    ast.literal_eval(
                        item['columnstring']), \
                    item['hasheader'], item[
                        'clientfileconfigid'], item[
                        'clientid'], item['filetypeid'], \
                    int(item['headerrownumber']), item['textdelimiter'].strip(), \
                    item['fileextension']

                files_list = create_s3_files_list_with_matching_pattern(src_dir, src_filename_pattern)
                data_append = []
                processed_file_list = []
                for file_path in files_list:
                    data_df = create_spark_df(file_path, src_file_ext, src_delim, src_header_row_nos)
                    file_created_by, file_last_modified_date, created_datetime, file_name = get_files_metadata_from_s3(
                        file_path)
                    if 'dat' in src_file_ext:
                        as_of_date = extract_as_of_date(file_name)
                        data_df = data_df.withColumn("asofdate", lit(as_of_date))
                        schema_names_list = list(dict(schema_names_dict.items()).values())
                        schema_names_list.append("ASOFDATE")
                    else:
                        schema_names_list = list(dict(sorted(schema_names_dict.items())).values())
                    data_col_names_list = trim_cols(data_df).columns
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
                            drop_unknown_field_df = drop_matching_regex_column(renamed_with_schema_df, ".*unknown*")
                            casted_df = cast_date_column(drop_unknown_field_df, ".*date*")
                            data_append.append(casted_df)

                        elif not header_flag:
                            data_with_schema_df = rename_cols(data_df, schema_names_list)
                            drop_unknown_field_df = drop_matching_regex_column(data_with_schema_df, ".*unknown*")
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
                        file_movement(f"{src_dir}{filenames}", "Processed")
                else:
                    logger.info(f"Data appended list is empty because all the files within {files_list} "
                                f"are being rejected due schema mismatch")
        except Exception as e:
            logger.error(str(e))
            raise e
