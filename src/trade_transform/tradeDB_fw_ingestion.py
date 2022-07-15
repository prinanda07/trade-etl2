import json
from datetime import datetime
from functools import reduce

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import row_number, monotonically_increasing_id, col, lit

from src.trade_transform import constants, postgres_config
from src.utils.aws_util import create_s3_files_list_with_matching_pattern, get_files_metadata_from_s3, file_movement
from src.utils.postgresDB_util import Database
from src.utils.python_util import extract_fixed_width_specs, read_txt_pandas, update_file_table
from src.utils.spark_df_util import get_fixed_width_details, write_postgres, logger, read_postgres


class TradeDBFWIngestion(object):
    def __init__(self, fw_config_json: json, fw_table_name):
        self.fw_json = fw_config_json
        self.fw_map_table_name = fw_table_name

    def process_fw_json(self, env_name):
        try:
            postgres_config.spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
            for fw_item in self.fw_json:
                src_dir, src_filename_pattern, skip_row_num, header_flag, src_clientfileconfigid, src_clientid, \
                src_filetypeid, src_header_row_nos, src_delim, src_file_ext = \
                    fw_item['srcfilepath'], \
                    fw_item['filemask'], \
                    fw_item['datastartingrownumber'], \
                    fw_item['hasheader'], fw_item[
                        'clientfileconfigid'], fw_item[
                        'clientid'], fw_item['filetypeid'], \
                    int(fw_item['headerrownumber']), fw_item['textdelimiter'].strip(), \
                    fw_item['fileextension']

                files_list = create_s3_files_list_with_matching_pattern(src_dir, src_filename_pattern)
                fixed_width_json = get_fixed_width_details(self.fw_map_table_name, src_clientfileconfigid, env_name)
                if bool(fixed_width_json):
                    col_specs, col_names = extract_fixed_width_specs(fixed_width_json)
                    data_append = []
                    processed_file_list = []
                    for file_path in files_list:
                        fw_data_df = read_txt_pandas(file_path, skip_rows=skip_row_num - 1, col_specs=col_specs,
                                                     col_name=col_names)
                        spark_fw_data_df = postgres_config.spark.createDataFrame(fw_data_df)
                        file_created_by, file_last_modified_date, created_datetime, file_name = get_files_metadata_from_s3(
                            file_path)
                        insert_query = 'INSERT INTO public."Files" (clientid, filetypeid, filename, filelocation, ' \
                                       'filecreatedby, filelastmodifieddate, createddatetime, ' \
                                       'lastupdateddatetime, cleintconfigid) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)'
                        ins_values = (
                            src_clientid, src_filetypeid, file_name, src_dir, file_created_by, file_last_modified_date,
                            created_datetime, datetime.today().strftime('%Y-%m-%d %H:%M:%S'), src_clientfileconfigid,)
                        Database(env_name).execute_query(insert_query, ins_values)
                        files_id = \
                            read_postgres('public."Files"', env_name).filter(col("filename") == file_name).select(
                                col('fileid')).collect()[0].asDict()['fileid']
                        id_df = spark_fw_data_df.withColumn("fileid", lit(files_id)) \
                            .withColumn("clientid", lit(src_clientid))
                        data_append.append(id_df)
                        update_file_table("InStage", file_name, env_name)
                        processed_file_list.append(file_name)
                    if bool(data_append):
                        final_df = reduce(DataFrame.unionByName, data_append).withColumn("rownum", row_number()
                                                                                         .over(
                            Window.orderBy(monotonically_increasing_id())))
                        write_postgres(final_df, constants.POSTGRES_TABLES_DICT.get(src_filetypeid), env_name)
                        for filenames in processed_file_list:
                            file_movement(f"{src_dir}{filenames}", "Processed")
                    else:
                        logger.info(f"Data append list is empty!..Please verify all the fixed width files")
                else:
                    logger.info(f"Rejected Files list--> {files_list} "
                                f"due unavailability of records in {self.fw_map_table_name} table")
                    for path in files_list:
                        file_created_by, file_last_modified_date, created_datetime, rejected_file_name = get_files_metadata_from_s3(
                            path)
                        insert_query = 'INSERT INTO public."Files" (clientid, filetypeid, filename, filelocation, ' \
                                       'filecreatedby, filelastmodifieddate, createddatetime, ' \
                                       'lastupdateddatetime, cleintconfigid) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)'
                        ins_values = (
                            src_clientid, src_filetypeid, rejected_file_name, src_dir, file_created_by,
                            file_last_modified_date,
                            created_datetime, datetime.today().strftime('%Y-%m-%d %H:%M:%S'), src_clientfileconfigid,)
                        Database(env_name).execute_query(insert_query, ins_values)
                        update_file_table("Rejected", rejected_file_name, env_name)
                        file_movement(path, "Rejected")
        except Exception as e:
            logger.error(str(e))
            raise e
