import json
import sys
from datetime import datetime

from pyspark.sql.functions import regexp_replace, col, when

from src.trade_transform.tradeDB_fw_ingestion import TradeDBFWIngestion
from src.trade_transform.tradeDB_ingestion import TradeDBIngestion
from src.utils.logger_builder import LoggerBuilder
from src.utils.params import Params
from src.utils.postgresDB_util import Database
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
    config_without_fw_df = config_df_with_req_details.filter(
        ~(col("textdelimiter").isin('fixedwidth', 'FIXEDWIDTH', 'FixedWidth'))) \
        .select(col("srcfilepath"), col("filemask"), col("columnstring"), col("hasheader"), col("clientfileconfigid"),
                col("clientid"), col("filetypeid"), col("headerrownumber"), col("textdelimiter"), col("fileextension")) \
        .repartition(20)
    config_with_fw_df = config_df_with_req_details.filter(
        col("textdelimiter").isin(['fixedwidth', 'FIXEDWIDTH', 'FixedWidth'])) \
        .select(col("srcfilepath"), col("filemask"), col("hasheader"), col("clientfileconfigid"),
                col("clientid"), col("filetypeid"), col("headerrownumber"), col("datastartingrownumber"),
                col("textdelimiter"), col("fileextension")) \
        .repartition(20)
    config_without_fw_json = config_without_fw_df.toJSON().map(lambda j: json.loads(j)).collect()
    config_with_fw_json = config_with_fw_df.toJSON().map(lambda j: json.loads(j)).collect()
    return config_without_fw_json, config_with_fw_json


def main():
    logger.info(f"Trade DB ETL job has started...")
    params = Params(sys.argv)
    env_name = params.get('ENV')
    client_config_json, client_config_fw_json = get_client_config('public."ClientFileConfig_shree"', env_name)
    mapping_tbl_name = params.get('FIXED_WIDTH_TABLE_NAME')
    try:
        start_time = datetime.now()
        trade_s3_processed = TradeDBIngestion(client_config_json)
        trade_s3_processed.process_json(env_name)
        trade_fw_s3_processed = TradeDBFWIngestion(client_config_fw_json, mapping_tbl_name)
        trade_fw_s3_processed.process_fw_json(env_name)
        logger.info("Trade DB ETL job has completed. " + "Time taken: {} seconds".format(datetime.now() - start_time))
    except Exception as ex:
        print(str(ex))
        raise Exception(str(ex))

    finally:
        spark.stop()


if __name__ == '__main__':
    main()
