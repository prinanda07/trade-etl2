import ast
import json

from src.jobs.trade_job_main import logger
from src.utils.aws_util import create_s3_files_list_with_matching_pattern
from src.utils.python_util import extract_fixed_width_specs, read_txt_pandas
from src.utils.spark_df_util import get_fixedwidth_details


class TradeDBFWIngestion(object):
    def __init__(self, fw_config_json: json, fw_table_name):
        self.fw_json = fw_config_json
        self.fw_map_table_name = fw_table_name

    def process_fw_json(self, env_name):
        try:
            for fw_item in self.fw_json:
                src_dir, src_filename_pattern, schema_names_dict, header_flag, src_clientfileconfigid, src_clientid, \
                src_filetypeid, src_header_row_nos, src_delim, src_file_ext = \
                    fw_item['srcfilepath'], \
                    fw_item['filemask'], \
                    ast.literal_eval(
                        fw_item['columnstring']), \
                    fw_item['hasheader'], fw_item[
                        'clientfileconfigid'], fw_item[
                        'clientid'], fw_item['filetypeid'], \
                    int(fw_item['headerrownumber']), fw_item['textdelimiter'].strip(), \
                    fw_item['fileextension']
                fixed_width_json = get_fixedwidth_details(self.fw_map_table_name, src_clientfileconfigid, env_name)
                col_specs, col_names = extract_fixed_width_specs(fixed_width_json)
                files_list = create_s3_files_list_with_matching_pattern(src_dir, src_filename_pattern)
                data_append = []
                processed_file_list = []



        except Exception as e:
            logger.error(str(e))
            raise e
