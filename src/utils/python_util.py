import glob
import json
import os
import re
from datetime import datetime

import pandas as pd

from src.utils.logger_builder import LoggerBuilder

logger = LoggerBuilder().build()


def create_files_list_with_matching_pattern(src_file_path, src_file_mask):
    files_list = glob.glob(f"{src_file_path}{src_file_mask}")
    return files_list


def extract_file_metadata(file_path: str):
    r = re.compile(".*.csv")
    file_name = list(filter(r.match, file_path.split("\\")))[0]
    stats = os.stat(file_path)
    return stats.st_uid, convert_unix_ts_to_formatted_ts(stats.st_mtime), \
           convert_unix_ts_to_formatted_ts(stats.st_ctime), file_name


def convert_unix_ts_to_formatted_ts(epoch_time):
    return datetime.utcfromtimestamp(epoch_time).strftime('%Y-%m-%d %H:%M:%S')


# 'openpyxl'
def read_excel_pandas(src_path, engine_name='xlrd', header_row_no=None):
    excel_df = pd.read_excel(src_path, engine=engine_name, header=header_row_no)
    return excel_df.dropna().reset_index(drop=True)


def read_csv_pandas(src_path, separator=',', header_row_no=None, row_skip=None):
    csv_df = pd.read_csv(src_path, delimiter=separator, header=header_row_no, skiprows=row_skip)
    return csv_df.dropna().reset_index(drop=True)


def extract_as_of_date(file_name: str):
    r = re.compile("^20")
    names_list = list(filter(r.match, file_name.split(".")))
    as_of_date = names_list[0].split("_")[0]
    return as_of_date


def read_txt_pandas(src_path, header_row_no=None, skip_rows=None, col_specs='infer', fixed_widths=None, col_name=None):
    txt_df = pd.read_fwf(src_path, header=header_row_no, skiprows=skip_rows, colspecs=col_specs, widths=fixed_widths,
                         names=col_name)
    txt_without_nan_df = txt_df.dropna().reset_index(drop=True)
    return txt_without_nan_df


def extract_fixed_width_specs(mapping_json: json):
    col_specs = []
    col_names = []
    for map_cols in mapping_json:
        pos_tuple = (int(map_cols["start_pos"]), int(map_cols["end_pos"]))
        col_specs.append(pos_tuple)
        col_names.append(map_cols["field_mapped_to"])
    return col_specs, col_names
