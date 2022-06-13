import glob
import os
import re
from datetime import datetime

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



