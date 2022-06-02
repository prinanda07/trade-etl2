import glob
import os
import re
from datetime import datetime

import boto3

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


def file_movement(src_path, target_folder):
    s3 = boto3.resource('s3')
    client_name = boto3.client('s3')
    src_dir = src_path.split("/")
    bucket_name = src_dir[2]
    file_name = src_dir[5]
    sub_dir = f"{src_dir[3]}/{src_dir[4]}"
    prefix = f"{sub_dir}/{file_name}"
    result = client_name.list_objects(Bucket=bucket_name, Prefix=prefix, Delimiter='/')
    key = result['Contents'][0]['Key']
    copy_source = {'Bucket': bucket_name, 'Key': key}
    target_filename = f"{sub_dir}/{target_folder}/{file_name}"
    s3.meta.client.copy(copy_source, bucket_name, target_filename)
    s3.Object(bucket_name, key).delete()
