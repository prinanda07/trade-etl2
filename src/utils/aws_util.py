import boto3
from botocore.exceptions import ClientError, BotoCoreError

from src.trade_transform.constants import SSM_SERVICE, REGION_NAME, S3_SERVICE


def get_parameter_from_ssm(parameter_name: str) -> str:
    try:
        ssm_client = get_service_client(SSM_SERVICE)
        return ssm_client.get_parameter(Name=parameter_name, WithDecryption=True)["Parameter"]["Value"]
    except ClientError as e:
        pass
    except BotoCoreError as e:
        pass


def get_service_client(service_name: str) -> boto3.client:
    try:
        return boto3.client(service_name, region_name=REGION_NAME)
    except ClientError as e:
        pass
    except BotoCoreError as e:
        pass


def create_s3_files_list_with_matching_pattern(src_file_path, src_file_mask):
    s3_files_path_list = []
    bucket_name = src_file_path.split("/")[2]
    client_name = get_service_client(S3_SERVICE)
    prefix = f"{src_file_path.split('/')[3]}/{src_file_path.split('/')[4]}/"
    pattern = src_file_mask.split("*")[0]
    result = client_name.list_objects(Bucket=bucket_name, Prefix=prefix, Delimiter='/')
    for i in result['Contents']:
        if pattern in i['Key']:
            s3_path = f"s3://{bucket_name}/{i['Key']}"
            s3_files_path_list.append(s3_path)
    return s3_files_path_list


def get_files_metadata_from_s3(file_path: str):
    client_name = get_service_client(S3_SERVICE)
    bucket_name = file_path.split("/")[2]
    prefix = f"{file_path.split('/')[3]}/{file_path.split('/')[4]}/{file_path.split('/')[5]}"
    result = client_name.list_objects(Bucket=bucket_name, Prefix=prefix, Delimiter='/')
    file_created_by = result['Contents'][0]['Owner'].get('DisplayName')
    file_last_modified_datetime = result['Contents'][0]['LastModified'].strftime('%Y-%m-%d %H:%M:%S')
    file_created_datetime = result['Contents'][0]['LastModified'].strftime('%Y-%m-%d %H:%M:%S')
    file_name = file_path.split('/')[5]
    return file_created_by, file_last_modified_datetime, file_created_datetime, file_name


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
