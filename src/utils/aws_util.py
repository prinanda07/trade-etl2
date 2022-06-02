import boto3
from botocore.exceptions import ClientError, BotoCoreError

from src.trade_transform.constants import SSM_SERVICE, REGION_NAME


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
