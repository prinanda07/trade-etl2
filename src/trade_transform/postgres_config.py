from awsglue.context import GlueContext
from pyspark.context import SparkContext
from src.trade_transform import constants
from src.utils.aws_util import get_parameter_from_ssm

glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session


def get_postgres_url(env):
    return get_parameter_from_ssm(constants.POSTGRES_ENDPOINT.format(env))


def get_postgres_user(env):
    return get_parameter_from_ssm(constants.POSTGRES_USER_NAME.format(env))


def get_postgres_password(env):
    return get_parameter_from_ssm(constants.POSTGRES_PASSWORD.format(env))


def get_postgres_driver():
    return constants.POSTGRES_CONNECTION_DICT.get("POSTGRES_DRIVER")
