POSTGRES_CONNECTION_DICT = {'POSTGRES_DRIVER': 'org.postgresql.Driver'}
# 'POSTGRES_ENDPOINT': 'jdbc:postgresql://ca-pgdbserver-instance-1.cjfja5sb3pyi.us-east-2.rds.amazonaws.com:5432/CA_TradeData',
# 'POSTGRES_USER': 'pgadmin',
# 'POSTGRES_PASSWORD': 'Galaxy1234',


POSTGRES_TABLES_DICT = {1: 'public."Raw_Accounts"',
                        2: 'public."Raw_Trades"',
                        3: 'public."Raw_Balances"'}

POSTGRES_ENDPOINT = '/trade/ca_tradedata/rds/{}/master_username'
POSTGRES_USER_NAME = '/trade_db/ca_tradedata/rds/{}/connection_url'
POSTGRES_PASSWORD = '/trade/ca_tradedata/rds/{}/master_password'
S3_SERVICE = "s3"
SSM_SERVICE = "ssm"
REGION_NAME = 'us-east-1'
