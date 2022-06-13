POSTGRES_CONNECTION_DICT = {'POSTGRES_DRIVER': 'org.postgresql.Driver'}
POSTGRES_TABLES_DICT = {1: 'public."Raw_Accounts"',
                        2: 'public."Raw_Trades"',
                        3: 'public."Raw_Balances"'}

POSTGRES_ENDPOINT = '/trade_db/ca_tradedata/rds/{}/connection_url'
POSTGRES_USER_NAME = '/trade/ca_tradedata/rds/{}/master_username'
POSTGRES_PASSWORD = '/trade/ca_tradedata/rds/{}/master_password'
S3_SERVICE = "s3"
SSM_SERVICE = "ssm"
REGION_NAME = 'us-east-2'
