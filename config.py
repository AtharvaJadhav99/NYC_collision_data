# config.py

# S3 Configuration
CONFIG_BUCKET = "nyc-collisions-ecc"
CONFIG_FILE_KEY = "config/last_collision_date.json"
LOGS_BUCKET = "nyc-collisions-ecc"
LOGS_KEY_PREFIX = "logs/"
DATA_BUCKET = "nyc-collisions-ecc"
RAW_DATA_PREFIX = "raw_data"
PROCESSED_DATA_PREFIX = "processed_data"

# Socrata API Configuration
BASE_URL = "https://data.cityofnewyork.us/resource/h9gi-nx95.json"
LIMIT = 50000

# Redshift Configuration (Update these values)
REDSHIFT_HOST = "default-workgroup.340752835797.us-east-2.redshift-serverless.amazonaws.com"  # e.g. mycluster.abc123xyz.us-east-1.redshift.amazonaws.com
REDSHIFT_PORT = 5439
REDSHIFT_DB = "dev"
REDSHIFT_USER = "admin"
REDSHIFT_PASSWORD = "XXXXX"
REDSHIFT_IAM_ROLE = "arn:aws:iam::340752835797:role/service-role/AmazonRedshift-CommandsAccessRole-20241204T130545"
REDSHIFT_TABLE = "public.collisions"
