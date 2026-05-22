import os

# API endpoints
JOBS_TABLE = os.getenv("jobs")
JOB_DETAIL_TABLE = os.getenv("job_detail")
AIRBYTE_SERVER = os.getenv("airbyte_server")
DATA_SOURCE_ITEM_TABLE = os.getenv("data_source_item")
output_bucket = os.getenv("Output_bucket_DQ")
aws_access_key_id=os.getenv("aws_access_key_id")
aws_secret_access_key=os.getenv("aws_secret_access_key")
output_bucket_path_dq_noncasino = os.getenv("Output_bucket_path_DQ_NC") # path for non-casino platforms, for example brc, voluum, etc
output_bucket_path_dq_casino = os.getenv("Output_bucket_path_DQ") # path for casino platforms, which is casino-operators/raw_data/