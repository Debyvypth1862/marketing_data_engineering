


import boto3
import ntpath



s3_client = boto3.resource(
    "s3",
    aws_access_key_id="",
    aws_secret_access_key="",
)

def test(name, name1):
    print(name)
bucket_name ="your-rawdata-bucket"
Input_bucket_path = ""
my_bucket = s3_client.Bucket(bucket_name)
# response = s3_client.list_objects(
#         Bucket=bucket_name,
#         Prefix=f"casino-operators/raw_data/Cellxpert/Affiliate_Xe__Casino_Friday_/",
#         MaxKeys = 1000000
#     )
# s3://your-rawdata-bucket/production/Brc/Whitelabel/Operator_1000/modified_date_postback_tracking/
files = [['Brc/Whitelabel/Operator_1000/postback_tracking/2025/03/19/2025-03-19_2025-03-19_15:46:45.json',
'Brc/Whitelabel/Operator_1000/postback_tracking/2025/03/19/2025-03-19_14:46:28.jsonl'],
['Brc/Whitelabel/Operator_1000/postback_tracking/2025/03/19/2025-03-19_2025-03-20_06:54:44.json',
'Brc/Whitelabel/Operator_1000/postback_tracking/2025/03/19/2025-03-19_14:46:27.jsonl'],
['Brc/Whitelabel/Operator_1000/postback_tracking/2025/03/19/2025-03-19_2025-04-24_19:16:16.json',
'Brc/Whitelabel/Operator_1000/postback_tracking/2025/03/19/2025-03-19_14:46:29.jsonl'],
['Brc/Whitelabel/Operator_1000/postback_tracking/2025/03/20/2025-03-20_2025-03-20_06:55:13.json',
'Brc/Whitelabel/Operator_1000/postback_tracking/2025/03/20/2025-03-20_14:46:32.jsonl'],
['Brc/Whitelabel/Operator_1000/postback_tracking/2025/03/20/2025-03-20_2025-03-20_14:25:38.json',
'Brc/Whitelabel/Operator_1000/postback_tracking/2025/03/20/2025-03-20_14:46:34.jsonl'],
['Brc/Whitelabel/Operator_1000/postback_tracking/2025/03/20/2025-03-20_2025-04-24_19:20:48.json',
'Brc/Whitelabel/Operator_1000/postback_tracking/2025/03/20/2025-03-20_14:46:35.jsonl'],
['Brc/Whitelabel/Operator_1000/postback_tracking/2025/03/25/2025-03-25_2025-04-24_19:11:44.json',
'Brc/Whitelabel/Operator_1000/postback_tracking/2025/03/25/2025-03-25_14:46:38.jsonl'],
['Brc/Whitelabel/Operator_1000/postback_tracking/2025/03/26/2025-03-26_2025-04-24_19:22:16.json',
'Brc/Whitelabel/Operator_1000/postback_tracking/2025/03/26/2025-03-26_14:46:43.jsonl'],
['Brc/Whitelabel/Operator_1000/postback_tracking/2025/03/27/2025-03-27_2025-04-24_19:08:17.json',
'Brc/Whitelabel/Operator_1000/postback_tracking/2025/03/27/2025-03-27_14:46:49.jsonl'],
['Brc/Whitelabel/Operator_1000/postback_tracking/2025/03/28/2025-03-28_2025-04-24_18:58:14.json',
'Brc/Whitelabel/Operator_1000/postback_tracking/2025/03/28/2025-03-28_14:46:54.jsonl'],
['Brc/Whitelabel/Operator_1000/postback_tracking/2025/03/29/2025-03-29_2025-04-24_19:10:10.json',
'Brc/Whitelabel/Operator_1000/postback_tracking/2025/03/29/2025-03-29_14:47:00.jsonl'],
['Brc/Whitelabel/Operator_1000/postback_tracking/2025/03/30/2025-03-30_2025-04-24_18:49:23.json',
'Brc/Whitelabel/Operator_1000/postback_tracking/2025/03/30/2025-03-30_14:47:09.jsonl']]  # Initialize an empty list to store file keys
    # Iterate through the contents of the response
print(test(f"dbt run-operation stage_external_sources --vars \"ext_data_refresh: true\"","dfdfdf"))

# for file in my_bucket.objects.filter(Prefix=f"production/Brc/Whitelabel/Operator_1000/modified_date_postback_tracking/"):

#     s3_client.Object(bucket_name,file.key.replace('1000','1001')).copy_from(CopySource=f"{bucket_name}/{file.key}")
#     s3_client.Object(bucket_name,file.key).delete()
    # old_file_full_path = file.key
    # old_file_name = ntpath.basename(old_file_full_path)
    # transaction_date = old_file_name[:10]
    # emitted_time = file.last_modified.strftime("%H:%M:%S")
    # emitted_date = file.last_modified.strftime("%Y-%m-%d")


    # print("Tessttttttttttttt")

    # print(f"old file path {old_file_full_path}")
    # if '-' in transaction_date and len(old_file_name) < 25:
    #     if 'json' in old_file_name and not 'Brc' in old_file_name:
    #         new_file_full_path = old_file_full_path.replace(old_file_name,f"{transaction_date}_{emitted_date}_{emitted_time}.json")
    #         s3_client.Object(bucket_name,new_file_full_path).copy_from(CopySource=f"{bucket_name}/{old_file_full_path}")
    #         s3_client.Object(bucket_name,old_file_full_path).delete()
    #         print(f"op 1 :{old_file_full_path}")
    #         print(f"op 1 :{new_file_full_path}")'

    # if len(old_file_name) > 27:
    #     if 'json' in old_file_name and not 'postback_tracking' in old_file_full_path and not 'postback_3rd_party_click_log' in old_file_full_path :
    #         if 'raw_data' in old_file_name:
    #             new_file_full_path = old_file_full_path.replace(old_file_name,f"{emitted_date}_{emitted_time}.jsonl")
    #             s3_client.Object(bucket_name,new_file_full_path).copy_from(CopySource=f"{bucket_name}/{old_file_full_path}")
    #             s3_client.Object(bucket_name,old_file_full_path).delete()
    #             print(f"op 2 :{old_file_full_path}")
    #             print(f"op 2 :{new_file_full_path}")
    #         else:
    #             new_file_full_path = old_file_full_path.replace(old_file_name,f"{transaction_date}_{emitted_time}.jsonl")
    #             s3_client.Object(bucket_name,new_file_full_path).copy_from(CopySource=f"{bucket_name}/{old_file_full_path}")
    #             s3_client.Object(bucket_name,old_file_full_path).delete()
    #             print(f"op 3 :{old_file_full_path}")
    #             print(f"op 3 :{new_file_full_path}")
    # break
    # if "1042" in file_name:
    #     print(f'file:{file_name}')
        # files.append(file.get("Key"))

