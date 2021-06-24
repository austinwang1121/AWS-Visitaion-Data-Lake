import json
import urllib.request
import boto3
from io import BytesIO
from gzip import GzipFile
from random import randint
# generate random integer values
from random import seed
from random import choice
# seed random number generator
from datetime import datetime

def lambda_handler(event, context):
    now = datetime.now()
    seed(now)

    random_migration = ["migration-1", "migration-2", "migration-3"]
    random_container = ["container-a", "container-b", "container-c", "null"]
    random_transfer = ["transfer-i", "transfer-ii", "transfer-iii", "null"]
    migration_id = choice(random_migration)
    container_id = choice(random_container)
    transfer_id = choice(random_transfer)
    if container_id == "null":
            transfer_id = "null"

    
    bucket_name = 'log-data-structured'

    
    file_name = now_string.replace('.','_')
    file_name = file_name.replace(':', '_')
    file_name = file_name.replace('-', '_')
    file_name += '.json'
    lambda_path = '/tmp/' + file_name
    s3_path = migration_id +  '/' + container_id + '/' + transfer_id + '/' + file_name        
    
    def generate_data():
        random_level = ["ERROR", "INFO", "WARNING"]
        random_data_class = ["PII", "UGC", "ATLASSIAN"]
        random_data_type = ["InMigration", "PostMigrationAction", "PostMigrationReport", "PreMigrationReport"]
        random_datetime = [["2019-08-13 15:10:27 +0000", "2019-08-13 15:15:27 +0000"], ["2019-08-13 15:07:27 +0000", "2019-08-13 15:12:27 +0000"], ["2019-08-13 15:09:27 +0000", "2019-08-13 15:14:27 +0000"], ["2019-08-13 15:04:27 +0000", "2019-08-13 15:09:27 +0000"], ["2019-08-13 15:02:27 +0000", "2019-08-13 15:07:27 +0000"], ["2019-08-13 15:12:27 +0000", "2019-08-13 15:17:27 +0000"], ["2019-08-13 15:05:27 +0000", "2019-08-13 15:10:27 +0000"], ["2019-08-13 15:05:27 +0000", "2019-08-13 15:10:27 +0000"], ["2019-08-13 15:05:27 +0000", "2019-08-13 15:10:27 +0000"], ["2019-08-13 15:05:27 +0000", "2019-08-13 15:10:27 +0000"]]
        data_list = []
        
        for i in random.randint(10,20):
            timestamp = choice(choice(random_datetime))
            level = choice(random_level)
            data_type = choice(random_data_type)
            data_class = choice(random_data_class)
            message=level + ": " + "log about " + migration_id + " " + container_id + " " + transfer_id
            datum = {"migrationId": migration_id, "containerId": container_id, "transferId": transfer_id, "dataClass": data_class, "createdDatetime":timestamp, "level":level, "dataType":data_type, "message": message}
            data_list.append(datum)
        return data_list


    s3 = boto3.client("s3")
    
    
    with open(lambda_path, 'w') as f:
        for point in generate_data():
            f.write(json.dumps(point))
            f.write("\n")
    
    s3.upload_file(lambda_path, bucket_name, s3_path)
        
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
