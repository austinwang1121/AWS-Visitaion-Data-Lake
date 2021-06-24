S#import json
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
import time

def lambda_handler(event, context):
    now = datetime.now()
    seed(now)
    s3 = boto3.client("s3")
    bucket_name = 'log-data-structured-new'


    def generate_S3_data():
        file_name = str(datetime.now()).replace(' ', '_').replace('.','_').replace(':', '_').replace('-', '_') + ".json"
        lambda_path = '/tmp/' + file_name

        # migration_id = "migration-" + str(randint(0,50)).zfill(2)
    
        # container_id = "container-" + str(randint(0,100)).zfill(3)
        # transfer_id = "transfer-" + str(randint(0,1000)).zfill(4)
        migration_id="migration-51"
        container_id="container-51"
        transfer_id="transfer-51"
        s3_path = "migrationid="+migration_id +  '/' + "containerid=" + container_id + '/' + "transferid=" + transfer_id + '/' + file_name        

        
        data_list = []
        for i in range(0, randint(5000,5000)):
            timestamp = "2019-08-" + str(randint(1,30)).zfill(2) + " " + str(randint(0,24)).zfill(2) + ":" + str(randint(0,60)).zfill(2) + ":" + str(randint(0,60)).zfill(2)
            level = choice(["ERROR", "INFO", "WARNING"])
            data_type = choice(["InMigration", "PostMigrationAction", "PostMigrationReport", "PreMigrationReport"])
            data_class = choice(["PII", "UGC", "ATLASSIAN"])
            message=data_type + "-" + data_class + "-" + level + " " + "For" + migration_id + " " + container_id + " " + transfer_id
            datum = {"migrationId": migration_id, "containerId": container_id, "transferId": transfer_id, "dataClass": data_class, "createdDatetime":timestamp, "level":level, "dataType":data_type, "message": message}
            data_list.append(datum)
        
        with open(lambda_path, 'w') as f:
            for point in data_list:
                f.write(json.dumps(point))
                f.write("\n")
        
        s3.upload_file(lambda_path, bucket_name, s3_path)
        print("file:" + file_name + " is uploaded to " + s3_path)
    
    num = 1000
    for _ in range(num):
        #code    
        generate_S3_data()

        
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
