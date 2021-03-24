import os
import pandas as pd

import boto3
from botocore.exceptions import ClientError
from botocore.config import Config
from utils import credentials as cred
from utils.slack import slack_message
from datetime import datetime


class S3Bucket:
    S3_BUCKET_NAME = cred.S3_BUCKET_NAME
    S3_IAM_ROLE = cred.S3_IAM_ROLE
    S3_DELIMITER = cred.S3_DELIMITER

    my_config = Config(
        region_name='eu-west-1'
    )

    s3_client = boto3.client(
        's3',
        aws_access_key_id=cred.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=cred.AWS_SECRET_ACCESS_KEY,
        config=my_config
    )

    s3_resource = boto3.resource(
        's3',
        aws_access_key_id=cred.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=cred.AWS_SECRET_ACCESS_KEY,
        config=my_config
    )

    def __init__(self):
        self.S3_BUCKET_NAME = cred.S3_BUCKET_NAME
        self.S3_IAM_ROLE = cred.S3_IAM_ROLE
        self.S3_DELIMITER = cred.S3_DELIMITER

        self.my_config = Config(
            region_name='eu-west-1'
        )
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=cred.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=cred.AWS_SECRET_ACCESS_KEY,
            config=self.my_config
        )

        self.s3_resource = boto3.resource(
            's3',
            aws_access_key_id=cred.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=cred.AWS_SECRET_ACCESS_KEY,
            config=self.my_config
        )

    def only_local_save_csv(self, df: pd.DataFrame, file_name):
        # save csv to local file
        local_file_name = os.path.join(file_name)
        df.to_csv(local_file_name, index=False, sep='\t', encoding='utf-8')
        return 1

    def upload_file(self, s3_file_name: str, s3_folder: str, local_file_name: str):
        try:
            response = self.s3_client.upload_file(
                local_file_name, self.S3_BUCKET_NAME, s3_folder + '/' + s3_file_name
            )
        except ClientError as e:
            return 0
        return 1

    def save_csv(self, df: pd.DataFrame, s3_file_name: str, s3_folder):
        # save csv to local file
        local_file_name = os.path.join(s3_file_name)
        df.to_csv(local_file_name, index=False, sep=self.S3_DELIMITER, encoding='utf-8')

        # upload the file to s3
        salida = self.upload_file(s3_file_name=s3_file_name, s3_folder=s3_folder, local_file_name=local_file_name)
        if salida == 0:
            return 0

        os.remove(local_file_name)
        return 1

    def list_folders(self, folder_url):
        bucket_list = self.s3_client.list_objects_v2(Bucket=self.S3_BUCKET_NAME)
        lista = []
        for my_bucket_object in bucket_list['Contents']:
            if folder_url != '':
                if my_bucket_object['Key'].startswith(folder_url) and not my_bucket_object['Key'].endswith('/'):
                    lista.append(my_bucket_object['Key'])
            else:
                if not my_bucket_object['Key'].endswith('/'):
                    lista.append(my_bucket_object['Key'])
        return lista

    def move_file(self, file_name_origin, file_name_destiny):
        copy_source = {'Bucket': self.S3_BUCKET_NAME, 'Key': file_name_origin}
        response = self.s3_client.copy_object(CopySource=copy_source, Bucket=self.S3_BUCKET_NAME, Key=file_name_destiny)
        response = self.s3_client.delete_object(Bucket=self.S3_BUCKET_NAME, Key=file_name_origin)
        # slack_message('S3', 'File {origin_file} moved to {destiny_file}'.format(origin_file=file_name_origin, destiny_file=file_name_destiny))

    def move_to_backup(self, filename):
        file_name_destiny = self.backup_name(filename)
        self.move_file(filename, file_name_destiny)

    def backup_name(self, filename):
        fecha = datetime.now().strftime("%Y%m%d")
        processed_files = 'processed_files'
        return '{processed_files}/{filename}_{fecha}.{extension}'.format(
            processed_files=processed_files,
            filename=filename[:-4],
            fecha=fecha,
            extension=filename[-3:]
        )

    def download_file(self, s3_file_name: str, target_filename: str):
        # download the file from s3
        try:
            with open(target_filename, 'wb') as f:
                self.s3_client.download_fileobj(self.S3_BUCKET_NAME, s3_file_name, f)
        except ClientError as e:
            return 0
        return 1