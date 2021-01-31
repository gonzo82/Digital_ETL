import sys
from Fountain.fountain_json import Json
from datetime import datetime
from utils.redshift import Redshift
from utils.s3_boto3 import S3Bucket
from utils import credentials as cred
from utils.slack import print_message, error_log


@error_log
def fountain_load_data():
    folder = 'fountain'
    filename = "descarga_fountain.csv"
    table_stg = 'bi_development_stg.fountain_stg'
    destiny_table = 'bi_development.fountain'
    origin_table = 'bi_development_stg.v_fountain_stg'

    print_message('FOUNTAIN', 'Starting to load the data')
    json = Json()
    dataset = json.getData()

    s3_bucket = S3Bucket()
    s3_bucket.save_csv(dataset, filename, folder)

    ddbb = Redshift()
    ddbb.open_connection()

    ddbb.truncate_table(table_stg)
    ddbb.copy_file_into_redshift(s3_file_name='{folder}/{filename}'.format(folder=folder, filename=filename),
                                 table_name=table_stg,
                                 ignore_header=1,
                                 delimiter=cred.S3_DELIMITER)
    ddbb.copy_table_from_table(origin_table, destiny_table)

    ddbb.close_connection()

    s3_bucket.move_to_backup('{folder}/{filename}'.format(folder=folder, filename=filename))

    print_message('FOUNTAIN', 'Data added to database')


if __name__ == '__main__':
    fountain_load_data()
