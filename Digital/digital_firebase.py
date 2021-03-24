from utils.redshift import Redshift
from utils.s3_boto3 import S3Bucket
import os


def process_file(filename):
    ddbb = Redshift()
    ddbb.open_connection()
    if 'session_start' in filename:
        ddbb.truncate_table('bi_development_stg.firebase_sessions_stg')
        ddbb.copy_file_into_redshift(s3_file_name=filename,
                                     table_name='bi_development_stg.firebase_sessions_stg',
                                     ignore_header=1,
                                     delimiter=',')
    elif 'ecomerce_purchase' in filename:
        ddbb.truncate_table('bi_development_stg.firebase_purchase_stg')
        ddbb.copy_file_into_redshift(s3_file_name=filename,
                                     table_name='bi_development_stg.firebase_purchase_stg',
                                     ignore_header=1,
                                     delimiter=',')
    else:
        ddbb.truncate_table('bi_development_stg.firebase_stg')
        ddbb.copy_file_into_redshift(s3_file_name=filename,
                                     table_name='bi_development_stg.firebase_stg',
                                     ignore_header=1,
                                     delimiter=',')
    ddbb.close_connection()


def process_stg_table():
    ddbb = Redshift()
    ddbb.open_connection()
    ddbb.update_table_from_table_current_month(origin_table_name='bi_development_stg.v_firebase_stg',
                                               destiny_table_name='bi_development.firebase',
                                               date_field='event_date')
    ddbb.update_table_from_table_current_month(origin_table_name='bi_development_stg.v_firebase_sessions_stg',
                                               destiny_table_name='bi_development.firebase_sessions',
                                               date_field='event_date')
    ddbb.update_table_from_table_current_month(origin_table_name='bi_development_stg.v_firebase_purchase_stg',
                                               destiny_table_name='bi_development.firebase_purchase',
                                               date_field='event_date')
    ddbb.close_connection()


def change_files():
    s3 = S3Bucket()
    # firebase_files = 'firebase'
    firebase_files = 'firebase'
    tmp_filename = 'tmp_file.csv'
    file_list = s3.list_folders(firebase_files)
    for file in file_list:
        local_filename = file.split('/')[1]
        s3.download_file(s3_file_name=file, target_filename=tmp_filename)
        tmp_file = open(tmp_filename, 'r')
        start_file = 0
        inicio = 0
        inicios = 0
        cabeceras = 0
        first_date = ''
        for line in tmp_file.readlines():
            if line.rstrip('\n').startswith('# Fecha de inicio'):
                inicios = inicios + 1
                if not first_date:
                    first_date = line.rstrip('\n').replace('# Fecha de inicio: ', '')
                    first_date = first_date[0:4] + '-' + first_date[4:6] + '-' + first_date[6:8]
        tmp_file.close()

        if inicios > 0:
            tmp_file = open(tmp_filename, 'r')
            local_file = open(local_filename, 'w')
            for line in tmp_file.readlines():
                if line.rstrip('\n').startswith('# Fecha de inicio'):
                    inicio = inicio + 1
                if inicios == inicio:
                    cabeceras = cabeceras + 1
                    if cabeceras >= 3:
                        local_file.writelines(line.rstrip('\n') + ',' + first_date + '\n')
            local_file.close()
            tmp_file.close()
            s3.upload_file(s3_file_name=local_filename, s3_folder='firebase', local_file_name=local_filename)
            os.remove(tmp_filename)
            os.remove(local_filename)