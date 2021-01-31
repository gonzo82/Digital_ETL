
from utils.redshift import Redshift

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