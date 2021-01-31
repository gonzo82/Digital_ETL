
from utils.redshift import Redshift

def truntcate_stg_table():
    ddbb = Redshift()

    ddbb.open_connection()
    ddbb.truncate_table('bi_development_stg.google_analytics_stg')

    ddbb.close_connection()

def process_file(filename):
    ddbb = Redshift()
    ddbb.open_connection()
    ddbb.copy_file_into_redshift(s3_file_name=filename,
                                 table_name='bi_development_stg.google_analytics_stg',
                                 ignore_header=7,
                                 delimiter=',')
    ddbb.close_connection()

def process_stg_table():
    ddbb = Redshift()
    ddbb.open_connection()
    ddbb.update_table_from_table_current_month(origin_table_name='bi_development_stg.v_google_analytics_stg',
                                               destiny_table_name='bi_development.google_analytics',
                                               date_field='event_date')
    ddbb.close_connection()