
from utils.redshift import Redshift

def process_file(filename):
    ddbb = Redshift()
    ddbb.open_connection()
    ddbb.truncate_table('bi_development_stg.facebook_stg_day')
    ddbb.copy_file_into_redshift(s3_file_name=filename,
                                 table_name='bi_development_stg.facebook_stg_day',
                                 ignore_header=2,
                                 delimiter=',')
    ddbb.close_connection()

def process_stg_table():
    ddbb = Redshift()
    ddbb.open_connection()
    ddbb.update_table_from_table_current_month(origin_table_name='bi_development_stg.v_facebook_stg_day',
                                               destiny_table_name='bi_development.facebook_cost',
                                               date_field='campaign_date')
    ddbb.close_connection()