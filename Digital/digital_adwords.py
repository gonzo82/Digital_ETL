
from utils.redshift import Redshift

def process_file(filename):
    ddbb = Redshift()
    ddbb.open_connection()
    ddbb.truncate_table('bi_development_stg.adwords_stg')
    ddbb.copy_file_into_redshift(s3_file_name=filename,
                                 table_name='bi_development_stg.adwords_stg',
                                 ignore_header=3,
                                 delimiter=',')
    ddbb.close_connection()

def process_stg_table():
    ddbb = Redshift()
    ddbb.open_connection()
    ddbb.update_table_from_table_current_month(origin_table_name='bi_development_stg.v_adwords_stg',
                                               destiny_table_name='bi_development.adwords_cost',
                                               date_field='campaign_date')
    ddbb.close_connection()