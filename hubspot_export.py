import sys
from datetime import datetime

from utils.slack import print_message, error_log
from utils.redshift import Redshift
from utils.sftp import Sftp
from utils import sql_sentence as sqls
from utils import credentials as cred


@error_log
def hubspot_send_date():
    parameters = sys.argv
    directory = 'assortment'
    file_name_users = 'users_data.csv'
    file_name_carts = 'carts_data.csv'
    file_name_abandoned = 'abandoned_carts_data.csv'
    hubspot_delimiter = ','

    redshift = Redshift()
    redshift.open_connection()
    sftp = Sftp(
        host=cred.FOXTER_FTP_IP,
        username=cred.FOXTER_FTP_USER,
        password=cred.FOXTER_FTP_PASSWORD
    )

    date_filter = 'incremental'
    if len(parameters) > 1:
        if parameters[1] == 'all':
            date_filter = 'all'
        elif parameters[1] == 'day':
            date_filter = '1 day'
        elif parameters[1] == 'month':
            date_filter = '30 day'
        # else:
        #     if datetime.today().hour == 7:
        #         date_filter = '1 day'
    # else:
    #     if datetime.today().hour == 7:
    #         date_filter = '1 day'

    if date_filter == 'incremental':
        users_sql = sqls.HUBSPOT_USERS_DATA + sqls.HUBSPOT_USERS_DATA_INCREMENTAL
        carts_sql = sqls.HUBSPOT_CARTS_DATA + sqls.HUBSPOT_CARTS_DATA_INCREMENTAL
        carts_abandoned_sql = sqls.HUBSPOT_ABANDONED_DATA + sqls.HUBSPOT_ABANDONED_DATA_INCREMENTAL
    elif date_filter == 'all':
        users_sql = sqls.HUBSPOT_USERS_DATA
        carts_sql = sqls.HUBSPOT_CARTS_DATA
        carts_abandoned_sql = sqls.HUBSPOT_ABANDONED_DATA
    elif date_filter == '1 day':
        users_sql = sqls.HUBSPOT_USERS_DATA + sqls.HUBSPOT_USERS_DATA_INCREMENTAL_DAY
        carts_sql = sqls.HUBSPOT_CARTS_DATA + sqls.HUBSPOT_CARTS_DATA_INCREMENTAL_DAY
        carts_abandoned_sql = sqls.HUBSPOT_ABANDONED_DATA
    elif date_filter == '30 day':
        users_sql = sqls.HUBSPOT_USERS_DATA + sqls.HUBSPOT_USERS_DATA_30_DAY
        carts_sql = sqls.HUBSPOT_CARTS_DATA + sqls.HUBSPOT_CARTS_DATA_30_DAY
        carts_abandoned_sql = sqls.HUBSPOT_ABANDONED_DATA

    # users_sql = sqls.HUBSPOT_USERS_DATA

    print_message('HUBSPOT', 'The export of the data starts ({date_filter})'.format(date_filter=date_filter))

    carts_data = redshift.fetch_data(carts_sql)
    sftp.send_file_df(df=carts_data, file_name=file_name_carts, directory=directory, delimiter=hubspot_delimiter)
    print_message('HUBSPOT', 'Carts data exported')

    carts_abandoned_data = redshift.fetch_data(carts_abandoned_sql)
    sftp.send_file_df(df=carts_abandoned_data, file_name=file_name_abandoned, directory=directory, delimiter=hubspot_delimiter)
    print_message('HUBSPOT', 'Abandoned carts data exported')


    users_data = redshift.fetch_data(users_sql)
    sftp.send_file_df(df=users_data, file_name=file_name_users, directory=directory, delimiter=hubspot_delimiter)
    print_message('HUBSPOT', 'Users data exported')

    # slack_message('HUBSPOT', 'Files loaded into the sftp')

    sftp.close()
    redshift.close_connection()


if __name__ == '__main__':
    hubspot_send_date()
