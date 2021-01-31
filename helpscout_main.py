# https://pypi.org/project/python-helpscout-v2/
from utils import credentials as cred
from Helpscout.helpscout_api import HelpScout_api
from utils.redshift import Redshift
from utils.s3_boto3 import S3Bucket
from utils import sql_sentence as sql
from utils.slack import print_message, error_log


@error_log
def helpscout_load_data():
    folder = 'helpscout'
    processed_files = 'processed_files'

    # Mailbox
    mailbox_filename = 'helpscout_mailbox.csv'
    mailbox_table_stg = 'bi_development_stg.helpscout_mailbox_stg'
    mailbox_destiny_table = 'bi_development.helpscout_mailbox'
    mailbox_origin_table = 'bi_development_stg.v_helpscout_mailbox_stg'

    # Mailbox
    conversations_filename = 'helpscout_conversations.csv'
    conversations_table_stg = 'bi_development_stg.helpscout_conversations_stg'
    conversations_destiny_table = 'bi_development.helpscout_conversations'
    conversations_origin_table = 'bi_development_stg.v_helpscout_conversations_stg'

    s3_bucket = S3Bucket()
    ddbb = Redshift()
    hs = HelpScout_api()

    # Get the mailboxes
    mailboxes = hs.getMailboxes()
    s3_bucket.save_csv(mailboxes, mailbox_filename, folder)
    ddbb.open_connection()
    ddbb.truncate_table(mailbox_table_stg)
    ddbb.copy_file_into_redshift(s3_file_name='{folder}/{filename}'.format(folder=folder, filename=mailbox_filename),
                                 table_name=mailbox_table_stg,
                                 ignore_header=1,
                                 delimiter=cred.S3_DELIMITER)
    ddbb.copy_table_from_table(mailbox_origin_table, mailbox_destiny_table)
    ddbb.close_connection()
    s3_bucket.move_to_backup('{folder}/{filename}'.format(folder=folder, filename=mailbox_filename))
    print_message('HELPSCOUT', 'Mailboxes data added to database')

    # Get the mailboxes
    conversations = hs.getConversationsLast()
    if len(conversations) > 0:
        s3_bucket.save_csv(conversations, conversations_filename, folder)
        ddbb.open_connection()
        ddbb.truncate_table(conversations_table_stg)
        ddbb.copy_file_into_redshift(
            s3_file_name='{folder}/{filename}'.format(folder=folder, filename=conversations_filename),
            table_name=conversations_table_stg,
            ignore_header=1,
            delimiter=cred.S3_DELIMITER)
        s3_bucket.move_to_backup('{folder}/{filename}'.format(folder=folder, filename=conversations_filename))
        ddbb.execute_query(sql.HELPSCOUT_DELETE)
        ddbb.execute_query(sql.HELPSCOUT_INSERT)
        ddbb.execute_query(sql.HELPSCOUT_ACTIVE)
        ddbb.close_connection()
        print_message('HELPSCOUT', 'Conversations data added to database')


if __name__ == '__main__':
    helpscout_load_data()