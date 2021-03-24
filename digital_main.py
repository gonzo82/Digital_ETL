from Digital import digital_adwords, digital_facebook, digital_google_analytics, digital_firebase
from utils.s3_boto3 import S3Bucket
from utils.slack import slack_message, error_log


@error_log
def load_file(file_name):
    if file_name.startswith('adwords'):
        digital_adwords.process_file(file_name)
    elif file_name.startswith('facebook'):
        digital_facebook.process_file(file_name)
    elif file_name.startswith('firebase'):
        digital_firebase.process_file(file_name)
    elif file_name.startswith('GoogleAnalytics'):
        digital_google_analytics.process_file(file_name)


@error_log
def process_file(file_type):
    if file_type == 'adwords':
        digital_adwords.process_stg_table()
    elif file_type == 'facebook':
        digital_facebook.process_stg_table()
    elif file_type == 'firebase':
        digital_firebase.process_stg_table()
    elif file_type == 'GoogleAnalytics':
        digital_google_analytics.process_stg_table()


@error_log
def digital_load_data():
    s3 = S3Bucket()
    processed_files = 'processed_files'

    file_types = ['adwords', 'facebook', 'firebase', 'GoogleAnalytics']

    digital_google_analytics.truntcate_stg_table()
    digital_firebase.change_files()

    for file_type in file_types:
        file_list = s3.list_folders(file_type)
        files = 0
        for file in file_list:
            if not file.startswith(processed_files):
                load_file(file)
                s3.move_to_backup(file)
                files = files + 1
        if files > 0:
            process_file(file_type)
            slack_message('DIGITAL', 'Files from {file_type} has been processed'.format(file_type=file_type))


if __name__ == '__main__':
    digital_load_data()
