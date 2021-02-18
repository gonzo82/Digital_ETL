from Aircall.aircall_json import JsonAircall
from utils import dates
from datetime import datetime, timedelta

from utils.redshift import Redshift
from utils.s3_boto3 import S3Bucket
from utils.slack import print_message, error_log
from utils import sql_sentence as sqls

@error_log
def generate_last_period():
    json = JsonAircall()
    s3_bucket = S3Bucket()
    folder = 'aircall'
    filename = 'aircall.csv'
    ddbb = Redshift()

    aircall_calls_stg_table = 'bi_development_stg.aircall_calls_stg'

    delete_loaded_records = sqls.AIRCALL_DELETE_LOADED_RECORDS

    load_new_records = sqls.AIRCALL_LOAD_NEW_RECORDS

    from_date = dates.return_seconds(1)
    rawCalls = json.getCalls(from_date=from_date)
    if len(rawCalls) > 0:
        rawCallsF = JsonAircall.formatDataFrame(rawCalls)
        s3_bucket.save_csv(df=rawCallsF, s3_file_name=filename, s3_folder=folder)

        ddbb.open_connection()
        ddbb.truncate_table(aircall_calls_stg_table)
        ddbb.copy_file_into_redshift(s3_file_name='{folder}/{file}'.format(folder=folder, file=filename),
                                     table_name=aircall_calls_stg_table,
                                     ignore_header=1)
        s3_bucket.move_to_backup('{folder}/{file}'.format(folder=folder, file=filename))
        ddbb.execute_query(delete_loaded_records)
        ddbb.execute_query(load_new_records)
        ddbb.close_connection()


@error_log
def generate_period():
    json = JsonAircall()
    s3_bucket = S3Bucket()
    folder = 'aircall'
    filename = 'aircall_{date}.csv'
    date_start = datetime.strptime('2021-02-01', '%Y-%m-%d')
    # end_date = datetime.strptime('2021-02-15', '%Y-%m-%d')
    end_date = datetime.now()
    # REALIZA PROCESAMIENTO DE LAS FECHAS DE 7 EN 7 DÍAS
    while date_start < end_date:
        from_date = dates.return_seconds_from_date(date_start.strftime('%Y-%m-%d'))
        to_date = dates.return_seconds_from_date((date_start + timedelta(days=7)).strftime('%Y-%m-%d'))
        print('date_start: {date_start} - from: {from_date} - to:{to_date}'.format(
            date_start=date_start,
            from_date=from_date,
            to_date=to_date)
        )
        rawCalls = json.getCalls(from_date=from_date, to_date=to_date)
        if len(rawCalls) > 0:
            rawCallsF = JsonAircall.formatDataFrame(rawCalls)
            s3_bucket.save_csv(df=rawCallsF, s3_file_name=filename.format(date=date_start.strftime('%Y%m%d')),
                               s3_folder=folder)
        date_start = date_start + timedelta(days=7)


if __name__ == '__main__':
    print_message('AIRCALL', 'Starting to load calls into the database')
    generate_last_period()
    # generate_period()
    print_message('AIRCALL', 'Calls loaded into the database')
