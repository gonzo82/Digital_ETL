from utils import credentials as cred
from Delighted.delighted_api import Delighted
from utils.redshift import Redshift
from utils.s3_boto3 import S3Bucket
from utils.slack import error_log, print_message
from utils import sql_sentence as sql
import sys


@error_log
def getDelighted(api_key):
    folder = 'delighted'

    # Surveys
    surveys_filename = 'delighted_surveys.csv'
    surveys_table_stg = 'bi_development_stg.delighted_surveys_stg'

    # People
    people_filename = 'delighted_people.csv'
    people_table_stg = 'bi_development_stg.delighted_people_stg'

    s3_bucket = S3Bucket()

    delighted = Delighted(api_key)

    surveyDataset = delighted.getSurveys()
    s3_bucket.save_csv(surveyDataset, surveys_filename, folder)
    peopleDataset = delighted.getPeople()
    s3_bucket.save_csv(peopleDataset, people_filename, folder)

    ddbb = Redshift()
    ddbb.open_connection()

    ddbb.truncate_table(surveys_table_stg)
    ddbb.copy_file_into_redshift(s3_file_name='{folder}/{filename}'.format(folder=folder, filename=surveys_filename),
                                 table_name=surveys_table_stg,
                                 ignore_header=1,
                                 delimiter=cred.S3_DELIMITER)
    ddbb.execute_query(sql.DELIGHTED_SURVEYS)
    s3_bucket.move_to_backup('{folder}/{filename}'.format(folder=folder, filename=surveys_filename))
    print_message('DELIGHTED', 'Surveys data added to database')

    ddbb.truncate_table(people_table_stg)
    ddbb.copy_file_into_redshift(s3_file_name='{folder}/{filename}'.format(folder=folder, filename=people_filename),
                                 table_name=people_table_stg,
                                 ignore_header=1,
                                 delimiter=cred.S3_DELIMITER)
    ddbb.execute_query(sql.DELIGHTED_PEOPLE)
    print_message('DELIGHTED', 'People data added to database')
    s3_bucket.move_to_backup('{folder}/{filename}'.format(folder=folder, filename=people_filename))

    ddbb.close_connection()


if __name__ == '__main__':
    parameters = sys.argv
    if len(parameters) > 1:
        getDelighted(parameters[1])
    else:
        getDelighted(cred.DELIGHTED_API_KEY_CSAT_V2)




