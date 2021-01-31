import time

import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
#from dotenv import load_dotenv
#load_dotenv()
from utils import credentials as cred
import os
from utils.slack import slack_message

MAX_QUERY_RETRIES = 3
COOL_DOWN_SECONDS = 10


class Redshift:
    """ Wrapper around psycopg2 library
    that takes care of querying AWS Redshfit cluster and returning data to python

    Basic usage:
    redshift = Redshift()
    redshift.open_connection()
    ...do work...
    redshift.close_connection()

    """
    def __init__(self):
        self.host = cred.host
        self.port = cred.port
        self.db = cred.dbname
        self.user = cred.user
        self.password = cred.password
        self.connection = None

    def open_connection(self):

        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                dbname=self.db,
                user=self.user,
                password=self.password,
                connect_timeout=5
            )
            # slack_message('Connection with Redshift established')

        except:
            raise Exception("Cannot open connection to AWS Redshift. "
                            "Make sure you have the necessary permissions "
                            "to connect")

    def close_connection(self):
        self.connection.close()

    def fetch_data(self, sql):

        query_completed = False
        num_retries = 0
        output = None
        # print(sql)
        while (not query_completed) and (num_retries < MAX_QUERY_RETRIES):

            try:
                # slack_message('REDSHIFT', 'Start fetching data as pandas df..')
                output = pd.read_sql_query(sql, self.connection)
                query_completed = True
                # slack_message('REDSHIFT', 'Finished fetching df')

            # except psycopg2.OperationalError as e:
            except all as e:
                # slack_message('REDSHIFT', e)
                # slack_message('REDSHIFT', f'Retrying query in {COOL_DOWN_SECONDS} seconds')
                num_retries += 1
                time.sleep(COOL_DOWN_SECONDS)

        return output

    def execute_query(self, sql, fetch_data=False):

        query_completed = False
        num_retries = 0
        output = None
        while (not query_completed) and (num_retries < MAX_QUERY_RETRIES):

            try:
                with self.connection:
                    with self.connection.cursor(cursor_factory=RealDictCursor) as c:
                        c.execute(sql)
                        if fetch_data:
                            output = c.fetchall()

                query_completed = True

            except psycopg2.OperationalError as e:
            # except all as e:
                slack_message('REDSHIFT', e)
                slack_message('REDSHIFT', f'Retrying query in {COOL_DOWN_SECONDS} seconds')
                num_retries += 1
                time.sleep(COOL_DOWN_SECONDS)

        return output

    def truncate_table(self, table_name):
        sql = 'truncate table {table_name}'.format(table_name=table_name)
        self.execute_query(sql)

    def copy_file_into_redshift(self, s3_file_name, table_name, ignore_header, delimiter=None):
        """
        This method copy a filte (s3_folder/s3_file_name) into a tablea (table_natable_name)
        """
        S3_BUCKET_NAME = cred.S3_BUCKET_NAME
        S3_IAM_ROLE = cred.S3_IAM_ROLE
        if not delimiter:
            delimiter = cred.S3_DELIMITER

        # create temporary staging table
        # sql = 'truncate table {table_name}'.format(table_name=table_name)
        # self.execute_query(sql)

        # copy data from s3 to staging table
        sql = """
            begin;
            copy {table_name} from 's3://{S3_BUCKET_NAME}/{s3_file_name}'
            iam_role '{S3_IAM_ROLE}' 
            csv
            delimiter '{delimiter}'
            ignoreheader {ignore_header};
            commit;
        """.format(
            table_name=table_name,
            S3_BUCKET_NAME=S3_BUCKET_NAME,
            s3_file_name=s3_file_name,
            S3_IAM_ROLE=S3_IAM_ROLE,
            delimiter=delimiter,
            ignore_header=ignore_header
        )
        # print(sql)
        self.execute_query(sql)

    def copy_table_from_table(self, origin_table_name, destiny_table_name):
        # replace the data of the destiny_table with the data of the origin_table
        sql = """
            begin transaction;

            truncate table {destiny_table_name} ;

            insert into {destiny_table_name} 
            select * from {origin_table_name};

            end transaction;
        """.format(
            destiny_table_name=destiny_table_name,
            origin_table_name=origin_table_name
        )
        self.execute_query(sql)

    def update_table_from_table_current_month(self, origin_table_name, destiny_table_name, date_field):
        # replace the data of the destiny_table with the data of the origin_table
        sql = """
            begin transaction;

            delete from {destiny_table_name}
            where
                exists ( select *
                        from {origin_table_name} otn
                        where
                            {destiny_table_name}.{date_field} = otn.{date_field}
                            and {destiny_table_name}.{date_field} >=
                                    (select
                                            date_trunc('month', max(otn2.{date_field}))::date
                                        from
                                            {origin_table_name} otn2
                                    )
                        )
            ;

            insert
                into {destiny_table_name} 
            select
                *
            from
                {origin_table_name} otn
            where
                otn.{date_field} >= (select
                                        date_trunc('month', max(otn2.{date_field}))::date
                                        from {origin_table_name} otn2
                                      )
            ;

            end transaction;
        """.format(
            destiny_table_name=destiny_table_name,
            origin_table_name=origin_table_name,
            date_field=date_field
        )
        self.execute_query(sql)