from Hubspot.hubspot_json import JsonHubspot
from utils.redshift import Redshift
from utils import credentials as cred
from utils import sql_sentence as sqls
from utils.slack import print_message, error_log
from utils.s3_boto3 import S3Bucket
import pandas as pd


@error_log
def load_hubspot_data():
    filename = "hubspot_campaign.csv"
    filename_all_events = "hubspot_events_all.csv"
    your_keys = {'ab',
                 'analyticsPageType',
                 'created',
                 'fromName',
                 'name',
                 'id'
                 }
    folder = 'hubspot'
    campaign_stg_table = 'bi_development_stg.hubspot_campaigns_stg'
    campaign_origin = 'bi_development_stg.v_hubspot_campaigns_stg'
    campaign_table = 'bi_development.hubspot_campaigns'
    events_stg_table = 'bi_development_stg.hubspot_events_stg'
    events_origin = 'bi_development_stg.v_hubspot_events_stg'
    events_table = 'bi_development.hubspot_events'
    processed_files = 'processed_files'

    sql_load_campaigns = sqls.HUBSPOT_LOAD_CAMPAIGNS

    sql_load_events = sqls.HUBSPOT_LOAD_EVENTS

    s3 = S3Bucket()
    ddbb = Redshift()
    ddbb.open_connection()

    json = JsonHubspot()
    emailMarketing = json.getMarketingEmails()
    dfObj = json.campaignsToDict(emailMarketing)

    s3.save_csv(df=dfObj.transpose(), s3_file_name=filename, s3_folder=folder)

    ddbb.truncate_table(campaign_stg_table)
    ddbb.copy_file_into_redshift(s3_file_name='{folder}/{filename}'.format(folder=folder, filename=filename),
                                 table_name=campaign_stg_table,
                                 ignore_header=1,
                                 delimiter=cred.S3_DELIMITER)
    ddbb.execute_query(sql_load_campaigns)
    s3.move_to_backup('{folder}/{filename}'.format(folder=folder, filename=filename))
    print_message('HUBSPOT', 'Campaigns loaded into database')

    all_events = pd.DataFrame()
    campaigns = ddbb.fetch_data('select email_campaign_id from bi_development.hubspot_campaigns order by created desc')
    last_event = ddbb.fetch_data("select pgdate_part('epoch', max(dateadd('hour', -4, created)))*1000 as created_num from bi_development.hubspot_events")
    ddbb.close_connection()

    last_event_num = 0
    for row in last_event['created_num']:
        last_event_num = row
    for row in campaigns['email_campaign_id']:
        events = json.getCampaignEvents(str(row), str(last_event_num).replace('.0', ''))
        # events = json.getCampaignEvents(str(row), dates.return_miliseconds(1))
        all_events = all_events.append(events)

    if len(all_events) > 0:
        tmp_file_name = filename_all_events
        S3Bucket().save_csv(all_events, tmp_file_name, 'hubspot')

    # slack_message('HUBSPOT', 'Events extracted from hubspot')

    file_list = s3.list_folders(folder_url=folder)
    files = 0
    ddbb.open_connection()
    ddbb.truncate_table(events_stg_table)
    for file in file_list:
        if not file.startswith(processed_files) \
                and str(file) != 'hubspot/hubspot_campaign.csv':
            ddbb.copy_file_into_redshift(s3_file_name=file,
                                         table_name=events_stg_table,
                                         ignore_header=1,
                                         delimiter=cred.S3_DELIMITER)
            s3.move_to_backup(file)
            files = files + 1
    ddbb.execute_query(sql_load_events)

    print_message('HUBSPOT', 'Events loaded into database')

    ddbb.close_connection()


if __name__ == '__main__':
    load_hubspot_data()