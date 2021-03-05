from urllib.error import HTTPError
from hubspot3 import Hubspot3
from urllib.request import urlopen
from utils import credentials as cred
import json
import pandas as pd


class JsonHubspot():
    API_KEY = cred.HUBSPOT_API_KEY
    url_base_campaign = cred.HUBSPOT_URL_BASE_CAMPAIGN
    url_base_events = cred.HUBSPOT_URL_BASE_EVENTS
    url_base_marketing_emails = cred.HUBSPOT_URL_BASE_MKT_EMAILS
    events_limit = 1000
    client = Hubspot3(api_key=API_KEY)

    def __init__(self):
        self.API_KEY = cred.HUBSPOT_API_KEY
        self.url_base_campaign = cred.HUBSPOT_URL_BASE_CAMPAIGN
        self.url_base_events = cred.HUBSPOT_URL_BASE_EVENTS
        self.url_base_marketing_emails = cred.HUBSPOT_URL_BASE_MKT_EMAILS

    def getCampaigns(self):
        json_url = urlopen(self.url_json)
        data = json.loads(json_url.read())
        return data['campaigns']

    def getCampaign(self, id_campaign):
        url_campaign = self.url_base_campaign + \
                       str(id_campaign) + \
                       '?hapikey=' + self.API_KEY
        campaign_json_url = urlopen(url_campaign)
        datasetCampaign = pd.read_json(campaign_json_url)
        return datasetCampaign

    def getCampaignEvents(self, id_campaign, time=None):

        continue_while = True
        url_event_base = self.url_base_events + \
                         '?hapikey=' + self.API_KEY + \
                         '&campaignId=' + str(id_campaign) + \
                         '&limit=' + str(self.events_limit)
        if time:
            url_event_base = url_event_base + '&startTimestamp=' + str(time)

        finalDataset = pd.DataFrame(dict())
        records = 0
        while continue_while:
            if records == 0:
                url_event = url_event_base
            else:
                url_event = url_event_base + '&offset=' + datasetEvent['offset'][0]
            try:
                event_json_url = urlopen(url_event)
                datasetEvent = pd.read_json(event_json_url)
            except HTTPError as e:
                return finalDataset
            records = records + len(datasetEvent)
            if records == 0:
                return finalDataset
            finalDataset = finalDataset.append(self.eventsToDict(datasetEvent))
            continue_while = datasetEvent['hasMore'][0]
        new_dataset = pd.DataFrame(dict())
        # print(finalDataset)
        if len(finalDataset) > 0:
            for field in cred.HUBSPOT_FIEDS:
                # print(finalDataset[field])
                new_dataset[field] = finalDataset[field]
        # print(new_dataset)
        return new_dataset

    def getMarketingEmails(self):
        url_event = self.url_base_marketing_emails + '?hapikey=' + self.API_KEY + '&limit=' + str(self.events_limit)
        event_json_url = urlopen(url_event)
        datasetEvent = pd.read_json(event_json_url)
        return datasetEvent['objects']

    def eventsToDict(self, events):
        index = 1
        rows = dict()
        try:
            for row in events['events']:
                row_filtered = dict()
                row_filtered['created'] = row['created']
                row_filtered['id'] = row['id']
                row_filtered['recipient'] = row['recipient']
                row_filtered['type'] = row['type']
                row_filtered['emailCampaignId'] = row['emailCampaignId']
                rows[index] = row_filtered
                index += 1
        except KeyError as e:
            pass
        return pd.DataFrame(rows).transpose()

    def campaignsToDict(self, emailMarketing):
        index = 1
        rows = dict()
        for row in emailMarketing:
            row_filtered = dict()
            row_filtered['ab'] = row['ab']
            row_filtered['analyticsPageType'] = row['analyticsPageType']
            row_filtered['created'] = row['created']
            row_filtered['fromName'] = row['fromName']
            row_filtered['name'] = row['name'].replace('✈', '')
            row_filtered['id'] = row['id']
            # row_filtered = {your_key: row[your_key] for your_key in your_keys}
            try:
                row_filtered['campaign'] = row['campaign']
            except KeyError as e:
                row_filtered['campaign'] = ''
            try:
                row_filtered['campaignName'] = row['campaignName']
            except KeyError as e:
                row_filtered['campaignName'] = ''

            for EmailCampaignId in row['allEmailCampaignIds']:
                row_filtered2 = row_filtered.copy()
                row_filtered2['EmailCampaignId'] = EmailCampaignId
                rows[index] = row_filtered2
                index += 1
        return pd.DataFrame(rows)
