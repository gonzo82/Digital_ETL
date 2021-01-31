import sys

import pandas as pd
from helpscout import HelpScout
from utils import credentials as cred
from datetime import date, timedelta

class HelpScout_api:

    def __init__(self):
        self.hs = HelpScout(app_id=cred.HELPSCOUT_API_ID, app_secret=cred.HELPSCOUT_APP_SECRET)

    def mailboxesToDict(self, mailboxes):
        index = 1
        rows = dict()
        try:
            for mailbox in mailboxes:
                row_filtered = dict(mailbox)
                rows[index] = row_filtered
                index += 1
        except KeyError as e:
            pass
        return pd.DataFrame(rows).transpose()

    def conversationToDict(selfself, conversations):
        index = 1
        rows = dict()
        try:
            for conversation in conversations:
                row_filtered = dict()
                try:
                    row_filtered['id'] = conversation.id
                    row_filtered['mailboxID'] = conversation.mailboxId
                    row_filtered['created_By_id'] = conversation.createdBy['id']
                    row_filtered['created_By_type'] = conversation.createdBy['type']
                    row_filtered['created_By_first'] = conversation.createdBy['first']
                    row_filtered['created_By_last'] = conversation.createdBy['last']
                    row_filtered['created_By_email'] = conversation.createdBy['email']
                    row_filtered['closed_By_User_type'] = conversation.closedByUser['type']
                    row_filtered['closed_By_User_first'] = conversation.closedByUser['first']
                    row_filtered['closed_By_User_last'] = conversation.closedByUser['last']
                    row_filtered['closed_By_User_email'] = conversation.closedByUser['email']
                    row_filtered['customer_Waiting_Since_time'] = conversation.customerWaitingSince['time']
                    row_filtered['number'] = conversation.number
                    row_filtered['preview'] = conversation.preview
                    row_filtered['primary_Customer_id'] = conversation.primaryCustomer['id']
                    row_filtered['primary_Customer_type'] = conversation.primaryCustomer['type']
                    row_filtered['primary_Customer_first'] = conversation.primaryCustomer['first']
                    row_filtered['primary_Customer_last'] = conversation.primaryCustomer['last']
                    try:
                        row_filtered['primary_Customer_email'] = conversation.primaryCustomer['email']
                    except Exception as e:
                        row_filtered['primary_Customer_email'] = ''
                    row_filtered['source_type'] = conversation.source['type']
                    row_filtered['source_via'] = conversation.source['via']
                    row_filtered['state'] = conversation.state
                    row_filtered['status'] = conversation.status
                    try:
                        row_filtered['subject'] = conversation.subject
                    except Exception as e:
                        row_filtered['subject'] = ''
                    row_filtered['tags'] = conversation.tags
                    row_filtered['type'] = conversation.type
                    row_filtered['user_Updated_At'] = conversation.userUpdatedAt
                except Exception as e:
                    print(e)
                    print(conversation)
                    print(row_filtered)
                rows[index] = row_filtered
                index += 1
        except KeyError as e:
            print(KeyError)
        return pd.DataFrame(rows).transpose()

    def getMailboxes(self):
        mailboxes = self.hs.hit('mailboxes', 'get')
        # json_mailboxes = json.loads(mailboxes[0])
        return self.mailboxesToDict(mailboxes[0]['mailboxes'])

    def getConversations(self, month=None, year=None, mailbox=None, status=None, date_type='updated'):
        params_query = ''
        if status:
            params_query = 'status={status}'.format(status=status)
        else:
            params_query = 'status=all'
        if month and year:
            if month == 12:
                year_to = year + 1
                month_to = 1
            else:
                year_to = year
                month_to = month + 1
            yearmonth_param = 'query=({date_type}At:[{year_from}-{month_from}-01T00:00:00Z TO {year_to}-' \
                           '{month_to}-01T00:00:00Z])'.format(
                date_type=date_type,
                year_from=year,
                month_from=month,
                year_to=year_to,
                month_to=month_to
            )
            params_query = '{actual}&{yearmonth_param}'.format(actual=params_query, yearmonth_param=yearmonth_param)
        if mailbox:
            params_query = '{actual}&mailbox={mailbox}'.format(actual=params_query, mailbox=mailbox)
        print(params_query)
        conversations = self.hs.conversations.get(params=params_query)
        conver = self.conversationToDict(conversations)
        return conver

    def getConversationsLast(self, hours=24):
        date_origin = (date.today() - timedelta(hours=hours)).strftime("%Y-%m-%d")
        date_end = (date.today() + timedelta(hours=hours)).strftime("%Y-%m-%d")
        params_query = 'status=all&query=(modifiedAt:[{date}T00:00:00Z TO *])'.format(date=date_origin)
        print(params_query)
        conversations = self.hs.conversations.get(params=params_query)
        conver = self.conversationToDict(conversations)
        return conver
