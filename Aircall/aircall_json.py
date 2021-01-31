import base64
import requests
import pandas as pd
import json
import utils.credentials as cred


class JsonAircall():
    def __init__(self):
        self.apiId = cred.AIRCALL_API_ID
        self.apiToken = cred.AIRCALL_API_TOKEN
        base_64_origin = self.apiId + ':' + self.apiToken
        self.encodedCredentials = base64.b64encode(str.encode(base_64_origin)).decode("utf-8")
        self.url_options = {
            'port': 443,
            'Authorization': 'Basic ' + self.encodedCredentials
        }
        self.base_url = 'https://{api_id}:{api_token}@api.aircall.io/v1/calls?per_page={rowspp}'.format(
            api_id=self.apiId,
            api_token=self.apiToken,
            rowspp=cred.AIRCALL_ROWS_PER_PAGE
        )

    def getCalls(self, from_date=None, to_date=None):
        if not from_date:
            from_date = 0
        url_base = self.base_url + '&from=' + str(from_date)
        if to_date:
            url_base = url_base + '&to=' + str(to_date)

        continues = True
        page = 1
        rawCalls = pd.DataFrame()
        num_calls = 0

        while continues:
            url = url_base + '&page=' + str(page)
            r = requests.get(
                url=url,
                params=self.url_options
            )
            try:
                calls = json.loads(r.text)['calls']
                if page == 1:
                    rawCalls = pd.DataFrame(calls)
                else:
                    rawCalls = rawCalls.append( pd.DataFrame(calls))
                page = page + 1
                if num_calls == len(rawCalls):
                    continues = False
                    print('Se han leido {registros} registros'.format(registros=len(rawCalls)))
                else:
                    num_calls = len(rawCalls)
            except:
                print(url)
                print(r.text)
                print('Se han leido {registros} registros'.format(registros=len(rawCalls)))
                continues = False
        return rawCalls

    def formatDataFrame(rawCalls):
        rawCallsF = pd.DataFrame()
        fields = ['id', 'direct_link', 'direction', 'status', 'missed_call_reason', 'started_at', 'answered_at', 'ended_at', 'duration', 'voicemail', 'recording', 'asset', 'raw_digits', 'user', 'contact', 'archived', 'assigned_to', 'tags', 'transferred_to', 'teams', 'cost', 'comments', 'number']
        for field in fields:
            try:
                rawCallsF[field] = rawCalls[field]
            except:
                rawCallsF[field] = ''

        return rawCallsF