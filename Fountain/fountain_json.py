from urllib.request import urlopen
import json
import pandas as pd
import numpy as np
from utils import credentials as cred

from utils.slack import slack_message


class Json:
    url_json = cred.FOUNTAIN_URL_BASE +\
               '?' + 'api_token=' + cred.FOUNTAIN_TOKEN +\
               '&' + 'template_id=' + cred.FOUNTAIN_TEMPLATE_ID

    def getData(self):
        json_url = urlopen(self.url_json)
        data = json.loads(json_url.read())
        url = ''
        if data['timestamped_exports'][0]['state'] == 'finished':
            url = data['timestamped_exports'][0]['file_url']
        else:
            url = data['timestamped_exports'][1]['file_url']

        dataset = pd.read_csv(url, low_memory=False)

        dataset = dataset.replace({"Barcelona ": "Barcelona", "BCN": "Barcelona", "madrid": "Madrid", "MAD": "Madrid",
                                   "barcelona": "Barcelona", "MADRID": "Madrid", "MADRID ": "Madrid",
                                   "Madrid ": "Madrid", "Malaga ": "Málaga", "Malaga": "Málaga", "VALENCIA": "Valencia",
                                   "Bilbao ": "Bilbao", "sevilla": "Sevilla"})
        dataset = dataset.replace({"adwords": "Google", "GoogleAdword": "Google"})
        dataset = dataset.replace({"Instagram": "Facebook"})

        dataset['Source'] = np.where(dataset['Utm source'].isnull(), dataset['Como conocido'], dataset['Utm source'])

        new_dataset = dataset[cred.FOUNTAIN_FIEDS]

        # slack_message('FOUNTAIN', f"Fichero actualizado a {data['timestamped_exports'][0]['created_at']} generado")

        return new_dataset

    def __init__(self):
        self.url_json = cred.FOUNTAIN_URL_BASE +\
                        '?' + 'api_token=' + cred.FOUNTAIN_TOKEN +\
                        '&' + 'template_id=' + cred.FOUNTAIN_TEMPLATE_ID
