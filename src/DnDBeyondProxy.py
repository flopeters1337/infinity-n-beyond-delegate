from pathlib import Path
import logging
import requests
import json
import time
import os
import src.config as config
logging.basicConfig(level=logging.INFO)


class DnDBeyondProxy:
    def __init__(self, cobalt_key, output_folder=None):
        self._last_auth = None
        self._token = None
        self._token_death_timestamp = None
        self._cobalt = cobalt_key
        self._output_folder = os.path.join('..', 'data', 'output') if not output_folder else output_folder

        # Get D&D Beyond mapping JSON file
        logging.info('Loading mapping.json')
        with open(os.path.join('..', 'meta', 'mapping.json'), mode='r', encoding='utf8') as fd:
            self._mapping = json.load(fd)

    def _authenticate(self):
        if not self._token or self._token_death_timestamp <= time.time():
            logging.info('Requesting new bearer token')
            try:
                headers = {'Cookie': 'CobaltSession={0}'.format(self._cobalt)}
                self._last_auth = time.time()
                result = requests.post(config.AUTH_URL, headers=headers).json()
                self._token = result['token']
                self._token_death_timestamp = self._last_auth + result['ttl']
            except KeyError:
                raise ConnectionError('Failed to authenticate using Cobalt key.')

    def get_items(self):
        self._authenticate()
        try:
            headers = {'Authorization': 'Bearer {0}'.format(self._token)}
            result = requests.get(config.ITEMS_URL, headers=headers)
            result = result.json()

            # Create the output file
            final_path = os.path.join(self._output_folder, 'raw')
            Path(final_path).mkdir(parents=True, exist_ok=True)
            with open(os.path.join(final_path, 'items.json'), mode='w') as fd:
                json.dump(result['data'], fd)
        except KeyError:
            raise RuntimeError('Failed to obtain items.')

    def get_spells(self):
        classes = {x['id'] for x in self._mapping['classConfigurations']}
        aggregator = []

        for class_id in classes:
            logging.info('Fetching spells for class ID {0}'.format(class_id))
            self._authenticate()
            try:
                headers = {'Authorization': 'Bearer {0}'.format(self._token)}
                params = {'classId': class_id, 'classLevel': 20}
                result = requests.get(config.ITEMS_URL, headers=headers, params=params).json()['data']
                aggregator = [*aggregator, *result]  # Merge new results with the previous ones

            except KeyError:
                raise RuntimeError('Failed to obtain spells.')

        # Create the output file
        final_path = os.path.join(self._output_folder, 'raw')
        Path(final_path).mkdir(parents=True, exist_ok=True)
        with open(os.path.join(final_path, 'spells.json'), mode='w') as fd:
            json.dump(aggregator, fd)
