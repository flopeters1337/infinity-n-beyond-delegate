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

        # Build mappings
        self._stealth_map = {x['id']: x['name'] for x in self._mapping['stealthCheckTypes']}
        self._attack_map = {x['id']: x['name'] for x in self._mapping['rangeTypes']}
        self._category_map = {x['id']: x['name'] for x in self._mapping['weaponCategories']}
        self._source_map = {x['id']: x['name'] for x in self._mapping['sources']}
        self._armor_map = {x['id']: x['name'] for x in self._mapping['armorTypes']}
        self._gear_map = {x['id']: x['name'] for x in self._mapping['gearTypes']}

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

    def _dump_data(self, data, filename, raw=True):
        data_type = 'raw' if raw else 'processed'
        final_path = os.path.join(self._output_folder, data_type)
        Path(final_path).mkdir(parents=True, exist_ok=True)
        with open(os.path.join(final_path, filename), mode='w') as fd:
            json.dump(data, fd)

    def get_items(self):
        self._authenticate()
        try:
            headers = {'Authorization': 'Bearer {0}'.format(self._token)}
            result = requests.get(config.ITEMS_URL, headers=headers)
            result = result.json()

            self._dump_data(result['data'], 'items.json')
        except KeyError:
            raise RuntimeError('Failed to obtain items.')

    def get_monsters(self, skip_size=100):
        aggregator = []
        count_current = None
        skip = 0

        while count_current is None or count_current > 0:
            self._authenticate()
            try:
                logging.info('Fetching {0} monsters after skipping {1}'.format(100, skip))
                headers = {'Authorization': 'Bearer {0}'.format(self._token)}
                params = {'skip': skip, 'take': 100, 'showHomebrew': 'f'}
                result = requests.get(config.MONSTER_URL, headers=headers, params=params).json()
                count_current = len(result['data'])
                aggregator = [*aggregator, *result['data']]
                skip += skip_size
            except KeyError:
                raise RuntimeError('Failed to obtain monsters.')

        self._dump_data(aggregator, 'monsters.json')

    def get_spells(self):
        classes = {x['id'] for x in self._mapping['classConfigurations']}
        aggregator = []

        for class_id in classes:
            logging.info('Fetching spells for class ID {0}'.format(class_id))
            self._authenticate()
            try:
                headers = {'Authorization': 'Bearer {0}'.format(self._token)}
                params = {'classId': class_id, 'classLevel': 20}
                result = requests.get(config.SPELLS_URL, headers=headers, params=params).json()['data']
                aggregator = [*aggregator, *result]  # Merge new results with the previous ones

            except KeyError:
                raise RuntimeError('Failed to obtain spells.')

        self._dump_data(aggregator, 'spells.json')

    def process_items(self, input_file=None):
        if not input_file:
            input_file = os.path.join('..', 'data', 'output', 'raw', 'items.json')
        output_file = os.path.join(self._output_folder, 'processed', 'items.json')

        # Read input file
        with open(input_file, mode='r', encoding='utf8') as fd:
            data = json.load(fd)

        logging.info('Processing items')
        result = [self._process_item(x) for x in data]

        # Write to file
        self._dump_data(result, 'items.json', raw=False)

    def _process_item(self, item):
        """ --- List of processed data ---
            <in items.json> <=> <in mapping.json>
            stealthCheck    <=> stealthCheckTypes
            attackType      <=> rangeTypes (Most likely guess?)
            categoryId      <=> weaponCategories
            sourceId        <=> sources
            armorTypeId     <=> armorTypes
            gearTypeId      <=> gearTypes
        """
        logging.debug('Processing item {0}'.format(item['name']))

        # Apply mappings
        item['stealthCheck'] = self._stealth_map[item['stealthCheck']] if item['stealthCheck'] else None
        item['attackType'] = self._attack_map[item['attackType']] if item['attackType'] else None
        item['category'] = self._category_map[item['categoryId']] if item['categoryId'] else None
        item['source'] = self._source_map[item['sourceId']] if item['sourceId'] else None
        for source in item['sources']:
            source['sourceName'] = self._source_map[source['sourceId']] if source['sourceId'] else None
        item['armorType'] = self._armor_map[item['armorTypeId']] if item['armorTypeId'] else None
        item['gearType'] = self._gear_map[item['gearTypeId']] if item['gearTypeId'] else None

        return item
