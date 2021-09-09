import requests
import pandas as pd
import logging

## set logging
logging.getLogger().setLevel(logging.INFO)

class Manifesto_data():

    '''This class helps you get data using the Manifesto API.'''

    def __init__(self, VERSION, KEY):
        self.version = VERSION
        self.key = KEY

        # set urls
        self.url_core_versions = f"https://manifesto-project.wzb.eu/api/v1/list_core_versions?api_key={self.key}.json"
        self.url_meta_data =f"https://manifesto-project.wzb.eu/api/v1/get_core?api_key={self.key}&key={self.version}"

        self.core_versions = requests.get(self.url_core_versions).json()
        #print(self.core_versions.items())
        self.version_numbers = []
        for k, v in self.core_versions.items():
            for e in v:
                self.version_numbers.append(e['id'])

        logging.info(f"Available version numbers: {self.version_numbers}")

        self.most_recent_version_number = self.version_numbers[-1]
        logging.info(f"The most recent version number is: {self.most_recent_version_number}.\n")

        if self.most_recent_version_number == self.version: logging.info(f"The most recent available version number ({self.most_recent_version_number}) is the same as the version you've initialized ({self.version})")
        else: logging.warn(f"The most recent available version number ({self.most_recent_version_number}) is NOT the same as the version you've initialized ({self.version})")

        self.available_countries = []
        self.available_dates = []

        self.meta_data = pd.DataFrame()
        self.country_meta_data = pd.DataFrame()

        self.start_year = 195000

    def get_meta_data(self):
        data = requests.get(self.url_meta_data).json()
        columns = data[0]
        self.meta_data = pd.DataFrame(data[1:], columns=columns)
        self.available_countries = list(self.meta_data['countryname'].unique())
        logging.info(f"Available countries:\n\n{self.available_countries}")
        self.available_dates = list(self.meta_data['date'].unique() )
      #  logging.info(f"Available time periods:\n\n{self.available_dates}")
        return self.meta_data

    def get_country_specific_df(self, COUNTRY):
        self.country_meta_data = self.meta_data[self.meta_data['countryname'] == COUNTRY]

    def get_country_specific_timeframes(self):
        '''This function gets the range of time periods that's available for a specific country'''
        return list(set(self.country_meta_data['date'][self.country_meta_data['date'].astype(int) > self.start_year]))

    def get_country_specific_parties(self):
        '''This function gets all available parties belonging to a specific country'''
        return dict(zip(self.country_meta_data.party, self.country_meta_data.partyname))

    def get_text_annotations(self):

        results = []
        for timeperiod in self.get_country_specific_timeframes():
            for party_id, partyname in self.get_country_specific_parties().items():
                u = f"https://manifesto-project.wzb.eu/api/v1/texts_and_annotations?api_key={self.key}&keys[]={party_id}_{timeperiod}&version=2020-2"
                data = requests.get(u).json()

                try:
                    df = pd.DataFrame()
                    df = pd.DataFrame(data['items'][0]['items'])
                    df['party_id'] = party_id
                    df['partyname'] = partyname
                    df['timeperiod'] = timeperiod
                    results.append(df)
                    logging.info(f"Succesfully added: {timeperiod}-->{partyname}")
                except:
                    logging.warning(f"This is not here: {timeperiod}-->{partyname}")
                    pass

        return pd.concat(results)

    def get_annotations_per_country(self, COUNTRY):
        self.get_country_specific_df(COUNTRY)
        return self.get_text_annotations()
