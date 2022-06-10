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
        self.available_years = []
        self.meta_data = pd.DataFrame()
        self.country_meta_data = pd.DataFrame()
        self.start_year = 1950
        self.start_date = 195000
    
    def get_meta_data(self):
        '''This creates the meta dataframe with the type of datasets that can be retrieved from CMP.
         It returns df with available countries + time periods.'''

        data = requests.get(self.url_meta_data).json()
        self.meta_data = pd.DataFrame(data[1:], columns= data[0])
        self.available_countries = list(self.meta_data['countryname'].unique())
        logging.info(f"Available countries:\n\n{self.available_countries}")
        self.meta_data['edate'] = pd.to_datetime(self.meta_data['edate'])
        self.meta_data['year'] = self.meta_data['edate'].dt.year
        self.available_years = list(self.meta_data['year'].unique() )
        logging.info(f"Available time periods:\n\n{self.available_years}")
        return self.meta_data

    def get_country_specific_df(self, COUNTRY):
        self.country_meta_data = self.meta_data[self.meta_data['countryname'] == COUNTRY]
        logging.info(f"You requested a specific df of the {COUNTRY}.\n\nFor this country, the following years are available: {self.get_country_specific_years()} ")

    def get_country_specific_years(self):
        '''This function gets the range of years that's available for a specific country'''

        return sorted(list(set(self.country_meta_data['year'][self.country_meta_data['year'].astype(int) > self.start_year])))

    def get_country_specific_cmp_years(self):
        '''This function gets the range of time periods that's available for a specific country'''

        return list(set(self.country_meta_data['date'][self.country_meta_data['date'].astype(int) > self.start_date]))

    def get_country_specific_parties(self):
        '''This function gets all available parties belonging to a specific country'''
        return dict(zip(self.country_meta_data.party, self.country_meta_data.partyname))

    def get_text_annotations(self, timeframe):

        results = []

        if timeframe == None:
            logging.info("No timeframe inserted, so instead we retrieve data from all the available time points")
            for y in self.get_country_specific_cmp_years():
    
                for party_id, partyname in self.get_country_specific_parties().items():
                    results.append(self.get_annotations(party_id, partyname, y) ) 

        else: 
            logging.info("Specific timeframes included. Starting to collect data from those timeframes next.")
            #map_years_to_dates = dict(zip(self.country_meta_data.year, self.country_meta_data.date))

            cmp_y = dict(zip(self.country_meta_data.year, self.country_meta_data.date))  #map the input years to the cmp-year format to be able to communicate with the API

            for y in timeframe: 
                #cmp_y = dict(zip(self.country_meta_data.year, self.country_meta_data.date))[y]
                for party_id, partyname in self.get_country_specific_parties().items():
                    results.append(self.get_annotations(party_id, partyname, cmp_y[y]))

        return pd.concat(results)

    def get_annotations(self, party_id, partyname, y):
  
        u = f"https://manifesto-project.wzb.eu/api/v1/texts_and_annotations?api_key={self.key}&keys[]={party_id}_{y}&version=2020-2"
        data = requests.get(u).json()

        try:
            df = pd.DataFrame()
            df = pd.DataFrame(data['items'][0]['items'])
            df['party_id'] = party_id
            df['partyname'] = partyname
            df['timeperiod'] = y
            #results.append(df)
            logging.info(f"Succesfully added: {y}-->{partyname}")
            return df
        except:
            logging.warning(f"This is not here: {y}-->{partyname}")
            pass

         #   return results

        #return pd.concat(results)

    def get_annotations_per_country(self, COUNTRY, timeframe=None):
        self.get_country_specific_df(COUNTRY)
        return self.get_text_annotations(timeframe)
