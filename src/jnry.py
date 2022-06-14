import logging
import pandas as pd
from datetime import datetime
import country_converter as coco
from dateutil import parser
from fuzzywuzzy import fuzz

## set logging
logging.getLogger().setLevel(logging.INFO)

class Jnry():
    
    def __init__(self, country, df = None, path_to_file=None, year=None, source=None):
        
        #self.MINUM_FUZZ_MATCH = 80 # decrease if you want to allow for less strict matching. 
        
        if path_to_file ==None:
            self.df = df
        else:
            self.df = pd.read_csv(path_to_file)
            
        self.year = year
        self.country = country
        self.source = source
        
        if self.source == None:
            logging.info("You did not specify the source of the datafile. Please be aware that this code was developped and tested on files from CAP or CMP.")
        else:
            logging.info(f"Processing a {self.source} file with data from {self.country}.")
        
        self.jnry_country = self.get_jnry_country()
        self.party_facts = self.get_party_facts()
        
        if self.source =='CMP':
            
            self.target_politicalparties = 'partyname'
            self.df['year']  = self.df['timeperiod'].apply(lambda x: datetime.strptime(str(x), '%Y%m').date().strftime("%Y") )

        elif self.source == 'CAP':
            self.target_politicalparties = 'politicalparty'
            
        self.pf = self.get_party_facts()
        self.pf_columns = ['name_short', 'name', 'name_english', 'name_other']
        
    def get_jnry_year(self, yr):
        if yr is None:
            logging.warning("Input year is missing")
            return datetime.now().year
        else:
            try:
                return parser.parse(str(int(yr))).year
            except:
                logging.warning(f"Can't properly parse the year in the target file, which is: {yr}; instead returning the current year: {datetime.now().year}")
                return datetime.now().year
                
    def get_jnry_country(self):
        try:
            return coco.convert(self.country, to='ISO3')
        except:
            logging.warning("Can't properly parse the country variable to ISO3 format")
            return self.jnry_country
        
    def get_unique_country_year_combinations(self):
        
        '''returns a tuple of the unique year * pol party combinations in the target df, 
        that should be matched with the PF df'''

        tuples_polparties = self.df[[self.target_politicalparties, 'year']].dropna()
        d = tuples_polparties.groupby([self.target_politicalparties, 'year']).size().reset_index()
        return list(zip(d[self.target_politicalparties], d.year))

        
    def get_party_facts(self):
        
        PF_URL = "https://partyfacts.herokuapp.com/download/core-parties-csv/"
        pf = pd.read_csv(f"{PF_URL}")
        return pf[pf.loc[:,'country'] == self.jnry_country]
    
    def get_pf_slice(self, yr):
        
        """This gets a relevant slice of the PartyFacts df;
        It filters the df based on the timeframe in the target dataset, to find the names that are most relevant.
        It returns a list of political partynames that are candidates for matching (based on country + timeframe)"""
                    
        d = self.party_facts[(self.party_facts.loc[:,'year_last'] >= yr ) | (self.party_facts.loc[:,'year_first'] < yr )] 

        # the last year that the name was used, should be in larger than or equal to the target year.
        # the first year that the name was used, should be smaller than (before) the target year
        # if the last year that the name was used is AFTER the target date,
        
        logging.info(f"the length of the slice of the partyfact data is: {len(d)}") # WHAT IF LEN > 1?!
                    
        party_fact_names = d[self.pf_columns].values.tolist()
        return d, party_fact_names
        
    
    def extract_rows(self, column_index, match, d):
        
        column = self.pf_columns[column_index]   
   
        partyfacts_id = d[d[column] == match]['partyfacts_id'].values[0]
        partyfacts_name = d[d[column] == match]['name'].values[0]
        partyfacts_name_short = d[d[column] == match]['name_short'].values[0]
        partyfacts_name_english =  d[d[column] == match]['name_english'].values[0] 
        wikipedia = d[d[column] == match]['wikipedia'].values[0]
        
        return column, partyfacts_id, partyfacts_name, partyfacts_name_short, partyfacts_name_english, wikipedia
                        
    def get_party_facts_ids(self):
        
        logging.info("exact and fuzzy matching will start now for the following target data:")
        logging.info([f"{k} : {v}" for k, v in self.get_unique_country_year_combinations()] )
        
        final_results = []
        
        for polparty, year in self.get_unique_country_year_combinations(): 
            yr = self.get_jnry_year(year)
            logging.info(f"Starting matching for {polparty}.......")
            
            d, party_fact_names = self.get_pf_slice(yr)
    
            exact_match = [[(idx, element) for idx, element in enumerate(i) if element ==polparty] for i in party_fact_names]
            non_empty = [x for x in exact_match if len(x) > 0]
            type_of_match='exact'
            highest = None
            
            if len(non_empty) > 0:
                idx = non_empty[0][0][0]   
                match = non_empty[0][0][1] 
                
                logging.info(f"Exact match{polparty} <---> {match}(= PF name)")
                column, partyfacts_id, partyfacts_name, partyfacts_name_short, partyfacts_name_english, wikipedia = self.extract_rows(idx, match, d)
            
            else: # no exact match found.
                
                fuzzy_matches = [[fuzz.ratio(polparty, name) if type(name) is str else 0 for name in i ]  for i in party_fact_names] 
                fuzzy_matches_names = [[(name, fuzz.ratio(polparty, name)) for name in i if type(name) is str ]  for i in party_fact_names] 
  
                highest = max(map(max, fuzzy_matches))

                idx = [[(idx, value) for idx, value in enumerate(innerlist) if value == highest] for innerlist in fuzzy_matches]
                non_empty = [x for x in idx if len(x) > 0]
                index = non_empty[0][0][0]
                
                polp = [[i[0] for i in x if i[1] == highest and len(i)>0] for x in fuzzy_matches_names]
                name_match = [x for x in polp if len(x) > 0][0][0]
                logging.info(f"Fuzzy match {polparty} <---> {name_match}(= PF name), {highest}% certainty.")
                
                column, partyfacts_id, partyfacts_name, partyfacts_name_short, partyfacts_name_english, wikipedia = self.extract_rows(index, name_match, d)
                type_of_match = 'fuzzy'
            
            results = {'year': year, 
                    'jnry_year' : yr , 
                     self.target_politicalparties : polparty, 
                    'country_iso' : self.jnry_country,
                    'partyfacts_id' : partyfacts_id,
                    'partyfacts_name' : partyfacts_name,
                    'partyfacts_name_short' : partyfacts_name_short,
                    'partyfacts_name_english' : partyfacts_name_english,
                    'wikipedia' : wikipedia,
                    'type_of_match': type_of_match ,
                    'certainty': highest , 
                    'match_found_with_pf_column': column}
            
            final_results.append(results)
    
        return final_results

    def merge_party_facts_with_target(self):
        jnry_data = pd.DataFrame(self.get_party_facts_ids())
        jnry = self.df.merge(jnry_data, on=['year', self.target_politicalparties], how='left')
        return jnry