"""
Get relevant CAP datafiles from the Netherlands.
In addition, it will assign some major topic codes.
output: dataframe with combined data + topic codes.
"""

import pandas as pd
import json
import numpy as np

class Get_Cap_Data():
    def __init__(self, path_to_data, path_to_cap_topics, path_to_df_selection):
        self.path_to_data = path_to_data

        # major topic scores that we will use here can be found in the .json file in this folder:
        with open(path_to_cap_topics) as json_file:
            self.topic_codes = json.load(json_file)

        # data files that we will be using here (dutch)
        with open(path_to_df_selection) as json_file:
            self.files = json.load(json_file)


    def get_cap_files(self):
        dataframes = []
        for i, k in self.files.items():
            df = pd.read_excel(f'{self.path_to_data}{k}')
            df['source'] = i
            dataframes.append(df)
        df = pd.concat(dataframes)

        # now add the major topic scores
        for k, v in self.topic_codes.items():
            df[k] = np.where(df['majortopic'] == v, 1, 0)

        return df
