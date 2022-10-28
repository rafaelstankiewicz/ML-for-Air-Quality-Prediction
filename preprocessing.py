import pandas as pd
import numpy as np

class Processor():
    """
    Class to preprocess AQS data in specified format to feed into models.
    """
    
    def __init__(self):
        pass

    def project_unique(self, df, measurement, verbose=False):
        """
        Keep only columns that have 2 of more unique values.
        """
        cols_dict = {col: df[col].nunique() for col in df.columns}
        all_cols = {k: v for k,v in cols_dict.items() if v <= 1}
        kept_cols = {k: v for k,v in cols_dict.items() if v > 1}
        
        # Keep only variables we care about (changing) 
        df = df[kept_cols.keys()].copy()
        df['datetime'] = pd.to_datetime(df['date_local'] + ' ' + df['time_local'])
        df.set_index('datetime', inplace=True)
        df = df.drop(['date_gmt', 'time_gmt', 'date_local', 'time_local'], axis=1)

        # NOTE: Should I drop this?
        if 'date_of_last_change' in df.columns:
            df = df.drop(['date_of_last_change'], axis=1)

        if verbose:
            print('Kept the following columns:')
            print(df.columns)
            print()
            print('Removed the following columns:')
            print([col for col in all_cols if col not in df.columns])
            print()

        df = df.rename({'sample_measurement': measurement}, axis=1)

        return df
    
    
    def process(self, df, measurement, change_freq=False, select_method=False, drop_lat_lon=True, remove_duplicates=False):
        '''
        Generates a more concise version of the dataset.

        Parameters:
            df: dataframe -- the dataframe of raw data for one parameter
            measurement: -- the code for the parameter for the dataframe
        
        Returns:
            HTTP Response Data: json or pd.DataFrame

        '''
        if select_method:
            df = df.loc[df['method'] == df['method'].unique()[0]].copy()

        # converts into data time and renames measurement
        df['datetime'] = pd.to_datetime(df['date_local'] + ' ' + df['time_local'])
        df = df[['datetime', 'sample_measurement', 'latitude', 'longitude', 'sample_duration', 'qualifier']]
        df = df.rename({'sample_measurement': measurement}, axis=1)
        # re-does the qualifier column so the parameter qualifiers will all appear differently
        qualifier_rename = measurement + " - qualifier"
        df = df.rename({'qualifier': qualifier_rename}, axis=1)

        # selects only hourly data
        df = df[df['sample_duration'] == "1 HOUR"]
        df = df.drop(['sample_duration'], axis=1)
        if df.empty:
            print(f"No hourly data for {measurement} (pulled data)")
            df = df.drop(['latitude', 'longitude', measurement, qualifier_rename], axis=1)
            return df
        
        df.set_index(['datetime'], inplace=True)

        if change_freq: 
            print(df.head())
            df = df.asfreq('1h', method='ffill')
        if drop_lat_lon:
            df = df.drop(['latitude', 'longitude'], axis=1)

        return df
        
    def join(self, dfs, code_names):
        '''
        Joins the concatanated data springs

        Parameters
            dfs: dataframe -- holds the concatted data
            code_names: [String] -- has a list of all parameters that were successfully pulled

        Returns:
            Dataframe with all columns lined up to equivalent time stamp
        '''
        df = dfs[0].join(dfs[1:], how='outer')
        df = df.drop([x for x in df.columns if (('latitude' in x) and (x != 'latitude'))], axis=1)
        df = df.drop([x for x in df.columns if (('longitude' in x) and (x != 'longitude'))], axis=1)

        # creates aggregation functions that deal with numeric and non-numeric data
        qualifier_names = [(x + " - qualifier") for x in code_names]
        funcs = {**{x: 'mean' for x in code_names}, **{x: 'first' for x in qualifier_names}}
        df = df.resample('1h').agg(funcs)
        return df
