##############################################################################
# Import necessary modules and files
##############################################################################


import pandas as pd
import os
import sqlite3
from sqlite3 import Error

from constants import *
import city_tier_mapping
from city_tier_mapping import *
from significant_categorical_level import list_platform, list_medium, list_source
from pathlib import Path



###############################################################################
# Define the function to build database
###############################################################################

def build_dbs():
    '''
    This function checks if the db file with specified name is present 
    in the /Assignment/01_data_pipeline/scripts folder. If it is not present it creates 
    the db file with the given name at the given path. 


    INPUTS
        DB_FILE_NAME : Name of the database file 'utils_output.db'
        DB_PATH : path where the db file should exist  


    OUTPUT
    The function returns the following under the given conditions:
        1. If the file exists at the specified path
                prints 'DB Already Exists' and returns 'DB Exists'

        2. If the db file is not present at the specified loction
                prints 'Creating Database' and creates the sqlite db 
                file at the specified path with the specified name and 
                once the db file is created prints 'New DB Created' and 
                returns 'DB created'


    SAMPLE USAGE
        build_dbs()
    '''

    db_file = os.path.join(DB_PATH, DB_FILE_NAME)
    
    print("DB Path : {}".format(DB_PATH))
    print("DB FILENAME : {}".format(DB_FILE_NAME))

    if os.path.exists(db_file):
        print('DB Already Exists')
        return 'DB Exists'
    else:
        print('Creating Database')
        try:
            conn = sqlite3.connect(db_file)
            conn.close()
            print('New DB Created')
            return 'DB created'
        except Exception as e:
            print(f"Failed to create the database: {e}")
            return 'DB creation failed'

###############################################################################
# Define function to load the csv file to the database
###############################################################################

def load_data_into_db():
    '''
    Thie function loads the data present in data directory into the db
    which was created previously.
    It also replaces any null values present in 'toal_leads_dropped' and
    'referred_lead' columns with 0.


    INPUTS
        DB_FILE_NAME : Name of the database file
        DB_PATH : path where the db file should be
        DATA_DIRECTORY : path of the directory where 'leadscoring.csv' 
                        file is present
        

    OUTPUT
        Saves the processed dataframe in the db in a table named 'loaded_data'.
        If the table with the same name already exsists then the function 
        replaces it.


    SAMPLE USAGE
        load_data_into_db()
    '''
    
    db_file = os.path.join(DB_PATH, DB_FILE_NAME)
    data_file = os.path.join(DATA_DIRECTORY, 'leadscoring.csv')
    
    # Read data into a DataFrame
    df = pd.read_csv(data_file)
    
    # Replace null values in specific columns
    df['total_leads_droppped'].fillna(0, inplace=True)
    df['referred_lead'].fillna(0, inplace=True)

    # Connect to the SQLite database and save DataFrame to a table named 'loaded_data'
    conn = sqlite3.connect(db_file)
    df.to_sql('loaded_data', conn, if_exists='replace', index=False)
    
    print('Load Data in to DB is completed .... ')

    conn.close()


###############################################################################
# Define function to map cities to their respective tiers
###############################################################################

    
def map_city_tier():
    '''
    This function maps all the cities to their respective tier as per the
    mappings provided in the city_tier_mapping.py file. If a
    particular city's tier isn't mapped(present) in the city_tier_mapping.py 
    file then the function maps that particular city to 3.0 which represents
    tier-3.


    INPUTS
        DB_FILE_NAME : Name of the database file
        DB_PATH : path where the db file should be
        city_tier_mapping : a dictionary that maps the cities to their tier

    
    OUTPUT
        Saves the processed dataframe in the db in a table named
        'city_tier_mapped'. If the table with the same name already 
        exsists then the function replaces it.

    
    SAMPLE USAGE
        map_city_tier()

    '''
    
    conn = sqlite3.connect(DB_FILE_NAME)

    # Check if 'loaded_data' table exists in the database
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='loaded_data'")
    exists = cursor.fetchone()

    if exists:
        # Fetch data from the 'loaded_data' table
        df = pd.read_sql_query("SELECT * FROM loaded_data", conn)

        # Mapping cities to their respective tiers using city_tier_mapping dictionary
        df['city_tier'] = df['city_mapped'].map(city_tier_mapping)

        # Replace unmapped cities with tier 3.0
        df['city_tier'].fillna(3.0, inplace=True)

        # Save the processed dataframe to a new table 'city_tier_mapped'
        df.to_sql('city_tier_mapped', conn, if_exists='replace', index=False)
        
        print('Mapping City to Tier is completed .... ')


        conn.close()

###############################################################################
# Define function to map insignificant categorial variables to "others"
###############################################################################


def map_categorical_vars():
    '''
    This function maps all the insignificant variables present in 'first_platform_c'
    'first_utm_medium_c' and 'first_utm_source_c'. The list of significant variables
    should be stored in a python file in the 'significant_categorical_level.py' 
    so that it can be imported as a variable in utils file.
    

    INPUTS
        DB_FILE_NAME : Name of the database file
        DB_PATH : path where the db file should be present
        list_platform : list of all the significant platform.
        list_medium : list of all the significat medium
        list_source : list of all rhe significant source

        **NOTE : list_platform, list_medium & list_source are all constants and
                 must be stored in 'significant_categorical_level.py'
                 file. The significant levels are calculated by taking top 90
                 percentils of all the levels. For more information refer
                 'data_cleaning.ipynb' notebook.
  

    OUTPUT
        Saves the processed dataframe in the db in a table named
        'categorical_variables_mapped'. If the table with the same name already 
        exsists then the function replaces it.

    
    SAMPLE USAGE
        map_categorical_vars()
    '''

    db_file = os.path.join(DB_PATH, DB_FILE_NAME)

    # Connect to the SQLite database
    conn = sqlite3.connect(db_file)

    # Fetch data from the 'loaded_data' table
    df = pd.read_sql_query("SELECT * FROM loaded_data", conn)

    # Mapping insignificant categorical variables
    df.loc[~df['first_platform_c'].isin(list_platform), 'first_platform_c'] = 'Other'
    df.loc[~df['first_utm_medium_c'].isin(list_medium), 'first_utm_medium_c'] = 'Other'
    df.loc[~df['first_utm_source_c'].isin(list_source), 'first_utm_source_c'] = 'Other'

    # Save the processed dataframe to a new table 'categorical_variables_mapped'
    df.to_sql('categorical_variables_mapped', conn, if_exists='replace', index=False)
    
    print('Mapping  categorical variables is completed .... ')

    conn.close()

##############################################################################
# Define function that maps interaction columns into 4 types of interactions
##############################################################################
def interactions_mapping():
    '''
    This function maps the interaction columns into 4 unique interaction columns
    These mappings are present in 'interaction_mapping.csv' file. 


    INPUTS
        DB_FILE_NAME: Name of the database file
        DB_PATH : path where the db file should be present
        INTERACTION_MAPPING : path to the csv file containing interaction's
                                   mappings
        INDEX_COLUMNS_TRAINING : list of columns to be used as index while pivoting and
                                 unpivoting during training
        INDEX_COLUMNS_INFERENCE: list of columns to be used as index while pivoting and
                                 unpivoting during inference
        NOT_FEATURES: Features which have less significance and needs to be dropped
                                 
        NOTE : Since while inference we will not have 'app_complete_flag' which is
        our label, we will have to exculde it from our features list. It is recommended 
        that you use an if loop and check if 'app_complete_flag' is present in 
        'categorical_variables_mapped' table and if it is present pass a list with 
        'app_complete_flag' column, or else pass a list without 'app_complete_flag'
        column.

    
    OUTPUT
        Saves the processed dataframe in the db in a table named 
        'interactions_mapped'. If the table with the same name already exsists then 
        the function replaces it.
        
        It also drops all the features that are not requried for training model and 
        writes it in a table named 'model_input'


    
    SAMPLE USAGE
        interactions_mapping()
    '''
    
#     db_file = os.path.join(DB_PATH, DB_FILE_NAME)

#     # Connect to the SQLite database
#     conn = sqlite3.connect(db_file)

#     # Fetch data from the 'categorical_variables_mapped' table
#     df = pd.read_sql_query("SELECT * FROM categorical_variables_mapped", conn)

#     # Load interaction mappings from the CSV file
#     interaction_map = pd.read_csv(INTERACTION_MAPPING)

#     # Merge interaction mappings with the original data
#     df = pd.merge(df, interaction_map, how='left', on='interaction_type')

#     # Drop columns not needed for training model
#     if NOT_FEATURES:
#         df = df.drop(columns=NOT_FEATURES)

#     # Save the processed dataframe to 'interactions_mapped' table
#     df.to_sql('interactions_mapped', conn, if_exists='replace', index=False)

#     # Write the required features for model input to 'model_input' table
#     if INDEX_COLUMNS_TRAINING:
#         df_features = df.set_index(INDEX_COLUMNS_TRAINING)
#         df_features.to_sql('model_input', conn, if_exists='replace')

#     conn.close()
    
    #cnx = sqlite3.connect(DB_PATH+DB_FILE_NAME)
    db_file = os.path.join(DB_PATH, DB_FILE_NAME)

    # Connect to the SQLite database
    cnx = sqlite3.connect(db_file)

    df = pd.read_sql('select * from categorical_variables_mapped', cnx)
    
    df_event_mapping = pd.read_csv(INTERACTION_MAPPING, index_col=[0])
    
    df_unpivot = pd.melt(df, id_vars=['created_date', 'first_platform_c',
       'first_utm_medium_c', 'first_utm_source_c', 'total_leads_droppped',
       'referred_lead', 'app_complete_flag'], var_name='interaction_type', value_name='interaction_value')
    
    df_unpivot['interaction_value'] = df_unpivot['interaction_value'].fillna(0)
    
    df = pd.merge(df_unpivot, df_event_mapping, on='interaction_type', how='left')
    
    df = df.drop(['interaction_type'], axis=1)
    
    df_pivot = df.pivot_table(
        values='interaction_value', index=['created_date', 'first_platform_c',
       'first_utm_medium_c', 'first_utm_source_c', 'total_leads_droppped',
       'referred_lead', 'app_complete_flag'], columns='interaction_mapping', aggfunc='sum')
    
    df_pivot = df_pivot.reset_index()

    df_pivot.to_sql(name='interactions_mapped', con=cnx, if_exists='replace', index=False)
    
    print('Interaction Mapping is completed .... ')

    cnx.close()
