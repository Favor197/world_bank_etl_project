import pandas as pd
from typing import List
from etl_log import etl_log

#call logging function
log = etl_log('Load')

def load(df: pd.DataFrame, extracted_data: List):
    """
    Save extracted data in json format and final dataset as csv file
    """
    #save original World Bank API data in json format
    initial_data = pd.DataFrame(extracted_data)
    initial_data.to_json('data/research_spend.json', orient='records', indent=2)
    log.info('World Bank API response data saved as data/research_spend.json')

    #save final dataset as csv file
    df.to_csv('data/research_spend.csv', index=False)
    log.info('Final dataset saved as data/research_spend.csv')
    print('ETL process complete')