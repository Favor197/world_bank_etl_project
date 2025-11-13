import pandas as pd
from etl_log import etl_log

#call logging function
log = etl_log('Merge')

def ingest(flat_file: str):
    """
    Ingest and clean all_countries csv file
    """
    #ingest flat file
    log.info(f'Ingesting {flat_file} file......')
    df_clean = pd.read_csv(flat_file)

    #correct wrong iso3 values
    df_clean['iso3'] =df_clean['iso3'].replace('UNK','XKX')

    return df_clean

def merge(df_research: pd.DataFrame, flat_file: str):
    """
    Merge two datasets to get one final dataset
    """
    log.info('Dataset merging process ongoing......')
    df_country = ingest(flat_file)
    df = df_research.merge(df_country, 
                           left_on='countryiso3code', 
                           right_on ='iso3', 
                           how='left')
    log.info('All dataset merged successfully: '+ str(df.shape))
    df.info()
    #return final dataset
    return df 