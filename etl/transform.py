import pandas as pd
from typing import List
from etl_log import etl_log

#call logging function
log = etl_log('Transform')

def normalize(df: pd.DataFrame, col: str):
    """
    Normalize nested columns
    """
    #for col in cols:
        #flatten nested columns
    df_norm = pd.json_normalize(df[col]).add_prefix(f'{col}_')

        #drop nested columns and join flat columns
    df_clean = df.drop(columns=[col]).join(df_norm)
    
    log.info('Data normalization process complete')
    return df_clean


def transform(extracted_data: List):
    """
    Transform and clean extracted data
    """
    log.info('Data transformation process ongoing......')
    df_research = pd.DataFrame(extracted_data)

    #drop columns not needed
    df_research = df_research.drop(columns=['unit', 'obs_status','decimal'])

    #call normalize function to flatten nested columns
    df_research = normalize(df_research, 'indicator')
    df_research = normalize(df_research, 'country')

    log.info('Data transformation process complete')
    return df_research
