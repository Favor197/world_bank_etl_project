from extract import extract
from transform import transform
from merge import merge
from load import load

def etl():
    """
    Call all functions to run ETL pipeline
    """
    url = 'https://api.worldbank.org/v2/country/all/indicator/GB.XPD.RSDV.GD.ZS'
    flat_file = 'data/all_countries.csv'

    #call extract function
    extracted_data = extract(url)

    #call transform function
    transfromed_data = transform(extracted_data)

    #call merge function
    merged_data = merge(transfromed_data, flat_file)

    #call load function
    load(merged_data, extracted_data)

if __name__ =='__main__':
    #call etl function to run etl pipeline
    etl()
