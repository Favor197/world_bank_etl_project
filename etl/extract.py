import requests
from etl_log import etl_log

#call logging function
log = etl_log('Extract')

def extract(url: str):
    """
    Extract multiple pages of data from World Bank API, using a single indicator
    """

    page=1
    all_response = []

    log.info('World Bank API extraction commenced')

    #iterate over all available pages 
    while True:
        url2 =f'{url}?format=json&per_page=1000&date=1990:2025&page={page}'
        response =requests.get(url2)
        data = response.json()

        #exit loop if data returns empty
        if not data[1]:
            log.info('All records has been extracted successfully')
            break

        #append extracted records to all_response list
        all_response.extend(data[1])

        print(f'Getting page {page}-----')
        page +=1

    log.info(f'{len(all_response)} records extracted')

    #return all extracted data
    return all_response