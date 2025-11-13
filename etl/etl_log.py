import logging

#set logging parameters
logging.basicConfig(filename = 'world_bank_etl.log', 
                    format = '%(asctime)s : %(levelname)s : %(name)s : %(message)s', 
                    level= logging.INFO)

def etl_log(name: str):
    return logging.getLogger(name)
