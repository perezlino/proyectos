import requests
import json
import logging
import pandas as pd

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(funcName)s - %(levelname)s - %(message)s',  
                    filename='logs.log',                                                
                    filemode='a') 
               
def get_random_user():                               
    logging.debug('Entramos a la función get_random_user')                   

    try:                                                                                 
        response = requests.get(f'https://randomuser.me/api/')
        logging.info('La respuesta fue exitosa') 
        #return response.json()
        #return json.loads(response.text.encode("utf-8"))
        return json.loads(response.text)    
    except Exception as exception_message:
        logging.warning(f'No fue posible obtener una respuesta: {exception_message}') 
    return None

def devolverDataframe():
    response = get_random_user()
    #respuesta = json.loads(response.text.encode("utf-8"))
    respuesta = response['results'][0]               
    df = pd.json_normalize({'firstname':respuesta['name']['first'],
                            'lastname':respuesta['name']['last'],
                            'country':respuesta['location']['country'],
                            'username':respuesta['login']['username'],
                            'password':respuesta['login']['password'],
                            'email':respuesta['email']})             
    return df

def exportarCSV():
    devolverDataframe().to_csv("random_user_b.csv", index=False)

if __name__ == '__main__':

    response = get_random_user()
    logging.debug('Obtenemos una respuesta')  

    logging.info(response)  