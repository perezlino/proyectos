import requests
import json
import logging
import pandas as pd

class RandomUser_Api():

    def __init__(self):
        pass

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(funcName)s - %(levelname)s - %(message)s') 
                
    def obtener_random_user(self):                                                
                                                                                            
        try:                                                                                 
            response = requests.get(f'https://randomuser.me/api/')
            logging.info('La respuesta fue exitosa') 
            #return response.json()
            #return json.loads(response.text.encode("utf-8"))
            return json.loads(response.text)    
        except Exception as exception_message:
            logging.warning(f'No fue posible obtener una respuesta debido a: {exception_message}') 
        return None

    def devolverDataframe(self):
        try:
            response = self.obtener_random_user()
            #respuesta = json.loads(response.text.encode("utf-8"))
            respuesta = response['results'][0]               
            df = pd.json_normalize({'firstname':respuesta['name']['first'],
                                    'lastname':respuesta['name']['last'],
                                    'country':respuesta['location']['country'],
                                    'username':respuesta['login']['username'],
                                    'password':respuesta['login']['password'],
                                    'email':respuesta['email']}) 
            logging.info('Se creó el Dataframe exitosamente')             
            return df
        except Exception as exception_message:
            logging.warning(f'No fue posible crear el Dataframe debido a: {exception_message}')
        return None

    def exportarCSV(self):

        try:
            self.devolverDataframe().to_csv("random_user.csv", index=False)
            logging.info('Se exportó el Dataframe exitosamente')
        except Exception as exception_message:
            logging.warning(f'No fue posible exportar el Dataframe a CSV debido a: {exception_message}')
        return None  

if __name__ == '__main__':

    response = RandomUser_Api()
    logging.info(response.get_random_user())  