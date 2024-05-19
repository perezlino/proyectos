import requests
import json
import logging
import pandas as pd
from datetime import datetime

class Mindicador():

    def __init__(self, indicador):
         self.indicador = indicador

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(funcName)s - %(levelname)s - %(message)s') 
                
    def obtener_ultimos_30_dias(self):                                              
                                                                                            
        try:                                                                                 
            url = f'https://mindicador.cl/api/{self.indicador}'
            response = requests.get(url)
            logging.info('La respuesta fue exitosa')    
            data = json.loads(response.text.encode("utf-8"))                
            df = pd.json_normalize(data, record_path='serie', meta=['codigo','nombre','unidad_medida'])
            df['fecha'] = df['fecha'].apply(lambda x:datetime.fromisoformat(x[:-1]).date())
            df.sort_values('fecha', inplace=True)
            logging.info('Se creó el Dataframe de manera exitosa')   
            return df  
        except Exception as exception_message:
            logging.warning(f'No fue posible obtener una respuesta debido a: {exception_message}') 
        return None
    
    def obtener_anio(self, anio):
        self.anio = anio   

        try:                                                                                 
            url = f'https://mindicador.cl/api/{self.indicador}/{self.anio}'
            response = requests.get(url)
            logging.info('La respuesta fue exitosa')               
            data = json.loads(response.text.encode("utf-8"))
            df = pd.json_normalize(data, record_path='serie')
            df['fecha'] = df['fecha'].apply(lambda x:datetime.fromisoformat(x[:-1]).date())
            df.sort_values('fecha', inplace=True)
            logging.info('Se creó el Dataframe de manera exitosa')             
            return df   
        except Exception as exception_message:
            logging.warning(f'No fue posible obtener una respuesta debido a: {exception_message}') 
        return None            

    def obtener_full_indicadores(self):

        try:                                                                                 
            url = f'https://mindicador.cl/api'
            response = requests.get(url)
            logging.info('La respuesta fue exitosa')                 
            data = json.loads(response.text.encode("utf-8"))                
            df = pd.DataFrame()
            for i in list(data.keys())[3:]:
                fila = pd.json_normalize(data[i])
                df = pd.concat([df , fila])
            logging.info('Se creó el Dataframe de manera exitosa')                 
            return df  
        except Exception as exception_message:
            logging.warning(f'No fue posible obtener una respuesta debido a: {exception_message}') 
        return None


if __name__ == '__main__':

    response = Mindicador()
    response.obtener_ultimos_30_dias(1,'uf')