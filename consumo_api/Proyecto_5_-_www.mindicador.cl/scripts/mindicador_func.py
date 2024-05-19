import requests
import json
import logging
import pandas as pd

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(funcName)s - %(levelname)s - %(message)s') 
               
def obtener_indicadores():                                                

    try:                                                                                 
        response = requests.get(f'https://mindicador.cl/api/')
        logging.info('La respuesta fue exitosa') 
        #return response.json()
        #return json.loads(response.text.encode("utf-8"))
        return json.loads(response.text)    
    except Exception as exception_message:
        logging.warning(f'No fue posible obtener una respuesta debido a: {exception_message}') 
    return None

def devolverDataframe(respuesta):

    try:
        response = respuesta
        list(response.keys())[3:]
        df = pd.DataFrame()
        for i in list(respuesta.keys())[3:]:
            fila = pd.json_normalize(response[i])
            df = pd.concat([df, fila])         

        logging.info('Se creó el Dataframe exitosamente')             
        return df
    except Exception as exception_message:
        logging.warning(f'No fue posible crear el Dataframe debido a: {exception_message}')
    return None

def exportarCSV(df):

    try:
        df.to_csv("indicadores.csv", index=False)
        logging.info('Se exportó el Dataframe exitosamente')
    except Exception as exception_message:
        logging.warning(f'No fue posible exportar el Dataframe a CSV debido a: {exception_message}')
    return None

def carga_api_a_csv_main():
    respuesta = obtener_indicadores()
    main_df = devolverDataframe(respuesta)
    exportarCSV(main_df)

if __name__ == '__main__':
    carga_api_a_csv_main()