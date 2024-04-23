import pandas as pd
import numpy as np
import os

def calidad_datos(data):
    tipos = pd.DataFrame({'tipo': data.dtypes}, index=data.columns)
    na = pd.DataFrame({'nulos': data.isna().sum()}, index=data.columns)
    na_prop = pd.DataFrame({'porc_nulos': data.isna().sum()/data.shape[0]}, index=data.columns) # data.shape : (144, n° columnas) --> data.shape[0] : 144
    # Ejemplo: df.loc[df['year']==0].shape[0] --> Accede a los registros donde la columna 'year' tenga valor igual a 0 y luego con shape[0] nos devuelve la cantidad de esos valores,
    # dado que shape[0] nos regresa el primer valor de la tupla de dimension de esta serie, que corresponde al numero de filas 
    ceros = pd.DataFrame({'ceros':[data.loc[data[col]==0].shape[0] for col in data.columns]}, index=data.columns)
    ceros_prop = pd.DataFrame({'porc_ceros':[data.loc[data[col]==0].shape[0]/data.shape[0] for col in data.columns]}, index=data.columns)
    summary = data.describe(include='all').T

    summary['limit_inf'] = summary['mean'] - summary['std'] * 1.5
    summary['limit_sup'] = summary['mean'] + summary['std'] * 1.5

    summary['outliers'] = data.apply(lambda x: sum(np.where((x < summary['limit_inf'][x.name]) | (x > summary['limit_sup'][x.name]), 1, 0)) if x.name in summary['limit_inf'].dropna().index else 0)

    return pd.concat([tipos, na, na_prop, ceros, ceros_prop, summary], axis=1).sort_values('tipo')