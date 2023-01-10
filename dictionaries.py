import pandas as pd
def diccionario_rechazos(columna_rechazos):
    df_correccion = pd.read_csv('./data/diccionario_defectos.csv', delimiter=';', encoding='latin-1')
    df_correccion['CORRECCION'] = df_correccion['CORRECCION'].str.upper()
    dicc_corr = dict(zip(df_correccion['ORIGINAL'], df_correccion['CORRECCION']))
    series_corregida = columna_rechazos.map(dicc_corr).fillna(columna_rechazos)
    return series_corregida

def diccionario_mercados(columna_mercados):
    df_correccion = pd.read_csv('./data/diccionario_mercados.csv', delimiter=';', encoding='latin-1')
    dicc_corr = dict(zip(df_correccion['Original'], df_correccion['Ajustado']))
    series_corregida = columna_mercados.map(dicc_corr)
    return series_corregida

def diccionario_claves(columna_claves):
    df_correccion = pd.read_csv('./data/diccionario_claves.csv', delimiter=';', encoding='latin-1')
    dicc_corr = dict(zip(df_correccion['ORIGINAL'], df_correccion['CORRECCION']))
    series_corregida = columna_claves.map(dicc_corr)
    return series_corregida