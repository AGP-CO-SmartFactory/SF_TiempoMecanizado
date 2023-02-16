# -*- coding: utf-8 -*-
"""
Created on Thu Feb  9 12:19:07 2023

@author: jpgarcia
"""

import pandas as pd 
import sqlite3
import numpy as np

cnx = sqlite3.connect('char_zfer.db')

df_zfer = pd.read_sql('SELECT * from ZFER_CHARS_CO', cnx)
df_zfer1 = df_zfer[df_zfer['POS'] == '0100']
df_zfer2 = df_zfer[df_zfer['POS'] == '0105']
df_zfer = pd.concat([df_zfer1, df_zfer2]).sort_values(by=['ZFER', 'POS'])
df_zfer = df_zfer[df_zfer['Acero'] == 1].reset_index()

x = df_zfer.loc[0]

def comparacion(x):
    df = df_zfer[df_zfer['ZFER'] == x['ZFER']]
    try:
        fila1 = df[0:1]['DXF_AREA'].values[0]
        fila2 = df[1:2]['DXF_AREA'].values[0]
    except:
        x['Error PRO'] = 1
    else:
        r = fila1-fila2
        if fila1 < fila2:
            x['Comparación'] = 1
    finally:
        return x
    
df_zfer = df_zfer.apply(comparacion, axis=1)
df_final = df_zfer[df_zfer['Comparación'] == 1].sort_values(['ZFER', 'POS'])
df_error = df_zfer[df_zfer['Error PRO'] == 1]
df_final.to_excel('error pu 105.xlsx')
