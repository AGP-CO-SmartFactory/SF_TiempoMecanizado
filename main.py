


# dxf_normal = ezdxf.readfile(filename='15981A100.dxf')
# dxf_gunport = ezdxf.readfile(filename='MAQ156751A101.dxf')
# dxf_gunport1 = ezdxf.readfile(filename='MAQBH156751A401.dxf')
# dxf_gunport2 = ezdxf.readfile(filename='MAQZ1036A101.dxf')

# msp_normal = dxf_normal.modelspace()
# msp_gunport = dxf_gunport.modelspace()
# msp_gunport1 = dxf_gunport1.modelspace()
# msp_gunport2 = dxf_gunport2.modelspace()

# print("NORMAL - 1 POLYLINE EXPECTED")
# for e in msp_normal:
#     print(e)
    
# print("\nGUNPORT - 2 ENTITIES EXPECTED")
# for e in msp_gunport:    
#     print(e)
    
# print("\nGUNPORT - 2 ENTITIES EXPECTED")
# for e in msp_gunport1:    
#     print(e)
    
# print("\nGUNPORT - 2 ENTITIES EXPECTED")
# for e in msp_gunport2:    
#     print(e)


"""
Created on Nov 21 2022
This code calculates the real cost of a rejection based on rejection type and 
the real area for each .dxf modulation file, either PC, TPU or Glass.
@author: jpgarcia
"""
from dotenv import load_dotenv
import pandas as pd
import numpy as np
import pyodbc
import glob
from connections import Connection
import dataframes
import os
import dictionaries
import ezdxf
# Creating constants

# Loading test environment
load_dotenv('connection.env')

def create_connection(connection):
    conn = pyodbc.connect(f'DRIVER={connection.driver}'
                            f'SERVER={connection.server}'
                            f'DATABASE={connection.database}'
                            f'UID={connection.uid}'
                            f'PWD={connection.pwd}'
                            f'Trusted_Connection=no;')
    return conn

# SQL Server connection
WR_conn = Connection(identifier=1)
mats_conn = Connection(identifier=2)


conn_parameters_list = [WR_conn, mats_conn]
conn_list = []

# Creating a PYODBC object for each of the parameters created 
for connection in conn_parameters_list:
    conn = create_connection(connection)
    conn_list.append(conn)

print('Descargando información...\n')
df_WR = dataframes.Dataframe(conn_list[0], 1).dataframe
df_mats = dataframes.Dataframe(conn_list[1], 0).dataframe

# Obtener todos los archivos que tengan MAQ
df_maq = df_WR[df_WR['DXF_NAME'].str.startswith('MAQ', 'PERF')]

