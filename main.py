
"""
Created on Nov 21 2022
This code calculates the real cost of a rejection based on rejection type and 
the real area for each .dxf modulation file, either PC, TPU or Glass.
@author: jpgarcia
"""
from dotenv import load_dotenv
import pandas as pd
import numpy as np
import pyodbc, glob, shutil
from connections import Connection
from dataframes import Dataframe
import diccionario_claves
import ezdxf
from sqlalchemy import create_engine, types
from dxfasc import polyPerimeter, polyArea
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

def area_perimetro(x):
    print(x)
    try:
        msp = ezdxf.readfile(x['Desenho_Ruta']).modelspace()
    except:
        return x
    else:
        entities = 0    
        area = 0
        perim = 0
        for e in msp:
            if len(msp) > 6:
                break
            entities+=1
            e_type = str(type(e))
            # Especificación para elementos con polilineas
            if e_type == "<class 'ezdxf.entities.polyline.Polyline'>":
                poly_vert = []
                for i in range(len(e)):
                    poly_vert.append(e[i].format('xy'))
                polyarea = polyArea(np.array(poly_vert))/1000000
                polyperi = polyPerimeter(np.array(poly_vert))
                if polyarea > area:
                    area = polyarea
                if polyperi > perim:
                    perim = polyperi                    
            elif e_type == "<class 'ezdxf.entities.lwpolyline.LWPolyline'>":
                polyarea = polyArea(np.array(list(e.vertices())))/1000000
                if polyarea > area:
                    area = polyarea
        x['Area_calc'] = area
        x['Peri_calc'] = perim
        return x
def caja_perforacion(x):
    msp = ezdxf.readfile(x['Desenho_Ruta']).modelspace()
    entities = len(msp)
    if entities <= 1:
        x['Perforacion'] = 0
        x['Caja'] = 1
    elif entities > 1:
        x['Perforacion'] = 1
        x['Caja'] = 0
    return x  
  
# SQL Server connection
WR_conn = Connection(identifier=1)
saga_conn = Connection(identifier=2)


conn_parameters_list = [WR_conn, saga_conn]
conn_list = []

# Creating a PYODBC object for each of the parameters created 
for connection in conn_parameters_list:
    conn = create_connection(connection)
    conn_list.append(conn)

print('Descargando información...\n')
df_WR = Dataframe(conn_list[0], 1).dataframe
df_mats = Dataframe(conn_list[1], 0).dataframe
df_mats = df_mats.drop(labels='Ordem_Serial', axis=1)
df_mats = df_mats.sort_values(by='Ordem_CodMaterial', ascending=True).drop_duplicates('Ordem_CodMaterial')
df_zferlist = Dataframe(conn_list[1], 2).dataframe

try: 
    dxf = pd.read_csv('./data/dxf_list.csv')
    
except:
    all_directories = glob.glob(r'\\192.168.2.2\cnc-revisados\**')
    ignored_directories = glob.glob(r'\\192.168.2.2\cnc-revisados\Accesos - NO BORRAR\**')
    all_dir_names = []
    ignored_dir_names = []
    
    for directory in all_directories:
        name = directory.split('\\')[4]
        all_dir_names.append(name)
    
    for directory in ignored_directories:
        name = directory.split('\\')[5].split(' - Acceso directo.lnk')[0]
        ignored_dir_names.append(name)
        
    # Removiendo las carpetas no deseadas de la carpeta de cnc-revisados
    carpetas = []
    for name in all_dir_names:
        if name not in ignored_dir_names:
            carpetas.append(name)
            
    archivos_dxf = []
    for carpeta in carpetas:
        print(carpeta)
        lista_rutas = glob.glob(fr'\\192.168.2.2\cnc-revisados\{carpeta}\**\*.dxf', recursive=True)
        if len(lista_rutas) > 0:
            for ruta in lista_rutas:
                archivos_dxf.append(ruta)
    
    dxf = []
    for ruta in archivos_dxf:
        nombre_dxf = ruta.split('\\')[-1].split('.dxf')[0]
        diccionario_dxf = {'Desenho_Name': nombre_dxf,
                           'Desenho_Ruta': ruta}
        dxf.append(diccionario_dxf)
    
    dxf = pd.DataFrame(dxf)
    dxf = dxf[np.where(dxf['Desenho_Ruta'].str.contains('OBSOLETO|obsoleto|Obsoleto'), False, True)]
    dxf.to_csv('./data/dxf_list.csv', index=False)

finally:    
    try:
        df_dxfmaq = pd.read_csv('./data/dxfmaq.csv')
    except:
        # Construcción de características de archivos MAQ y PER
        df_maq = pd.concat([dxf[dxf['Desenho_Name'].str.startswith('PER')], dxf[dxf['Desenho_Name'].str.startswith('MAQ')]])
        df_maq['DXF_NAME'] = df_maq['Desenho_Name']
        df_maq['Desenho_Name'] = df_maq['Desenho_Name'].str[3:]                
        df_dxfmaq = pd.merge(df_maq, df_WR, how='left', on='DXF_NAME')   
        df_dxfmaq = df_dxfmaq.apply(caja_perforacion, axis=1)
        df_dxfmaq.to_csv('./data/dxfmaq.csv', index=False)
    finally:
        # Desarrollo de la tabla con archivos MAQ y PER
        df_dxfperf = df_dxfmaq[['Desenho_Name', 'Desenho_Ruta', 'Perforacion', 'Caja']]
        df_zferlist = df_zferlist[np.where(df_zferlist['Desenho_Name'].str.startswith('PRO'), False, True)]
        df_zferlist = df_zferlist[['Ordem_CodMaterial', 'Desenho_Name']].drop_duplicates()
        df_dxfperf = df_dxfperf[['Desenho_Name', 'Perforacion', 'Caja']].sort_values(by='Perforacion', ascending=False).drop_duplicates(subset=['Desenho_Name'], keep='first')
        
        # Cruce de las tablas de ZFER, perforaciones y materiales
        df_zferlist = pd.merge(df_zferlist, df_dxfperf, how='left', on='Desenho_Name')
        df_zferlist = pd.merge(df_zferlist, df_mats, how='left', on='Ordem_CodMaterial')
        df_zferlist = df_zferlist.fillna(0)
        df_zferlist['CodTipoPieza'] = df_zferlist['Desenho_Name'].str[-2:]
        df_zferlist['CodTipoPieza'] = df_zferlist['CodTipoPieza'].str.extract('(\d+)', expand=False)
        df_zferlist = pd.merge(df_zferlist, dxf[['Desenho_Name', 'Desenho_Ruta']], on='Desenho_Name', how='left')
        
        # Adding dimensions to each of the files
        df_WR = df_WR.rename({'DXF_NAME': 'Desenho_Name'}, axis=1)
        df_zferlist = pd.merge(df_zferlist, df_WR, how='left', on='Desenho_Name')
        
        # Tabla final
        df_dbzfer = df_zferlist[['Ordem_CodMaterial', 'CodTipoPieza', 'Acero', 'AL', 'Aluminum', 'Caja', 'Malla', 'Perforacion', 'Tejido', 'Desenho_Name', 'DXF_AREA', 'DXF_PERIM', 'Desenho_Ruta']].drop_duplicates(['Ordem_CodMaterial', 'CodTipoPieza', 'Acero', 'AL', 'Aluminum', 'Malla', 'Perforacion', 'Tejido', 'Desenho_Name'])
        df_dbzfer['POS'] = df_dbzfer['Desenho_Name'].str[-3]
        df_dbzfer['POS'] = df_dbzfer['POS'].map(diccionario_claves.claves_modelo)
        indices_posmax = list(df_dbzfer.sort_values(by=['Ordem_CodMaterial', 'POS'], ascending=False).drop_duplicates(['Ordem_CodMaterial', 'CodTipoPieza', 'Acero', 'AL', 'Aluminum', 'Malla', 'Perforacion', 'Tejido']).index)
        df_dbzfer.loc[indices_posmax, 'POS'] = '36VTPA'
        df_dbzfer = df_dbzfer.sort_values(by=['Ordem_CodMaterial', 'POS'], ascending=True)
        
        # Calcular valores aproximados de area para determinar archivos errados
        df_dbzfer = df_dbzfer.apply(area_perimetro, axis=1)
        
            
        # Creando la base de datos local SQL
        engine = create_engine('sqlite:///char_zfer.db')
        df_dbzfer.to_excel('ZFER_CHAR_CO.xlsx')
        df_dbzfer.to_sql('ZFER_CHARS_CO', con=engine, if_exists='replace', dtype={'CodTipoPieza': types.NVARCHAR(length=2), 'Acero': types.Integer(),
                                                                                  'AL': types.Integer(), 'Aluminum': types.Integer(), 'Caja': types.Integer(),
                                                                                  'Malla': types.Integer(), 'Perforacion': types.Integer(), 'POS': types.NVARCHAR(length=20),
                                                                                  'Tejido': types.Integer()})
        
        # Calcular las piezas con diferencia en su valor de área
        df_errarea = df_dbzfer[np.where(abs(df_dbzfer['DXF_AREA']-df_dbzfer['Area_calc']) > 0.06, True, False)]
        df_area0 = df_errarea[df_errarea['Area_calc'] == 0]
        df_difarea = df_errarea[df_errarea['Area_calc'] > 0]
        
        # Copiando archivos con error de lectura en Bridge Monitor
        df_errorlectura = pd.merge(df_dbzfer[df_dbzfer['DXF_AREA'].isna()], dxf, how='left', on='Desenho_Name')
        df_errorlectura = df_errorlectura[np.where(df_errorlectura['Desenho_Ruta'].isna(), False, True)]
        lista_errores_bridge = list(df_errorlectura['Desenho_Ruta'])
        lista_errores_area = list(df_difarea['Desenho_Ruta'])
        for i in lista_errores_bridge:
            shutil.copy(i, r'\\192.168.2.2\general\Smart Factory\Caracteristicas producto\Errores BridgeMonitor\Errores lectura')
        
        for i in lista_errores_area:
            shutil.copy(i, r'\\192.168.2.2\general\Smart Factory\Caracteristicas producto\Errores BridgeMonitor\Errores area')
