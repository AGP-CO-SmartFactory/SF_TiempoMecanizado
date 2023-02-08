
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

def perimetro(x):
    try:
        msp = ezdxf.readfile(x['Desenho_Ruta']).modelspace()
    except:
        return x
    else:
        entities = 0    
        perim = 0
        area = 0
        for e in msp:
            if len(msp) > 7:
                break
            entities+=1
            e_type = str(type(e))
            # Especificación para elementos con polilineas
            if e_type == "<class 'ezdxf.entities.polyline.Polyline'>":
                poly_vert = []
                for i in range(len(e)):
                    poly_vert.append(e[i].format('xy'))
                polyperi = polyPerimeter(np.array(poly_vert))
                polyarea = polyArea(np.array(poly_vert))
                if polyperi > perim:
                    perim = polyperi    
                if polyarea > area:
                    area = polyarea
            elif e_type == "<class 'ezdxf.entities.lwpolyline.LWPolyline'>":
                polyperi = polyPerimeter(np.array(list(e.vertices())))
                polyarea = polyArea(np.array(list(e.vertices())))
                if polyperi > perim:
                    perim = polyperi  
                if polyarea > area:
                    area = polyarea
        x['Peri_calc'] = perim
        x['Area_calc'] = area/1000000
        print(x)
        return x
    
def caja_perforacion(x):
    msp = ezdxf.readfile(x['Desenho_Ruta']).modelspace()
    entities = len(msp)
    if entities <= 1:
        x['Perforacion'] = 0
    elif entities > 1:
        x['Perforacion'] = 1
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
df_zferlist = Dataframe(conn_list[1], 2).dataframe

try: 
    dxf = pd.read_csv(r'\\192.168.2.2\general\Smart Factory\dxf_list.csv')
    
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
    dxf.to_csv(r'\\192.168.2.2\general\Smart Factory\dxf_list.csv', index=False)

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
        df_dxfmaq = df_dxfmaq.apply(perimetro, axis=1)
        df_dxfmaq['DXF_PERIM'] = np.where(df_dxfmaq['DXF_PERIM'].isnull(), df_dxfmaq['Peri_calc'], df_dxfmaq['DXF_PERIM'])
        df_dxfmaq['DXF_AREA'] = np.where(df_dxfmaq['DXF_AREA'].isnull(), df_dxfmaq['Area_calc'], df_dxfmaq['DXF_AREA'])
        df_dxfmaq = df_dxfmaq[['Desenho_Name', 'Desenho_Ruta', 'DXF_NAME', 'DXF_AREA', 'DXF_PERIM', 'Perforacion']]
        buffer = pd.merge(df_zferlist, df_WR.rename({'DXF_NAME': 'Desenho_Name'}, axis=1), on='Desenho_Name', how='left')
        df_dxfmaq = pd.merge(df_dxfmaq, buffer[['Desenho_Name', 'DXF_AREA', 'DXF_PERIM']].rename({'DXF_AREA': 'ABase', 'DXF_PERIM': 'PerBase'}, axis=1))
        df_dxfmaq = df_dxfmaq.drop_duplicates().fillna(0)
       
        def definicion_caja(x):
            if x['DXF_PERIM']-x['PerBase'] > 5:
                x['Caja'] = 1
            elif x['PerBase'] == 0 and x['Perforacion'] == 0:
                x['Caja'] = 1
            else:
                x['Caja'] = 0
            if x['Caja'] == 0:
                return x
            else:
                if x['Desenho_Name'] == '00' or x['Desenho_Name'] == '09':
                    r = 60
                    h = abs(0.5*(x['DXF_PERIM']-x['PerBase']-(2*3.1416*r)+(4*r)))
                    x['x_caja'] = abs(x['DXF_PERIM']+2*(r*(1-3.1416)-h))
                    x['y_caja'] = 2*r+h
                else:
                    return x
            print(x)
            return x
        df_dxfmaq = df_dxfmaq.apply(definicion_caja, axis=1)

        df_dxfmaq.to_csv('./data/dxfmaq.csv', index=False)
        
    # finally:
    #     # Desarrollo de la tabla con archivos MAQ y PER
    #     df_dxfperf = df_dxfmaq[['Desenho_Name', 'Desenho_Ruta', 'Perforacion', 'Caja']]
    #     df_zferlist = df_zferlist[np.where(df_zferlist['Desenho_Name'].str.startswith('PRO'), False, True)]
    #     df_zferlist = df_zferlist[['Ordem_CodMaterial', 'Desenho_Name']].drop_duplicates()
    #     df_dxfperf = df_dxfperf[['Desenho_Name', 'Perforacion', 'Caja']].sort_values(by='Perforacion', ascending=False).drop_duplicates(subset=['Desenho_Name'], keep='first')
        
    #     # Cruce de las tablas de ZFER, perforaciones y materiales
    #     df_zferlist = pd.merge(df_zferlist, df_dxfperf, how='left', on='Desenho_Name')
    #     df_zferlist = df_zferlist.fillna(0)
    #     df_zferlist['CodTipoPieza'] = df_zferlist['Desenho_Name'].str[-2:]
    #     df_zferlist['CodTipoPieza'] = df_zferlist['CodTipoPieza'].str.extract('(\d+)', expand=False)
    #     df_zferlist = pd.merge(df_zferlist, dxf[['Desenho_Name', 'Desenho_Ruta']], on='Desenho_Name', how='left')
        
    #     # Adding dimensions to each of the files
    #     df_WR = df_WR.rename({'DXF_NAME': 'Desenho_Name'}, axis=1)
    #     df_zferlist = pd.merge(df_zferlist, df_WR, how='left', on='Desenho_Name')
        
    #     # Tabla final
    #     df_dbzfer = df_zferlist[['Ordem_CodMaterial', 'CodTipoPieza', 'Acero', 'AL', 'Aluminum', 'Caja', 'Malla', 'Perforacion', 'Tejido', 'Desenho_Name', 'DXF_AREA', 'DXF_PERIM', 'Desenho_Ruta']].drop_duplicates(['Ordem_CodMaterial', 'CodTipoPieza', 'Acero', 'AL', 'Aluminum', 'Malla', 'Perforacion', 'Tejido', 'Desenho_Name'])
    #     df_dbzfer['POS'] = df_dbzfer['Desenho_Name'].str[-3]
    #     df_dbzfer['POS'] = df_dbzfer['POS'].map(diccionario_claves.claves_modelo)
    #     indices_posmax = list(df_dbzfer.sort_values(by=['Ordem_CodMaterial', 'POS'], ascending=False).drop_duplicates(['Ordem_CodMaterial', 'CodTipoPieza', 'Acero', 'AL', 'Aluminum', 'Malla', 'Perforacion', 'Tejido']).index)
    #     df_dbzfer.loc[indices_posmax, 'POS'] = '36VTPA'
    #     df_dbzfer = df_dbzfer.sort_values(by=['Ordem_CodMaterial', 'POS'], ascending=True)
        
    #     # # Calcular valores aproximados de area para determinar archivos errados
    #     # df_dbzfer = df_dbzfer.apply(area_perimetro, axis=1)
        
            
    #     # Creando la base de datos local SQL
    #     engine = create_engine('sqlite:///char_zfer.db')
    #     df_dbzfer.to_excel('ZFER_CHAR_CO.xlsx')
    #     df_dbzfer.to_sql('ZFER_CHARS_CO', con=engine, if_exists='replace', dtype={'CodTipoPieza': types.NVARCHAR(length=2), 'Acero': types.Integer(),
    #                                                                               'AL': types.Integer(), 'Aluminum': types.Integer(), 'Caja': types.Integer(),
    #                                                                               'Malla': types.Integer(), 'Perforacion': types.Integer(), 'POS': types.NVARCHAR(length=20),
    #                                                                               'Tejido': types.Integer()})

        
    #     # # Copiando archivos con error de lectura en Bridge Monitor
    #     # df_errorlectura = pd.merge(df_dbzfer[df_dbzfer['DXF_AREA'].isna()], dxf, how='left', on=['Desenho_Name', 'Desenho_Ruta'])
    #     # df_errorlectura = df_errorlectura[np.where(df_errorlectura['Desenho_Ruta'].isna(), False, True)]
    #     # lista_errores_bridge = list(df_errorlectura['Desenho_Ruta'])
    #     # lista_errores_area = list(df_difarea['Desenho_Ruta'])
    #     # for i in lista_errores_bridge:
    #     #     shutil.copy(i, r'\\192.168.2.2\general\Smart Factory\Caracteristicas producto\Errores BridgeMonitor\Errores lectura')
        
    #     # for i in lista_errores_area:
    #     #     shutil.copy(i, r'\\192.168.2.2\general\Smart Factory\Caracteristicas producto\Errores BridgeMonitor\Errores area')
