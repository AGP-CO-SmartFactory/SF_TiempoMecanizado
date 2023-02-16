
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
        print('Asignando variable de perforación') 
        df_dxfmaq = df_dxfmaq.apply(caja_perforacion, axis=1)
        print('Calculando perímetros de piezas con archivo MAQ')
        df_dxfmaq = df_dxfmaq.apply(perimetro, axis=1)
        df_dxfmaq['DXF_PERIM'] = np.where(df_dxfmaq['DXF_PERIM'].isnull(), df_dxfmaq['Peri_calc'], df_dxfmaq['DXF_PERIM'])
        df_dxfmaq['DXF_AREA'] = np.where(df_dxfmaq['DXF_AREA'].isnull(), df_dxfmaq['Area_calc'], df_dxfmaq['DXF_AREA'])
        df_dxfmaq = df_dxfmaq[['Desenho_Name', 'Desenho_Ruta', 'DXF_NAME', 'DXF_AREA', 'DXF_PERIM', 'Perforacion']]
        buffer = pd.merge(df_zferlist, df_WR.rename({'DXF_NAME': 'Desenho_Name'}, axis=1), on='Desenho_Name', how='left')
        df_dxfmaq = pd.merge(df_dxfmaq, buffer[['Desenho_Name', 'DXF_AREA', 'DXF_PERIM']].rename({'DXF_AREA': 'ABase', 'DXF_PERIM': 'PerBase'}, axis=1))
        df_dxfmaq = df_dxfmaq.drop_duplicates().fillna(0)
       
        def definicion_caja(x):
            if x['Desenho_Name'][-2:] == '00':
                r = 60
                x['h_caja'] = x['DXF_PERIM']-x['PerBase']-(2*np.pi*r)+(8*r)
                x['w_caja'] = (x['ABase']-x['DXF_AREA'])*1000000/x['h_caja']
                if x['h_caja'] > 30:
                    x['Caja'] = 1
                else:
                    x['Caja'] = 0
                    x['h_caja'] = None
                    x['w_caja']
            else:
                x['Caja'] = 0
            return x
        print('Calculando dimensiones de cajas')
        df_dxfmaq = df_dxfmaq.apply(definicion_caja, axis=1)

        df_dxfmaq.to_csv('./data/dxfmaq.csv', index=False)
        
    finally:
        def curvado_recortado(x):
            print(x)
            try:
                df = df_dbzfer[df_dbzfer['ZFER'] == x['ZFER']]
                df = df[0:2]
                df1 = df[0:1]
                df2 = df[1:2]
                if df1['Desenho_Name'].values[0] == df2['Desenho_Name'].values[0]:
                    x['CurvadoRecortado'] = 0
                else:
                    x['CurvadoRecortado'] = 1                
            except:
                pass
            finally:
                return x
            
        # Desarrollo de la tabla con archivos MAQ y PER
        df_dxfperf = df_dxfmaq[['DXF_NAME', 'Desenho_Name', 'Perforacion', 'Caja', 'w_caja', 'h_caja']]
        df_zferlist = df_zferlist[np.where(df_zferlist['Desenho_Name'].str.startswith('PRO'), False, True)]
        df_zferlist = df_zferlist.drop_duplicates()
        df_dbzfer = pd.merge(df_zferlist, df_dxfperf, on='Desenho_Name', how='left').drop_duplicates()
        df_WR = df_WR.rename({'DXF_NAME': 'Desenho_Name'}, axis=1)
        df_dbzfer = pd.merge(df_dbzfer, df_WR, how='left', on='Desenho_Name')
        df_dbzfer['CodTipoPieza'] = df_dbzfer['Desenho_Name'].str[-2:]
        
        # Agregando info de curvado recortado
        df_cr = df_dbzfer[df_dbzfer['POS'] == '0100'][['ZFER', 'Desenho_Name', 'POS']]
        print('Asignando variable de curvado recortado')
        df_cr = df_cr.apply(curvado_recortado, axis=1)
        df_dbzfer = pd.merge(df_dbzfer, df_cr[['ZFER', 'CurvadoRecortado']], on='ZFER', how='left')
        # Creando la base de datos local SQL
        df_dbzfer = df_dbzfer.rename({'CLV_MODEL': 'ClaveModelo', 'Desenho_Name': 'DesenhoName', 'Desenho_ZTipo': 'ZTipo', 'POS': 'Pos', 
                          'DXF_NAME': 'ArchivoMecanizado', 'DXF_AREA': 'AreaReal', 'DXF_PERIM': 'PerimetroReal', 'CodTipoPieza': 'CodPieza'}, axis=1)
        # engine = create_engine('sqlite:///char_zfer.db')
        df_dbzfer.to_csv(r'\\192.168.2.2\general\Smart Factory\ZFER_CHAR_CO.csv')
        # df_dbzfer.to_sql('ZFER_CHARS_CO', con=engine, if_exists='replace', dtype={'ZFER': types.Integer(), 'ClaveModelo': types.NVARCHAR(length=25), 'DesenhoName': types.NVARCHAR(length=25),
        #                                                                           'CodPieza': types.NVARCHAR(length=2), 'Acero': types.Integer(), 'AL': types.Integer(), 'Aluminum': types.Integer(), 
        #                                                                           'Caja': types.Integer(), 'Malla': types.Integer(), 'Perforacion': types.Integer(), 'Pos': types.NVARCHAR(length=20),
        #                                                                           'Tejido': types.Integer()})