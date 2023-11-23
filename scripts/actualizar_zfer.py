"""
This code collects and calculates the primary characteristics for a ZFER produced 
in AGP SGlass CO. It makes a recollection of all the critical variables in the 
different databases that the company has and combines them into a single dataframe 
that is to be exported to a SQL Server table. 

The use of this code is exclusive for AGP Glass and cannot be sold or 
distributed to other companies. Unauthorized distribution of this code is a 
violation of AGP intellectual property.

Author: Juan Pablo Rodriguez Garcia (jpgarcia@agpglass.com)
"""
import pandas as pd
import parameters
from databases import Databases
from functions import Functions

def main():
    print('Inicializando programa...\n')
    db = Databases()
    functions = Functions(db.conns['conn_smartfa'])
    df_base = pd.read_sql(parameters.queries['query_cal_acabados'], db.conns['conn_calenda'])
    df_base = df_base.drop_duplicates(subset=['ZFER'], keep='first').fillna('')
    unique_zfer = list(df_base['ZFER'].unique())
    cal_unique_zfer = str(unique_zfer)[1:-1] # This ZFER list filter all of the queries from now on    
    # Create a query for the ZFER_HEAD dataframe - START
    print('Descargando información desde ingeniería (ZFER-HEAD)...\n')   
    parameters.create_query(query="SELECT MATERIAL as ZFER, ZFOR FROM ODATA_ZFER_HEAD",
                            where=f"WHERE MATERIAL in ({cal_unique_zfer})", dict_name='zfer_head')    
    df_zfer_head = pd.read_sql(parameters.queries['zfer_head'], db.conns['conn_colsap']).drop_duplicates('ZFER', keep='first')
    # Create a query for the ZFER_HEAD dataframe - END
    print('Descargando información desde ingeniería (ZFER-BOM)...\n')
    # Create a query for the ZFER_bom dataframe - START
    parameters.create_query(query="""SELECT MATERIAL as ZFER, POSICION, CLASE, CAST(DIMEN_BRUTA_1 as float) as ANCHO, 
                            CAST(DIMEN_BRUTA_2 as float) as LARGO, CAST(CANT_PIEZAS_BRUTA as float) as AREA FROM ODATA_ZFER_BOM""",
                            where=f"""WHERE CLASE like 'Z_VD%' AND CENTRO = 'CO01' AND MATERIAL in ({cal_unique_zfer})""", dict_name='zfer_bom')    
    df_zfer_bom = pd.read_sql(parameters.queries['zfer_bom'], db.conns['conn_colsap'])
    df_zfer_bom['CLASE'] = df_zfer_bom.apply(lambda x: x['CLASE'][0:-1] if x['CLASE'][-1] == "_" else x['CLASE'], axis=1) #Eliminar lineas al final del texto
    # Create a query for the ZFER_bom dataframe - END
    print('Unificando tablas...\n')
    # Merging the base dataframe into one
    df = pd.merge(df_base.astype({'ZFER': int}), df_zfer_head, on='ZFER', how='outer')
    df = pd.merge(df, df_zfer_bom, on='ZFER', how='outer').drop_duplicates()
    df['ClaveModelo'] = df['POSICION'].map(parameters.dict_clavesmodelo)
    df = df.fillna({'BordePintura': '', 'BordePaquete': '', 'ClaveModelo':'', 'ZFOR': 0, 'Caja': 0, 'Tiempo': 0})    
    df = df.dropna(subset=['ANCHO', 'LARGO'])
    df = functions.definir_cantos(df)    
    df = df.apply(functions.agregar_pasadas, axis=1)
    df = df.apply(functions.tiempo_acabado, axis=1)
    df = df.reset_index()
    df2 = df.drop(['Cambios', 'index', 'POSICION', 'CantoP'], axis=1)
    df2 = df2.rename({'CLASE': 'Material', 'ANCHO': 'Ancho', 'LARGO': 'Largo', 'AREA': 'Area', 'Tiempo': 'TiempoMecanizado'}, axis=1)
    df2 = df2.drop_duplicates(subset=['ZFER', 'ClaveModelo'], keep='first')
    df2 = df2.fillna({'BordePintura': '', 'BordePaquete': '', 'ClaveModelo':'', 'Operacion1':'', 'Operacion2':'', 'ZFOR': 0, 'Caja': 0, 'Tiempo': 0})
    df2 = df2.astype({'ZFER': int, 'ZFOR': int, 'Material': str, 'Ancho': str,
                      'Largo': str, 'ClaveModelo': str, 'Perimetro': str, 'TiempoMecanizado': float,
                      'BrilloC': bool, 'BrilloP': bool, 'Bisel': bool, 'CantoC': bool})
    df2 = df2.rename({'ENG_BehaviorDiffs': 'BehaviorDiffs', 'ENG_GeometricDiffs': 'GeometricDiffs'}, axis=1)
    df2.index = df2.index.rename('ID')
    return df2