
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

def main():
    print('Inicializando programa...\n')
    db = Databases()
    df_base = pd.read_sql(parameters.queries['query_calendario'], db.conns['conn_calenda'])
    unique_zfer = list(df_base['ZFER'].unique())
    cal_unique_zfer = str(unique_zfer)[1:-1] # This ZFER list filter all of the queries from now on    
    # Create a query for the ZFER_HEAD dataframe - START
    parameters.create_query(query="SELECT MATERIAL as ZFER, ZFOR FROM ODATA_ZFER_HEAD",
                            where=f"WHERE MATERIAL in ({cal_unique_zfer})", dict_name='zfer_head')    
    df_zfer_head = pd.read_sql(parameters.queries['zfer_head'], db.conns['conn_colsap']).drop_duplicates('ZFER', keep='first')
    unique_zfor = list(df_zfer_head['ZFOR'].dropna().unique())
    sql_unique_zfor = str(unique_zfor)[1:-1]
    # Create a query for the ZFER_HEAD dataframe - END
    print('Leyendo datos desde hojas de ruta de mecanizado...\n')
    # Create a query for the HR table - START
    parameters.create_query(query=f"""WITH a as (SELECT * FROM HR_MATERIALS WHERE MATERIAL like '5%' AND MATERIAL in ({sql_unique_zfor}))
                            SELECT hr.ID_HRUTA, TXT_MECANIZADO, a.MATERIAL as ZFOR FROM ODATA_HR_CONSULTA hr
                                inner join a on hr.ID_HRUTA = a.ID_HRUTA
                            WHERE TXT_MECANIZADO is not null and TXT_MECANIZADO <> ''
                            GROUP BY hr.ID_HRUTA, TXT_MECANIZADO, a.MATERIAL""", dict_name='hojasruta')    
    df_hojasruta = pd.read_sql(parameters.queries['hojasruta'], db.conns['conn_colsap'])
    df_hojasruta['Operacion1'] = 'MECANIZADO'
    df_hojasruta['ClaveModelo'] = df_hojasruta['TXT_MECANIZADO'].str.split(',')
    df_hojasruta = df_hojasruta.explode('ClaveModelo')
    # Create a query for the HR table - END
    print('Leyendo datos desde hojas de ruta de serigrafía...\n')
    # Create a query for the HR table to return the windows with black band - START
    parameters.create_query(query=f"""WITH a as (SELECT * FROM HR_MATERIALS WHERE MATERIAL like '5%' AND MATERIAL in ({sql_unique_zfor}))
                            SELECT hr.ID_HRUTA, TXT_VITRIFICADO, a.MATERIAL as ZFOR FROM ODATA_HR_CONSULTA hr
                                inner join a on hr.ID_HRUTA = a.ID_HRUTA
                            WHERE TXT_MECANIZADO is not null and TXT_MECANIZADO <> ''
                            GROUP BY hr.ID_HRUTA, TXT_VITRIFICADO, a.MATERIAL""", dict_name='hojasruta_serigrafia')
    df_pinturas = pd.read_sql(parameters.queries['hojasruta_serigrafia'], db.conns['conn_colsap'])
    df_pinturas['Operacion2'] = 'VITRIFICADO'
    df_pinturas['ClaveModelo'] = df_pinturas['TXT_VITRIFICADO'].str.split(',')
    df_pinturas = df_pinturas.explode('ClaveModelo')
    # Create a query for the HR table to return the windows with black band - END
    parameters.create_query(query='SELECT * FROM SF_TiemposMecanizado_ZFER', dict_name='tiempos_cnc', 
                            where=f'WHERE ZFER in ({cal_unique_zfer})')
    df_tiemposcnc = pd.read_sql(parameters.queries['tiempos_cnc'], db.conns['conn_smartfa'])
    df_tiemposcnc = df_tiemposcnc.drop(['ZFOR', 'ID'], axis=1)
    print('Unificando tablas...\n')
    # Merging the base dataframe into one
    df = pd.merge(df_base, df_zfer_head, on='ZFER', how='outer')
    df = pd.merge(df, df_tiemposcnc, on='ZFER', how='left')

    # Extraer los valores null en el ZFOR
    df_nullZFOR = df[pd.isnull(df['ZFOR'])] 
    df = df.dropna(subset=['ZFOR'])
    df = pd.merge(df.astype({'ZFOR': int}), df_pinturas[['ZFOR', 'ClaveModelo', 'Operacion2']].astype({'ZFOR': int}), 
                  on=['ZFOR', 'ClaveModelo'], how='left').drop_duplicates()  
    print('Combinando tablas...\n')
    # Combinar los vidrios con mecanizado de df
    df = pd.merge(df.astype({'ZFOR': int}), df_hojasruta[['ZFOR', 'ClaveModelo', 'Operacion1']].astype({'ZFOR': int}), 
                  on=['ZFOR', 'ClaveModelo'], how='left').drop_duplicates()
    df = pd.concat([df, df_nullZFOR]) # Introducir de nuevo los lites sin ZFOR para referencia
    df = df.fillna({'BordePintura': '', 'BordePaquete': '', 'ClaveModelo':'', 'Operacion1':'', 'Operacion2':'', 'ZFOR': 0, 'Tiempo': 0})    
    
    df = df.reset_index()
    df2 = df.drop(['BordePintura', 'BordePaquete', 'index', 'GeometricDiffs', 'BehaviorDiffs'], axis=1)
    df2 = df2.rename({'POSICION': 'Posicion', 'CLASE': 'Material', 'ANCHO': 'Ancho', 'LARGO': 'Largo'}, axis=1)
    df2 = df2.drop_duplicates(subset=['Orden', 'ZFER', 'ClaveModelo'], keep='first')
    df2 = df2.fillna({'BordePintura': '', 'BordePaquete': '', 'ClaveModelo':'', 'Operacion1':'', 'Operacion2':'', 'ZFOR': 0, 'TiempoMecanizado': 0})
    df2 = df2.astype({'Orden': int, 'ZFER': int, 'ZFOR': int, 'CodTipoPieza': int, 'Material': str, 'Ancho': float,
                      'Largo': float, 'ClaveModelo': str, 'Operacion1': str, 'Operacion2': str, 'Perimetro': float, 'TiempoMecanizado': float,
                      'BrilloC': bool, 'BrilloP': bool, 'Bisel': bool, 'CantoC': bool})
    df2.index = df2.index.rename('ID')
    return df2