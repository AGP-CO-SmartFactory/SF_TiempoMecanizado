
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
import sql
from databases import Databases
from functions import Functions

def main():
    print('Descargando información...\n')
    db = Databases()
    functions = Functions(db.conn_smartf)
    df_base = pd.read_sql(parameters.queries['query_calendario'], db.conn_calend)
    unique_zfer = list(df_base['ZFER'].unique())
    cal_unique_zfer = str(unique_zfer)[1:-1] # This ZFER list filter all of the queries from now on    
    # Create a query for the ZFER_HEAD dataframe - START
    parameters.create_query(query="SELECT MATERIAL as ZFER, ZFOR FROM ODATA_ZFER_HEAD",
                            where=f"WHERE MATERIAL in ({cal_unique_zfer})", dict_name='zfer_head')    
    df_zfer_head = pd.read_sql(parameters.queries['zfer_head'], db.conn_colsap).drop_duplicates('ZFER', keep='first')
    unique_zfor = list(df_zfer_head['ZFOR'].dropna().unique())
    sql_unique_zfor = str(unique_zfor)[1:-1]
    # Create a query for the ZFER_HEAD dataframe - END

    # Create a query for the ZFER_bom dataframe - START
    parameters.create_query(query="""SELECT MATERIAL as ZFER, POSICION, CLASE, CAST(DIMEN_BRUTA_1 as float) as ANCHO, 
                            CAST(DIMEN_BRUTA_2 as float) as LARGO FROM ODATA_ZFER_BOM","""
                            where=f"""WHERE MATERIAL in ({cal_unique_zfer}) AND CLASE like 'Z_VD%' AND CENTRO = 'CO01' 
                            ORDER BY ZFER, POSICION ASC""", dict_name='zfer_bom')    
    df_zfer_bom = pd.read_sql(parameters.queries['zfer_bom'], db.conn_colsap)
    df_zfer_bom['CLASE'] = df_zfer_bom.apply(lambda x: x['CLASE'][0:-1] if x['CLASE'][-1] == "_" else x['CLASE'], axis=1)
    # Create a query for the ZFER_bom dataframe - END

    # Create a query for the HR table - START
    parameters.create_query(query=f"""WITH HR as (SELECT ID_HRUTA, TXT_MECANIZADO FROM ODATA_HR_CONSULTA 
                            with (nolock) WHERE TXT_MECANIZADO is not null and TXT_MECANIZADO <> ''),
                            zfor as (SELECT MATNR as ZFOR, PLNNR as HojaRuta FROM ODATA_HR_MAPL O 
                            with (nolock) WHERE WERKS='CO01' and MATNR in ({sql_unique_zfor}) GROUP BY MATNR, PLNNR)
                        SELECT ZFOR, TXT_MECANIZADO as ClaveModelo, MAX(HojaRuta) as HojaRuta FROM HR with (nolock)
                        inner join zfor on HR.ID_HRUTA = zfor.HojaRuta GROUP BY ZFOR, TXT_MECANIZADO""", dict_name='hojasruta')    
    df_hojasruta = pd.read_sql(parameters.queries['hojasruta'], db.conn_produc)
    df_hojasruta['Operacion1'] = 'MECANIZADO'
    df_hojasruta['ClaveModelo'] = df_hojasruta['ClaveModelo'].str.split(',')
    df_hojasruta = df_hojasruta.explode('ClaveModelo')
    # Create a query for the HR table - END

    # Create a query for the HR table to return the windows with black band - START
    parameters.create_query(query=f"""WITH HR as (SELECT ID_HRUTA, TXT_SERIGRAFIA FROM ODATA_HR_CONSULTA 
                            with (nolock) WHERE TXT_SERIGRAFIA is not null and TXT_SERIGRAFIA <> ''),
                            zfor as (SELECT MATNR as ZFOR, PLNNR as HojaRuta FROM ODATA_HR_MAPL O 
                            with (nolock) WHERE WERKS='CO01' and MATNR in ({sql_unique_zfor}) GROUP BY MATNR, PLNNR)
                        SELECT ZFOR, TXT_SERIGRAFIA as ClaveModelo, MAX(HojaRuta) as HojaRuta FROM HR with (nolock)
                        inner join zfor on HR.ID_HRUTA = zfor.HojaRuta GROUP BY ZFOR, TXT_SERIGRAFIA
                        """, dict_name='hojasruta_serigrafia')
    df_pinturas = pd.read_sql(parameters.queries['hojasruta_serigrafia'], db.conn_produc)
    df_pinturas['Operacion2'] = 'SERIGRAFIA'
    df_pinturas['ClaveModelo'] = df_pinturas['ClaveModelo'].str.split(',')
    df_pinturas = df_pinturas.explode('ClaveModelo')
    # Create a query for the HR table to return the windows with black band - END

    # Merging the base dataframe into one
    df = pd.merge(df_base, df_zfer_head, on='ZFER', how='outer')
    df = pd.merge(df, df_zfer_bom, on='ZFER', how='outer').drop_duplicates()
    df['ClaveModelo'] = df['POSICION'].map(parameters.dict_clavesmodelo)

    # Agregar la característica de perforacion a los ZFER en la lista
    df_perf = pd.read_sql(parameters.queries['query_perforacion'], db.conn_colsap)
    df_perf['Perforacion'] = 1
    df_perf2 = pd.merge(df, df_perf, on='ZFER', how='right').dropna().drop_duplicates()

    # Extraer los valores null en el ZFOR
    df_nullZFOR = df[pd.isnull(df['ZFOR'])] 
    df = df.dropna(subset=['ZFOR'])
    df = pd.merge(df.astype({'ZFOR': int}), df_pinturas[['ZFOR', 'ClaveModelo', 'Operacion']].astype({'ZFOR': int}), 
                  on=['ZFOR', 'ClaveModelo'], how='left').drop_duplicates()  
      
    # Combinar los vidrios con mecanizado de df
    df = pd.merge(df.astype({'ZFOR': int}), df_hojasruta[['ZFOR', 'ClaveModelo', 'Operacion']].astype({'ZFOR': int}), 
                  on=['ZFOR', 'ClaveModelo'], how='left').drop_duplicates()
    df = pd.concat([df, df_nullZFOR]) # Introducir de nuevo los lites sin ZFOR para referencia
    df = df.fillna({'BordePintura': '', 'BordePaquete': '', 'ClaveModelo':'', 'Operacion_x':'', 'Operacion_y':'', 'ZFOR': 0, 'Caja': 0})    
    df = functions.definir_cantos(df)    
    df = df.apply(functions.agregar_pasadas, axis=1)
    df = df.apply(functions.tiempo_acabado, axis=1)
    df2 = df.drop(['BordePintura', 'BordePaquete', 'Cambios'], axis=1)
    df2 = df2.rename({'POSICION': 'Posicion', 'CLASE': 'Material', 'ANCHO': 'Ancho', 'LARGO': 'Largo', 
                      'Operacion_y': 'Operacion2', 'Operacion_x': 'Operacion1'}, axis=1)
    df2 = df2.drop_duplicates(subset=['Orden', 'ZFER', 'ClaveModelo'], keep='first')
    #sql.data_update(df2) # Carga de datos al dataframe
    return df2