
"""
This code collects and calculates the primary characteristics for a ZFER produced in AGP SGlass CO.
It makes a recollection of all the critical variables in the different databases that the company has
and combines them into a single dataframe that is to be exported to a SQL Server table. 

This code uses sqlalchemy, ezdxf, pandas, numpy and dxfasc, a library written by nikhartam to calculate
the perimeter of a .dxf file. 

The use of this code is exclusive for AGP Glass and cannot be sold or distributed to other companies.
Unauthorized distribution of this code is a violation of AGP intellectual property.

Author: Juan Pablo Rodriguez Garcia (jpgarcia@agpglass.com)
"""
import pandas as pd
from connections import Connection
import parameters
import sql
# Loading test environment

def main():
    print('Descargando información...\n')
    conn_calend = Connection(parameters.conexiones['SERCAL'], parameters.conexiones['DATCAL'], parameters.conexiones['UIDCAL'], parameters.conexiones['PWDCAL'])
    conn_colsap = Connection(parameters.conexiones['SERING'], parameters.conexiones['DATING'], parameters.conexiones['UIDING'], parameters.conexiones['PWDING'])
    conn_produc = Connection(parameters.conexiones['SERCP'], parameters.conexiones['DATCP'], parameters.conexiones['UIDCP'], parameters.conexiones['PWDCP'])
    conn_smartf = Connection(parameters.conexiones['SERSF'], parameters.conexiones['DATSF'], parameters.conexiones['UIDSF'], parameters.conexiones['PWDSF'])
    
    df_calendario = pd.read_sql(parameters.queries['query_calendario'], conn_calend.conn)
    unique_zfer = list(df_calendario['ZFER'].unique())
    cal_unique_zfer = str(unique_zfer)[1:-1] # This ZFER list filter all of the queries from now on
    
    # Create a query for the ZFER_HEAD dataframe
    parameters.create_query(query="SELECT MATERIAL as ZFER, ZFOR FROM ODATA_ZFER_HEAD",
                            where=f"WHERE MATERIAL in ({cal_unique_zfer})", dict_name='zfer_head')
    
    df_zfer_head = pd.read_sql(parameters.queries['zfer_head'], conn_colsap.conn).drop_duplicates('ZFER', keep='first')
    unique_zfor = list(df_zfer_head['ZFOR'].unique())
    sql_unique_zfor = str(unique_zfor)[1:-1]
    
    # Create a query for the ZFER_bom dataframe
    parameters.create_query(query="SELECT MATERIAL as ZFER, POSICION, CLASE, CAST(DIMEN_BRUTA_1 as float) as ANCHO, CAST(DIMEN_BRUTA_2 as float) as LARGO FROM ODATA_ZFER_BOM",
                            where=f"WHERE MATERIAL in ({cal_unique_zfer}) AND CLASE like 'Z_VD%' AND CENTRO = 'CO01' ORDER BY ZFER, POSICION ASC", dict_name='zfer_bom')
    
    df_zfer_bom = pd.read_sql(parameters.queries['zfer_bom'], conn_colsap.conn)
    
    # Create a query for the HR table
    parameters.create_query(query=f"""WITH a as (SELECT MATNR as ZFOR, PLNNR as HojaRuta FROM ODATA_HR_MAPL O with (nolock)
    				inner join ODATA_HR_CONSULTA C on C.ID_HRUTA = O.PLNNR WHERE WERKS = 'CO01' AND MATNR in ({sql_unique_zfor}) GROUP BY MATNR, PLNNR)
                    SELECT PLNNR as HojaRuta, KTSCH as ClaveModelo, LTXA1 as Operacion, ZFOR FROM ODATA_HR_PLPO o with (nolock) 
                    	inner join a on a.HojaRuta = o.PLNNR WHERE LTXA1 = 'MECANIZADO'
                        """, dict_name='hojasruta')
    
    df_hojasruta = pd.read_sql(parameters.queries['hojasruta'], conn_produc.conn)
    
    # Create a query for the HR table to return the windows with black band
    parameters.create_query(query=f"""WITH a as (SELECT MATNR as ZFOR, PLNNR as HojaRuta FROM ODATA_HR_MAPL O with (nolock)
    				inner join ODATA_HR_CONSULTA C on C.ID_HRUTA = O.PLNNR WHERE WERKS = 'CO01' AND MATNR in ({sql_unique_zfor}) GROUP BY MATNR, PLNNR)
                    SELECT PLNNR as HojaRuta, KTSCH as ClaveModelo, LTXA1 as Operacion, ZFOR FROM ODATA_HR_PLPO o with (nolock) 
                    	inner join a on a.HojaRuta = o.PLNNR WHERE LTXA1 = 'SERIGRAFIA'
                        """, dict_name='hojasruta_serigrafia')
    
    df_pinturas = pd.read_sql(parameters.queries['hojasruta_serigrafia'], conn_produc.conn)
    
    df = pd.merge(df_calendario, df_zfer_head, on='ZFER', how='outer')
    df = pd.merge(df, df_zfer_bom, on='ZFER', how='outer').drop_duplicates()
    df['ClaveModelo'] = df['POSICION'].map(parameters.dict_clavesmodelo)
    
    # Extraer los valores null en el ZFOR
    df_nullZFOR = df[pd.isnull(df['ZFOR'])] 
    
    # Tomar únicamente los vidrios con mecanizado de df
    df = df.dropna(subset='ZFOR')
    df = pd.merge(df.astype({'ZFOR': int}), df_hojasruta[['ZFOR', 'ClaveModelo']].astype({'ZFOR': int}), on=['ZFOR', 'ClaveModelo'], how='inner').drop_duplicates()
    df = pd.merge(df.astype({'ZFOR': int}), df_pinturas[['ZFOR', 'ClaveModelo', 'Operacion']].astype({'ZFOR': int}), on=['ZFOR', 'ClaveModelo'], how='left').drop_duplicates()
    df = pd.concat([df, df_nullZFOR]) # Introducir de nuevo los lites sin ZFOR para referencia
    df = df.fillna({'BordePintura': '', 'BordePaquete': '', 'ClaveModelo':'', 'Operacion':'', 'ZFOR': 0})
    
    # Desde este punto se tienen que calcular las diferentes características
    df['Perimetro'] = (df['ANCHO']*2 + df['LARGO']*2)*(1-0.089)
    df['BrilloC'] = df.apply(lambda x: True if (x['ClaveModelo'] == '01VEXT' or x['Operacion'] == 'SERIGRAFIA') and 'Brillante' in x['BordePintura'] and 'Bisel' not in x['BordePintura'] else False, axis=1)
    df['BrilloP'] = df.apply(lambda x: True if 'Brillante' in x['BordePaquete'] and (x['ClaveModelo'] == '01VEXT' or x['Operacion'] == 'SERIGRAFIA') else False, axis=1)
    df['Bisel'] = df.apply(lambda x: True if (x['ClaveModelo'] == '01VEXT' and 'Bisel' in x['BordePintura']) or (x['ZFER'] == 700027561 and x['ClaveModelo'] == '01VEXT') else False, axis=1)
    df['CantoC'] = df.apply(lambda x: True if (x['ClaveModelo'] == '01VEXT' or x['Operacion'] == 'SERIGRAFIA') and x['BrilloC'] != True and x['BrilloP'] != True and x['Bisel'] != True else False, axis=1)
    df['CantoP'] = df.apply(lambda x: True if (x['ClaveModelo'] != '01VEXT' and  x['Operacion'] != 'SERIGRAFIA') or () else False, axis=1)
    
    # A partir de esta tabla se toman los avances de la CNC y se calculan los tiempos por medio de la función
    def calculate_time(x):
        avance = df_avances[df_avances['Referencia'] == x['CLASE']]
        if x['BrilloC'] == True:
            tiempo = round((x['Perimetro']/avance['AvanceBrilloC']).values[0], 2)
        elif x['BrilloP'] == True:
            tiempo = round((x['Perimetro']/avance['AvanceBrilloPlano']).values[0], 2)
        elif x['Bisel'] == True:
            tiempo = round(x['Perimetro']/avance['AvanceBisel'].values[0], 2)
        elif x['CantoC'] == True:
            tiempo = round(x['Perimetro']/avance['AvanceCantoC'].values[0], 2)
        elif x['CantoP'] == True:
            tiempo = round(x['Perimetro']/avance['AvanceCantoPlano'].values[0], 2)        
        x['Tiempo'] = tiempo
        return x
    
    df_avances = pd.read_sql(parameters.queries['query_avances'], conn_smartf.conn)
    df = df.apply(calculate_time, axis=1)
    df2 = df[['Orden', 'ZFER', 'CodTipoPieza', 'POSICION', 'CLASE', 'ANCHO', 'LARGO', 'ClaveModelo', 'Perimetro', 'Tiempo']]
    df2 = df2.rename({'POSICION': 'Posicion', 'CLASE': 'Material', 'ANCHO': 'Ancho', 'LARGO': 'Largo'}, axis=1)
    
    sql.data_update(df2) # Carga de datos al dataframe
    return df_nullZFOR

if __name__ == '__main__':
    null_ZFOR = main()