
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
from dotenv import load_dotenv
import pandas as pd, numpy as np
import glob, functions
from connections import Connection
from dataframes import Dataframe
import parameters
# Loading test environment

   
print('Descargando información...\n')
conn_calend = Connection(parameters.conexiones['SERCAL'], parameters.conexiones['DATCAL'], parameters.conexiones['UIDCAL'], parameters.conexiones['PWDCAL'])
conn_colsap = Connection(parameters.conexiones['SERING'], parameters.conexiones['DATING'], parameters.conexiones['UIDING'], parameters.conexiones['PWDING'])
conn_produc = Connection(parameters.conexiones['SERCP'], parameters.conexiones['DATCP'], parameters.conexiones['UIDCP'], parameters.conexiones['PWDCP'])

df_calendario = pd.read_sql(parameters.queries['query_calendario'], conn_calend.conn)
unique_zfer = list(df_calendario['ZFER'].unique())
cal_unique_zfer = str(unique_zfer)[1:-1] # This ZFER list filter all of the queries from now on

# Create a query for the ZFER_HEAD dataframe
parameters.create_query(query="SELECT MATERIAL as ZFER, PART_NUMBER, ZFOR, PLANO FROM ODATA_ZFER_HEAD",
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

test = pd.merge(df_calendario, df_zfer_head, on='ZFER', how='outer')
test2 = pd.merge(test, df_zfer_bom, on='ZFER', how='outer').drop_duplicates()
