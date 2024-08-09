from connections import Connection
import parameters
import pandas as pd
class Databases:
    def __init__(self):
        self.engines = {'conn_calenda': Connection(parameters.conexiones['SERCAL'], parameters.conexiones['DATCAL'], 
                                                parameters.conexiones['UIDCAL'], parameters.conexiones['PWDCAL']).conn,
                      'conn_colsap': Connection(parameters.conexiones['SERING'], parameters.conexiones['DATING'], 
                                                parameters.conexiones['UIDING'], parameters.conexiones['PWDING']).conn,
                      'conn_producc': Connection(parameters.conexiones['SERCP'], parameters.conexiones['DATCP'], 
                                                parameters.conexiones['UIDCP'], parameters.conexiones['PWDCP']).conn,
                      'conn_smartfa': Connection(parameters.conexiones['SERSF'], parameters.conexiones['DATSF'], 
                                                parameters.conexiones['UIDSF'], parameters.conexiones['PWDSF']).conn,
                      'conn_comerci': Connection(parameters.conexiones['SERCO'], parameters.conexiones['DATCO'], 
                                                parameters.conexiones['UIDCO'], parameters.conexiones['PWDCO']).conn,
                      'conn_genesis': Connection(parameters.conexiones['SERGN'], parameters.conexiones['DATGN'], 
                                                parameters.conexiones['UIDGN'], parameters.conexiones['PWDGN']).conn,}
    def crear_dataframe(self, query, key):
        with self.engines[key].connect() as connection:
            df = pd.read_sql(query, connection)
        return df