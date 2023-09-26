﻿import pandas as pd
import sqlalchemy 
from sqlalchemy.engine import URL
import os
from dotenv import load_dotenv

# Variables de entorno del código
load_dotenv('C:\envs\zferbp_envar.env')
class Loader:
    def __init__(self, tabla):
        SERVER = os.getenv('SERSF')
        UID = os.getenv('UIDSF')
        PWD = os.getenv('PWDSF')
        DB = os.getenv('DATSF')
        DRIVER = 'ODBC Driver 18 for SQL Server'
        connection_url = URL.create("mssql+pyodbc", username=UID, password=PWD,
            host=SERVER, database=DB, query={"driver": DRIVER, "TrustServerCertificate": "yes",})
        self.engine = sqlalchemy.create_engine(connection_url)
        self.connection = self.engine.connect()
        self.meta = sqlalchemy.MetaData()
        self.tabla_maestra = sqlalchemy.Table(tabla, self.meta, autoload_with=self.engine)

    def erase_table(self):
        print('Limpiando tabla...')
        # Limpiando todos los registros que son anteriores a la fecha inicial de la busqueda en Historian
        table_deletion = (sqlalchemy.delete(self.tabla_maestra))
        self.connection.execute(table_deletion)
    
    def update_row(self, row):
        table_update = (sqlalchemy.update(self.tabla_maestra)
                        .where(self.tabla_maestra.c.ID == int(row['ID']))
                        .values(BordePaquete = row['BordePaquete'], BordePintura = row['BordePintura'],
                                TiempoMecanizado = row['TiempoMecanizado'], Bisel = row['Bisel'],
                                BrilloC = row['BrilloC'], BrilloP = row['BrilloP'], CantoC = row['CantoC']))
        self.connection.execute(table_update)
               
    def data_update(self, df_final: pd.DataFrame):
        """
        Parameters
        ----------
        fi : str
            Fecha inicial del periodo a buscar en historian.
        fia: str
            Fecha inicial para el borrado de alertas
        df_final : pd.DataFrame
            Dataframe con la información más reciente para actualizar SQL
        """
        # Creating the connection to the SQL server   
        print('Cargando datos...')    
        df_final.to_sql('SF_TiemposMecanizado', self.connection, if_exists='append', index_label='ID')
        self.connection.close()
        self.engine.dispose()

    def zfer_create(self, df_final: pd.DataFrame, dispose=True):
        """
        Parameters
        ----------
        fi : str
            Fecha inicial del periodo a buscar en historian.
        fia: str
            Fecha inicial para el borrado de alertas
        df_final : pd.DataFrame
            Dataframe con la información más reciente para actualizar SQL
        """
        # Creating the connection to the SQL server
        print('Cargando datos...') 
        df_final.to_sql('SF_TiemposMecanizado_ZFER', self.connection, if_exists='append', index_label='ID')
        if dispose:
            self.connection.close()
            self.engine.dispose()
    
    def update_tablebyrow(self, df_final):
        print('Actualizando tabla con los nuevos registros...')
        unique_zfer = str(list(df_final['ZFER'].unique()))[1:-1]
        df_sql = pd.read_sql(f"""SELECT ID, ZFER, ClaveModelo FROM SF_TiemposMecanizado_ZFER 
                             WHERE ZFER in ({unique_zfer})""", self.connection)
        df_update = pd.merge(df_final, df_sql, on=['ZFER', 'ClaveModelo'], how='left')
        df_newzfers = df_update[(pd.isna(df_update['ID'])) & (df_update['Material'] != 'nan')]
        max_id = pd.read_sql('SELECT MAX(ID) FROM SF_TiemposMecanizado_ZFER', self.connection)
        max_id = int(max_id.values[0])
        print(f"Hay {max_id} ZFER activos en la tabla")
        df_newzfers = df_newzfers.assign(ID = lambda x: range(max_id+1, max_id + len(df_newzfers) + 1))
        df_newzfers = df_newzfers.set_index('ID')
        print(f"Se tienen {len(df_newzfers)} nuevos ZFER para agregar")
        self.zfer_create(df_newzfers, dispose=False) # Cargar los nuevos ZFER a la base de datos
        df_update = df_update.dropna(subset='ID')
        df_update = df_update.fillna(0)
        for enum, row in df_update.iterrows():
            self.update_row(row)
        