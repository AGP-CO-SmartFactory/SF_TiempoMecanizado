import pandas as pd
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

    def zfer_update(self, df_final: pd.DataFrame):
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
        self.connection.close()
        self.engine.dispose()