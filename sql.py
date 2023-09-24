import pandas as pd
import sqlalchemy 
from sqlalchemy.engine import URL
import os
from dotenv import load_dotenv

# Variables de entorno del código
load_dotenv('C:\envs\zferbp_envar.env')
SERVER = os.getenv('SERSF')
UID = os.getenv('UIDSF')
PWD = os.getenv('PWDSF')
DB = os.getenv('DATSF')
DRIVER = 'ODBC Driver 18 for SQL Server'

def data_update(df_final: pd.DataFrame):
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
    print('Actualizando la información a la base de datos...')
    
    connection_url = URL.create(
        "mssql+pyodbc",
        username=UID,
        password=PWD,
        host=SERVER,
        database=DB,
        query={
            "driver": DRIVER,
            "TrustServerCertificate": "yes",})
    
    
    engine = sqlalchemy.create_engine(connection_url)
    connection = engine.connect()
    meta = sqlalchemy.MetaData()
    tabla_maestra = sqlalchemy.Table('SF_TiemposMecanizado', meta, autoload_with=engine)
    
    print('Limpiando tabla...')
    # Limpiando todos los registros que son anteriores a la fecha inicial de la busqueda en Historian
    table_deletion = (sqlalchemy.delete(tabla_maestra))        
    connection.execute(table_deletion)

    print('Cargando datos...')
    df_final.to_sql('SF_TiemposMecanizado', connection, if_exists='append', index_label='ID')
