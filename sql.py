import pandas as pd
import sqlalchemy 
from sqlalchemy.engine import URL
from dotenv import dotenv_values

# Variables de entorno del código
class Loader:
    def __init__(self, tabla):
        self.basetable = tabla
        params_dict = dotenv_values('C:\envs\zferbp_envar.env')
        SERVER = params_dict['SERSF']
        UID = params_dict['UIDSF']
        PWD = params_dict['PWDSF']
        DB = params_dict['DATSF']
        DRIVER = 'ODBC Driver 18 for SQL Server'
        connection_url = URL.create("mssql+pyodbc", username=UID, password=PWD,
            host=SERVER, database=DB, query={"driver": DRIVER, "TrustServerCertificate": "yes",})
        self.engine = sqlalchemy.create_engine(connection_url)
        self.meta = sqlalchemy.MetaData()

    def borrar_datos_antiguos(self):
        '''
        Borra los datos almacenados que sean menores a una fecha límite inicial

        Parameters
        ----------
        fecha_limite : datetime64
            Fecha en la cuál todo lo que se encuentre atrás de esta se borra.
        '''
        print('Borrando datos antiguos')
        tabla_maquina = sqlalchemy.Table(self.basetable, self.meta, autoload_with=self.engine)
        stmt = sqlalchemy.delete(tabla_maquina)
        with self.engine.connect() as connection:
            connection.execute(stmt)
            connection.commit() 
    
    def update_row(self, row):
        tabla_maquina = sqlalchemy.Table(self.basetable, self.meta, autoload_with=self.engine)
        table_update = (sqlalchemy.update(tabla_maquina)
                        .where(tabla_maquina.c.ID == int(row['ID']))
                        .values(BordePaquete = row['BordePaquete'], BordePintura = row['BordePintura'],
                                TiempoMecanizado = row['TiempoMecanizado'], C_Bisel = row['C_Bisel'],
                                C_BrilloC = row['C_BrilloC'], C_BrilloP = row['C_BrilloP'], C_CantoC = row['C_CantoC'],
                                C_Caja = row['C_Caja'], C_Chaflan = row['C_Chaflan']))
        with self.engine.connect() as connection:
            connection.execute(table_update)
               
    def cargar_datos(self, df, basetable=None):
        '''
        Se encarga de cargar los datos a la tabla base de la máquina
    
        Parameters
        ----------
        df : pd.DataFrame
            Dataframe que contiene la información a cargar.
    
        '''
        if not basetable:
            basetable = self.basetable
        print(f'Cargando datos a: {basetable}')
        with self.engine.connect() as connection:
            df.to_sql(name=basetable, con=connection, if_exists='append', index_label='ID')
    
    def update_tablebyrow(self, df_final):
        print('Actualizando tabla con los nuevos registros...')
        unique_zfer = str(df_final['ZFER'].unique().tolist())[1:-1]
        
        with self.engine.connect() as connection:
            df_sql = pd.read_sql(f"""SELECT ID, ZFER, ClaveModelo FROM SF_TiemposMecanizado_ZFER 
                                WHERE ZFER in ({unique_zfer})""", connection)
            df_update = pd.merge(df_final, df_sql, on=['ZFER', 'ClaveModelo'], how='left')
            df_newzfers = df_update[(pd.isna(df_update['ID'])) & (df_update['Material'] != 'nan')]
            max_id = pd.read_sql('SELECT MAX(ID) FROM SF_TiemposMecanizado_ZFER', connection)
            max_id = int(max_id.values[0])
            print(f"Hay {max_id} registros activos en la tabla")
            df_newzfers = df_newzfers.assign(ID = lambda x: range(max_id+1, max_id + len(df_newzfers) + 1))
            df_newzfers = df_newzfers.set_index('ID')
            print(f"Se agregarán {len(df_newzfers)} nuevos registros")
            self.cargar_datos(df_newzfers) # Cargar los nuevos ZFER a la base de datos
            df_update = df_update.dropna(subset='ID')
            df_update = df_update.fillna(0)
            for enum, row in df_update.iterrows():
                self.update_row(row)
        