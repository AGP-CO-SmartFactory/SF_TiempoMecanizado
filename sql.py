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
        self.connection = self.engine.connect()
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
        self.connection.execute(stmt)
        self.connection.commit() 
    
    def update_row(self, row):
        tabla_maquina = sqlalchemy.Table(self.basetable, self.meta, autoload_with=self.engine)
        table_update = (sqlalchemy.update(tabla_maquina)
                        .where(tabla_maquina.c.ID == int(row['ID']))
                        .values(BordePaquete = row['BordePaquete'], BordePintura = row['BordePintura'],
                                TiempoMecanizado = row['TiempoMecanizado'], Bisel = row['Bisel'],
                                BrilloC = row['BrilloC'], BrilloP = row['BrilloP'], CantoC = row['CantoC']))
        self.connection.execute(table_update)
               
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
        df.to_sql(name=basetable, con=self.connection, if_exists='append', index_label='ID')
        self.connection.commit()        
    
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
        self.cargar_datos(df_newzfers) # Cargar los nuevos ZFER a la base de datos
        df_update = df_update.dropna(subset='ID')
        df_update = df_update.fillna(0)
        for enum, row in df_update.iterrows():
            self.update_row(row)
        self.connection.commit()
        