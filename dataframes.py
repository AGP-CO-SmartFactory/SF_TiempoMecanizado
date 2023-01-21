import os
from datetime import datetime as dt
from datetime import date, timedelta
from dotenv import load_dotenv
import pandas as pd
import numpy as np
import json

#import datetime as dt
load_dotenv('connection.env')
pd.options.mode.chained_assignment = None
HOY = dt.today()
pd.options.display.float_format = '{:.2f}'.format

# TO DO: ADD 'BordePaquete' COLUMN
COLUMNS = ['AgpDuraP', 'Gunport', 'HeatingWiredHeatplex', 'HeatingMetalCoating',
           'HighPerformance', 'LightWeight', 'MetallicSupportForMirror', 
           'MultiHit', 'SilverPaste', 'SolarPlus', 'SteelPlus', 'SunAdvance', 
           'SunBand', 'TNT', 'TNTFlex', 'Frames', 'EncapsulatedFrames', 
           'HeatingGlass', 'CuttedMass', 'BlockChamfered', 'BlackBand', 
           'CameraShade', 'MirrorShade', 'Perforations', 'SensorShade', 
           'VinShade']

"""
Database retrieval
"""
class Dataframe:
    def __init__(self, conn, identifier=5):
        self.filedate = 0
        self.identifier = identifier
        self.connection = conn
        self.dataframe = self.loaddataframe(identifier)

    def loaddataframe(self, identifier):
        def check_date(path):
            try:
                file_date = os.path.getmtime(path)
                self.filedate = file_date
            except:
                pass
            else:
                file_date = dt.utcfromtimestamp(file_date).strftime('%D')
                file_date = dt.strptime(file_date, '%m/%d/%y')
                now = dt.now().strftime('%D')
                now = dt.strptime(now, '%m/%d/%y')
                delta = now - file_date
                if delta.days >= 15:
                    os.remove(path)

        if identifier == 0:
            path = './data/df_mats.csv'
            check_date(path)
            # Loading the dataframe
            try:
                df = pd.read_csv(path)
                return df
            except:
                df = pd.read_sql_query("With A as (Select OP.Ordem_Serial, OP.Ordem_CodMaterial, OP.Ordem_Centro, CASE WHEN D.Desenho_Descricao LIKE '%BORO%' THEN 1 ELSE 0 END AS ALUMINUM, CASE WHEN D.Desenho_Descricao LIKE 'MAAC%' THEN 1 ELSE 0 END AS ACERO, CASE WHEN D.Desenho_Descricao LIKE '%TNT%' THEN 1 ELSE 0 END AS MALLA, CASE WHEN D.Desenho_Descricao LIKE 'CPET%' THEN 1 ELSE 0 END AS AL, CASE WHEN D.Desenho_Descricao LIKE 'PVTE%' THEN 1 ELSE 0 END AS TEJIDO, D.Desenho_Descricao from SAGA_OrdensProducao OP inner join SAGA_Desenhos D on D.Desenho_OrdemSerial = OP.Ordem_Serial where Ordem_Centro in ('CO01') and (Desenho_Descricao like '%BORO%' OR Desenho_Descricao like 'MAAC%' OR Desenho_Descricao like '%TNT%' or Desenho_Descricao like 'CPET%' or Desenho_Descricao like 'PVTE%')) Select Ordem_Serial, CAST (Ordem_CodMaterial as int) as Ordem_CodMaterial, Ordem_Centro, SUM(ALUMINUM) as Aluminum, SUM(ACERO) as Acero, SUM(MALLA) as Malla, SUM(AL) as AL, SUM(TEJIDO) as Tejido From A Group by Ordem_Serial, Ordem_CodMaterial, Ordem_Centro", self.connection)
                df.to_csv(path, index=False)
                return df

        if identifier == 1:
            SQLTABLE1 = os.environ.get('SQLTABLEWR1')
            SQLTABLE2 = os.environ.get('SQLTABLEWR2')
            path = './data/df_WebReader.csv'
            check_date(path)
            # Loading the dataframe
            try:
                df = pd.read_csv(path, index_col='ZFER')
                return df
            except:
                df = pd.read_sql_query(f"SELECT DXF_KEY, DXF_NAME, DXF_HEIGHT, DXF_WIDTH, DXF_MIN_RECT, DXF_AREA, DXF_PERIM from {SQLTABLE1} D inner join {SQLTABLE2} C on C.Center_Key = D.DXF_CENTER_KEY WHERE DXF_NAME <> '' and C.Center_Code= 'CO01'", self.connection)
                df.to_csv(path, index=True)
                return df   
        
        if identifier == 2:
            path = './data/df_zferlist.csv'
            check_date(path)
            # Loading the dataframe
            try:
                df = pd.read_csv(path)
                return df
            except:
                df = pd.read_sql_query("SELECT DISTINCT CAST(OP.Ordem_CodMaterial as int) as Ordem_CodMaterial, D.Desenho_Name from SAGA_OrdensProducao OP inner join SAGA_Desenhos D on D.Desenho_OrdemSerial = OP.Ordem_Serial where Ordem_Centro in ('CO01') and D.Desenho_ZTipo LIKE '%VD%'", self.connection)
                df.to_csv(path, index=False)
                return df
