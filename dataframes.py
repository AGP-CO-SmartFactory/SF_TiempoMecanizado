import os
from datetime import datetime as dt
from dotenv import load_dotenv
import pandas as pd
import numpy as np
import queries

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
                df = pd.read_sql_query(f"SELECT DXF_NAME, DXF_AREA, DXF_PERIM from {SQLTABLE1} D inner join {SQLTABLE2} C on C.Center_Key = D.DXF_CENTER_KEY WHERE DXF_NAME <> '' and C.Center_Code= 'CO01'", self.connection)
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
                df = pd.read_sql_query(queries.df_zferlist_query, self.connection)
                df = df.drop_duplicates(subset=['ZFER', 'CLV_MODEL', 'Desenho_Name'], keep='first')
                df.to_csv(path, index=False)
                return df
