from sqlalchemy.engine import URL
import sqlalchemy
import os
import pyodbc


def pick_driver():
    """SQL_DRIVER fuerza un driver; si no, usa el 18 y, si no esta instalado, el 17."""
    forced = os.environ.get('SQL_DRIVER')
    if forced:
        return forced
    installed = pyodbc.drivers()
    for name in ('ODBC Driver 18 for SQL Server', 'ODBC Driver 17 for SQL Server'):
        if name in installed:
            return name
    return 'ODBC Driver 18 for SQL Server'

"""
Class that handles the connections to the SQL servers
"""
class Connection:
    def __init__(self, server, database, uid, pwd):
        self.driver = pick_driver()
        self.server = server
        self.database = database
        self.uid = uid
        self.pwd = pwd
        self.conn = self.connect()
    
    def connect(self):       
        connection_url = URL.create(
            "mssql+pyodbc",
            username=self.uid,
            password=self.pwd,
            host=self.server,
            database=self.database,
            query={
                "driver": self.driver,
                "TrustServerCertificate": "yes",})
        engine = sqlalchemy.create_engine(connection_url)
        return engine