from sqlalchemy.engine import URL
import sqlalchemy

"""
Class that handles the connections to the SQL servers
"""
class Connection:
    def __init__(self, server, database, uid, pwd):
        self.driver = 'ODBC Driver 18 for SQL Server'
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
        conn = engine.connect()
        return conn