import os
from dotenv import load_dotenv
"""
Class that handles the connections to the SQL servers
"""
load_dotenv('connection.env')
class Connection:
    def __init__(self, identifier):
        self.identifier = identifier
        parameters = self.handler(self.identifier)
        self.driver = os.environ.get("DRIVER")
        self.server = parameters[0]
        self.database = parameters[1]
        self.uid = parameters[2]
        self.pwd = parameters[3]
    
    def handler(self, identifier):       
        if identifier == 1:
            server = os.environ.get("SERWR")
            database = os.environ.get("DATWR")
            uid = os.environ.get("UIDWR")
            pwd = os.environ.get("PWDWR")
            
        if identifier == 2:
            server = os.environ.get("SERSAGA")
            database = os.environ.get("DATSAGA")
            uid = os.environ.get("UIDSAGA")
            pwd = os.environ.get("PWDSAGA")    
        
        if identifier == 3:
            server = os.environ.get("SERCAL")
            database = os.environ.get("DATCAL")
            uid = os.environ.get("UID")
            pwd = os.environ.get("PWD")  

        parameters = [server, database, uid, pwd]
        return parameters