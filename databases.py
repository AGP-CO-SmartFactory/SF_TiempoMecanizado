import parameters
from connections import Connection
class Databases:
    def __init__(self):
        self.conn_calend = Connection(parameters.conexiones['SERCAL'], parameters.conexiones['DATCAL'], 
                                    parameters.conexiones['UIDCAL'], parameters.conexiones['PWDCAL']).conn
        self.conn_colsap = Connection(parameters.conexiones['SERING'], parameters.conexiones['DATING'], 
                                    parameters.conexiones['UIDING'], parameters.conexiones['PWDING']).conn
        self.conn_produc = Connection(parameters.conexiones['SERCP'], parameters.conexiones['DATCP'], 
                                    parameters.conexiones['UIDCP'], parameters.conexiones['PWDCP']).conn
        self.conn_smartf = Connection(parameters.conexiones['SERSF'], parameters.conexiones['DATSF'], 
                                    parameters.conexiones['UIDSF'], parameters.conexiones['PWDSF']).conn    