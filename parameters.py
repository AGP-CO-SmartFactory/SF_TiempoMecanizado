# -*- coding: utf-8 -*-
"""
Esta sección contiene todos los parámetros que se van a usar en el código y que están por separado o bien que no clasifican
dentro de otro de los códigos. Generálmente, parámetros de conexión, tags o alguna variable dummy que se desee usar
"""
import os
from dotenv import load_dotenv

load_dotenv(r'C:\envs\zferbp_envar.env')

conexiones = {'SERING': os.environ.get('SERING'),
    'DATING': os.environ.get('DATING'),
    'UIDING': os.environ.get('UIDING'),
    'PWDING': os.environ.get('PWDING'),
    'SERWS': os.environ.get('SERWS'),
    'DATWS': os.environ.get('DATWS'),
    'UIDWS': os.environ.get('UIDWS'),
    'PWDWS': os.environ.get('PWDWS'),
    'SERCAL': os.environ.get('SERCAL'),
    'DATCAL': os.environ.get('DATCAL'),
    'UIDCAL': os.environ.get('UIDCAL'),
    'PWDCAL': os.environ.get('PWDCAL'),
    'SERCP': os.environ.get('SERCP'),
    'DATCP': os.environ.get('DATCP'),
    'UIDCP': os.environ.get('UIDCP'),
    'PWDCP': os.environ.get('PWDCP'),}

queries = {'query_calendario': "SELECT CodTipoPieza, Vehiculo, Cliente, CAST(Orden as int) Orden, CAST(ZFER as int) as ZFER FROM TCAL_CALENDARIO_COLOMBIA_DIRECT WHERE LlegoAlmacen = 'False' AND Puestodetrabajo not in ('Ingenieria') AND Orden > 0 AND Orden < 99999999"
    }

def create_query(query, dict_name, where=None):
    template = query
    if where:
        template += f' {where}'
    queries[dict_name] = template
    
    

