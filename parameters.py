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
    'PWDCP': os.environ.get('PWDCP'),
    'SERSF': os.environ.get('SERSF'),
    'DATSF': os.environ.get('DATSF'),
    'UIDSF': os.environ.get('UIDSF'),
    'PWDSF': os.environ.get('PWDSF')}

queries = {'query_calendario': "SELECT CodTipoPieza, CAST(Orden as int) Orden, CAST(ZFER as int) as ZFER, BordePintura, BordePaquete FROM TCAL_CALENDARIO_COLOMBIA_DIRECT WHERE LlegoAlmacen = 'False' AND Puestodetrabajo not in ('Ingenieria') AND Orden > 0 AND Orden < 99999999",
           'query_avances': "SELECT * FROM SF_Tabla_AvancesCNC"
    }

def create_query(query, dict_name, where=None):
    template = query
    if where:
        template += f' {where}'
    queries[dict_name] = template

dict_clavesmodelo = {99: '32VPMO',
                     100: '01VEXT', 199:'33VPR01',
                     200: '02VP01', 299:'34VPR02',
                     300: '03VP02', 399:'35VPR03',
                     400: '04VP03', 499:'35VPR03',
                     500: '05VP04',
                     600: '06VP05',
                     700: '07VP06',
                     800: '08VP07',
                     900: '09VP08',
                     1000: '10VP09',
                     1100: '11VP10',
                     1200: '12VP11',
                     1300: '13VP12',
                     1400: '14VP13',
                     1500: '15VP14',
                     1600: '16VP15',
                     1700: '17VP16',
                     1800: '18VP17',
                     1900: '19VP18',
                     3600: '36VTPA',
                     3700: '37VSTP',
    }
    
    

