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
    'PWDSF': os.environ.get('PWDSF'),
    'SERCO': os.environ.get('SERCO'),
    'DATCO': os.environ.get('DATCO'),
    'UIDCO': os.environ.get('UIDCO'),
    'PWDCO': os.environ.get('PWDCO'),
    'SERGN':os.environ.get('SERGN'),
    'DATGN':os.environ.get('DATGN'),
    'UIDGN':os.environ.get('UIDGN'),
    'PWDGN':os.environ.get('PWDGN')}

queries = {'query_calendario': """SELECT CodTipoPieza, Orden, ZFER 
                                   FROM TCAL_CALENDARIO_COLOMBIA_DIRECT WHERE LlegoAlmacen = 'False' 
                                   AND Puestodetrabajo not in ('Ingenieria') AND Orden > 0""",
           'query_cal_acabados': """SELECT CAST(ZFER as int) as ZFER, BordePintura, BordePaquete, AbrvPieza as PartShort
                                  FROM TCAL_CALENDARIO_COLOMBIA_DIRECT WHERE LlegoAlmacen = 'False' 
                                  AND ZFER like '7%' ORDER BY Orden DESC""",
           'query_avances': "SELECT * FROM SF_Tabla_AvancesCNC",
           #01 son cajas, 02 son chaflanes y 03 son perforaciones
           'query_caracteristicas': """SELECT MATERIAL as ZFER, ATWRT as ENG_GeometricDiffs FROM ODATA_ZFER_CLASS_001 
                               WHERE ATNAM = 'Z_GEOMETRIC_DIFFERENTIALS'  AND CENTRO = 'CO01'""",
           'query_acabados': """WITH BPNID as (SELECT EdgePaintID, EdgePaintName_ES as BordePintura FROM Seed_Web_GenesisSap_SGlass.MatEdgePaints),
                             	 BPAID as (SELECT BlockEdgeID, BlockEdgeName_ES as BordePaquete FROM Seed_Web_GenesisSap_SGlass.MatBlockEdges)
                                 SELECT DISTINCT SpecID as ZFER, PartShort, BordePintura, BordePaquete
                                 FROM Seed_Web_GenesisSap_SGlass.SalesOrderDetails SOD with (nolock)
                                 	inner join BPNID on SOD.EdgePaintID = BPNID.EdgePaintID
                                 	inner join BPAID on SOD.EdgePacketID = BPAID.BlockEdgeID
                                 WHERE SpecID like '7%'""",
            'zfer_head': """SELECT MATERIAL as ZFER, ZFOR FROM ODATA_ZFER_HEAD with (nolock) WHERE STATUS is null"""}

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
                     500: '05VP04', 599:'35VPR03',
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

zfer_biseles = [700018491, 700075393, 700018112, 700018100, 700143618, 700147234, 700080990, 700018320, 700051716, 
                700126819, 700020794, 700039926, 700047555, 700129366, 700133250, 700018071, 700027561, 700129075, 
                700148839, 700080988, 700149895, 700046196, 700047484, 700046197, 700047485, 700080989, 700149896, 
                700018101, 700046198, 700018106, 700046199, 700047488, 700149897, 700047489, 700149898, 700027561]
    
    

