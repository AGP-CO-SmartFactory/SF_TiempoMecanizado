# -*- coding: utf-8 -*-
"""
En este archivo se contienen las pruebas unitarias para garantizar que la 
información producida en las funciones tiene sentido
"""

import unittest
import pandas as pd
from random import randint, choice
from functions import Functions
from databases import Databases

db = Databases()
functions = Functions(db.conns['conn_smartfa'])

class TestFunctions(unittest.TestCase): 
    def setUp(self):
        dataset = []
        for i in range(1, 2000):     
            y = {'ZFER': randint(700000000, 799999999),
                    'BordePintura': choice(['Canto C Brillante', 'No Aplica (Pacha)', 'Según Plano', 'Bisel 4X2 Brill + Acero A 1.3mm', '']),
                    'BordePaquete': choice(['MOD. TECOFLEX', 'TECOFLEX RETROFIT', 'MOD. SMOOTH TECOFLEX', '']),
                    'ZFOR': randint(510000000, 519999999),
                    'POSICION': choice([100, 199, 200, 299, 300, 399, 400, 499, 500, 599, 600, 699, 700, 800, 900, 1000, 1100, 3600, 3700]),
                    'CLASE': choice(['Z_VDIN_0400', 'Z_VDIN_0600', 'Z_VDIN_0500', 'Z_VDIN_0300', 'Z_VDIN_0800', 'Z_VDIN_1000', 'Z_VDWH_1200', 'Z_VDWH_1000',
                                        'Z_VDGD_0600_VA', 'Z_VDAL_A_0800_VG', 'Z_VDIN_1200_ALT', 'Z_VDAL_A_0650_VG', 'Z_VDIN_1200', 'Z_VDWH_0800', 'Z_VDGD_0500_VA',
                                        'Z_VDGL_0600_VC', 'Z_VDAL_A_0650_VF', 'Z_VDAL_A_1000_VF', 'Z_VDAL_A_0800_VF', 'Z_VDTEC_15_0600', 'Z_VDIN_1500', 'Z_VDWH_0500',
                                        'Z_VDWH_0600', 'Z_VDTEC_35_0300', 'Z_VDIN_0200','Z_VDIN_0900', 'Z_VDGL_0500_VC', 'Z_VDAL_A_0500_VF', 'Z_VDIN_1900', '']),
                    'ANCHO': randint(1, 2500),
                    'LARGO': randint(1, 2500),
                    'ClaveModelo': choice(['36VTPA', '02VP01', '03VP02', '01VEXT', '33VPR01', '04VP03', '07VP06', '05VP04', '06VP05', '08VP07', '32VPMO', '35VPR03', '37VSTP', '34VPR02', ''])
            }
            y['Area'] = y['ANCHO'] * y['LARGO'] / 1000000
            dataset.append(y)
        self.x = pd.DataFrame(dataset)
        print(self.x.head(20))
    
    def tearDown(self) -> None:
        print('Finalizando prueba')
        return super().tearDown()
    
    def test_definir_cantos(self):
        modified_x = functions.definir_cantos(self.x)
        for index, row in modified_x.iterrows():
            print(f'Vidrio actual {index} - {row["ZFER"]}')
            if row['ClaveModelo'] == '01VEXT':
                if 'Bisel' in row['BordePintura']:
                    self.assertTrue(row['Bisel'], 'El vidrio es un externo y dice que lleva Bisel, por tanto debe ser verdadero')
                else:
                    if 'Brillante' in row['BordePintura']:
                        self.assertTrue(row['BrilloC'], 'Si no es un vidrio externo y pide acabado brillante, el campo BrilloC debe ser verdadero')
            else:
                self.assertFalse(row['Bisel'], 'El vidrio no es un vidrio externo, y por tanto no debe tener Bisel')
                if 'Brillante' in row['BordePaquete']:
                    self.assertTrue(row['BrilloP'], 'Si no es un vidrio externo y pide acabado brillante, el campo BrilloP debe ser verdadero')
