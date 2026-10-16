# -*- coding: utf-8 -*-
"""
Pruebas de Loader.reemplazar_datos con SQLite en memoria (no tocan SQL Server).
Ejecutar desde la raiz del repo:  python -m unittest tests.sql_loader_test
"""
import os
import sys
import types
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    import pyodbc  # noqa: F401
except ImportError:  # entorno de desarrollo sin el driver: connections.py solo necesita pyodbc.drivers()
    stub = types.ModuleType('pyodbc')
    stub.drivers = lambda: []
    sys.modules['pyodbc'] = stub

import pandas as pd
import sqlalchemy
from sqlalchemy import text

import sql


class TestReemplazarDatos(unittest.TestCase):
    def setUp(self):
        self.loader = object.__new__(sql.Loader)  # evita leer C:\envs y abrir SQL Server
        self.loader.basetable = 'T'
        self.loader.engine = sqlalchemy.create_engine('sqlite://')
        self.loader.meta = sqlalchemy.MetaData()
        with self.loader.engine.begin() as conn:
            conn.execute(text('CREATE TABLE T (ID INTEGER PRIMARY KEY, v TEXT)'))
            conn.execute(text("INSERT INTO T VALUES (1, 'viejo1'), (2, 'viejo2')"))

    def contenido(self):
        with self.loader.engine.connect() as conn:
            return [tuple(r) for r in conn.execute(text('SELECT ID, v FROM T ORDER BY ID'))]

    def test_reemplaza_todo_el_contenido(self):
        df = pd.DataFrame({'v': ['n1', 'n2', 'n3']}, index=pd.Index([10, 11, 12], name='ID'))
        self.loader.reemplazar_datos(df)
        self.assertEqual(self.contenido(), [(10, 'n1'), (11, 'n2'), (12, 'n3')])

    def test_si_falla_la_carga_se_conservan_los_datos_anteriores(self):
        df = pd.DataFrame({'v': ['a', 'b']}, index=pd.Index([5, 5], name='ID'))  # ID duplicado
        with self.assertRaises(Exception):
            self.loader.reemplazar_datos(df)
        self.assertEqual(self.contenido(), [(1, 'viejo1'), (2, 'viejo2')])

    def test_dataframe_vacio_no_toca_la_tabla(self):
        with self.assertRaises(ValueError):
            self.loader.reemplazar_datos(pd.DataFrame({'v': []}))
        self.assertEqual(self.contenido(), [(1, 'viejo1'), (2, 'viejo2')])


if __name__ == '__main__':
    unittest.main()
