"""
This code collects and calculates the primary characteristics for a ZFER produced in AGP SGlass CO.
It makes a recollection of all the critical variables in the different databases that the company has
and combines them into a single dataframe that is to be exported to a SQL Server table. 

This code uses sqlalchemy, ezdxf, pandas, numpy and dxfasc, a library written by nikhartam to calculate
the perimeter of a .dxf file. 

The use of this code is exclusive for AGP Glass and cannot be sold or distributed to other companies.
Unauthorized distribution of this code is a violation of AGP intellectual property.

Author: Juan Pablo Rodriguez Garcia (jpgarcia@agpglass.com)
"""
import main
import pyodbc
import pandas as pd

df_dbzfer = main.main()
