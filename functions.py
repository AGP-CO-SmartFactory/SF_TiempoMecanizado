"""
The use of this code is exclusive for AGP Glass and cannot be sold or distributed to other companies.
Unauthorized distribution of this code is a violation of AGP intellectual property.

Author: Juan Pablo Rodriguez Garcia (jpgarcia@agpglass.com)
"""
import pyodbc, ezdxf, dxfasc, numpy as np

def create_connection(connection):
    """
    Create a connection to a SQL Server database using environment variables.
    This module uses PYODBC and requires MS ODBC Driver for SQL Server installed
    to work

    Parameters
    ----------
    connection : connections.Connection Object
        Object that contains the basic data to create a connection to SQL Server.

    Returns
    -------
    conn : pyodbc.connect Object
        Object that contains all the data from the server to be read from.

    """
    conn = pyodbc.connect(f'DRIVER={connection.driver}'
                            f'SERVER={connection.server}'
                            f'DATABASE={connection.database}'
                            f'UID={connection.uid}'
                            f'PWD={connection.pwd}'
                            f'Trusted_Connection=no;')
    return conn

def perimetro(x):
    """
    Calculate the perimeter of the largest entity on a .dxf file. It assignates all the values
    as a new column to the input. This must be used with the .apply function from Pandas
    """
    try:
        msp = ezdxf.readfile(x['Desenho_Ruta']).modelspace()
    except:
        return x
    else:
        entities = 0    
        perim = 0
        area = 0
        for e in msp:
            if len(msp) > 7:
                break
            entities+=1
            e_type = str(type(e))
            # Especificación para elementos con polilineas
            if e_type == "<class 'ezdxf.entities.polyline.Polyline'>":
                poly_vert = []
                for i in range(len(e)):
                    poly_vert.append(e[i].format('xy'))
                polyperi = dxfasc.polyPerimeter(np.array(poly_vert))
                polyarea = dxfasc.polyArea(np.array(poly_vert))
                if polyperi > perim:
                    perim = polyperi    
                if polyarea > area:
                    area = polyarea
            elif e_type == "<class 'ezdxf.entities.lwpolyline.LWPolyline'>":
                polyperi = dxfasc.polyPerimeter(np.array(list(e.vertices())))
                polyarea = dxfasc.polyArea(np.array(list(e.vertices())))
                if polyperi > perim:
                    perim = polyperi  
                if polyarea > area:
                    area = polyarea
        x['Peri_calc'] = perim
        x['Area_calc'] = area/1000000
        return x

def caja_perforacion(x):   
    """
    Detects if a "MAQ" or "PER" file contains more than one entity and assigns 1 or 0 
    if it contains or not an object respectively.
    """
    msp = ezdxf.readfile(x['Desenho_Ruta']).modelspace()
    entities = len(msp)
    if entities <= 1:
        x['Perforacion'] = 0
    elif entities > 1:
        x['Perforacion'] = 1
    return x 

def definicion_caja(x):
    """
    Makes a calculation based on the difference of perimeters between the 01VEXT and 
    the 02VP01 to assign an approximate value of the dimensions of a box
    """
    if x['Desenho_Name'][-2:] == '00':
        r = 60
        x['h_caja'] = x['DXF_PERIM']-x['PerBase']-(2*np.pi*r)+(8*r)
        x['w_caja'] = (x['ABase']-x['DXF_AREA'])*1000000/x['h_caja']
        if x['h_caja'] > 30:
            x['Caja'] = 1
        else:
            x['Caja'] = 0
            x['h_caja'] = None
            x['w_caja']
    else:
        x['Caja'] = 0
    return x

