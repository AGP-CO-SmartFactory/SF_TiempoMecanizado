
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
from dotenv import load_dotenv
import pandas as pd, numpy as np
import glob, functions
from connections import Connection
from dataframes import Dataframe

def main():
    """
    This is the main function of the program. It makes the connection across all of the servers, combines the data and converts it 
    to a Pandas Dataframe that is returned to be uploaded to a SQL server

    Returns
    -------
    Pandas Dataframe
        Contains all the characteristics of a single ZFER

    """
    # Creating constants
    
    # Loading test environment
    load_dotenv('connection.env')
    
    
    # SQL Server connection
    WR_conn = Connection(identifier=1)
    saga_conn = Connection(identifier=2)
    cal_conn = Connection(identifier=3)
    
    
    conn_parameters_list = [WR_conn, saga_conn, cal_conn]
    conn_list = []
    
    # Creating a PYODBC object for each of the parameters created 
    for connection in conn_parameters_list:
        conn = functions.create_connection(connection)
        conn_list.append(conn)
    
    print('Descargando información...\n')
    df_WR = Dataframe(conn_list[0], 1).dataframe
    df_zferlist = Dataframe(conn_list[1], 2).dataframe
    df_zferlist = df_zferlist.drop_duplicates()
    
    # Adjusting the primary table
    df_WR2 = df_WR.rename({'DXF_NAME': 'Desenho_Name'}, axis=1)
    df_zferlist = pd.merge(df_zferlist, df_WR2, how='left', on='Desenho_Name')
    df_zferlist['CodTipoPieza'] = df_zferlist['Desenho_Name'].str[-2:]
    
    try: 
        dxf = pd.read_csv(r'\\192.168.2.2\general\Smart Factory\dxf_list.csv')
        
    except:
        all_directories = glob.glob(r'\\192.168.2.2\cnc-revisados\**')
        ignored_directories = glob.glob(r'\\192.168.2.2\cnc-revisados\Accesos - NO BORRAR\**')
        all_dir_names = []
        ignored_dir_names = []
        
        for directory in all_directories:
            name = directory.split('\\')[4]
            all_dir_names.append(name)
        
        for directory in ignored_directories:
            name = directory.split('\\')[5].split(' - Acceso directo.lnk')[0]
            ignored_dir_names.append(name)
            
        # Removiendo las carpetas no deseadas de la carpeta de cnc-revisados
        carpetas = []
        for name in all_dir_names:
            if name not in ignored_dir_names:
                carpetas.append(name)
                
        archivos_dxf = []
        for carpeta in carpetas:
            print(carpeta)
            lista_rutas = glob.glob(fr'\\192.168.2.2\cnc-revisados\{carpeta}\**\*.dxf', recursive=True)
            if len(lista_rutas) > 0:
                for ruta in lista_rutas:
                    archivos_dxf.append(ruta)
        
        dxf = []
        for ruta in archivos_dxf:
            nombre_dxf = ruta.split('\\')[-1].split('.dxf')[0]
            diccionario_dxf = {'Desenho_Name': nombre_dxf,
                               'Desenho_Ruta': ruta}
            dxf.append(diccionario_dxf)
        
        dxf = pd.DataFrame(dxf)
        dxf = dxf[np.where(dxf['Desenho_Ruta'].str.contains('OBSOLETO|obsoleto|Obsoleto'), False, True)]
        dxf.to_csv(r'\\192.168.2.2\general\Smart Factory\dxf_list.csv', index=False)
    
    finally:    
        # Construcción de características de archivos MAQ y PER
        df_maq = pd.concat([dxf[dxf['Desenho_Name'].str.startswith('PER')], dxf[dxf['Desenho_Name'].str.startswith('MAQ')]])
        df_maq['DXF_NAME'] = df_maq['Desenho_Name']
        df_maq['Desenho_Name'] = df_maq['Desenho_Name'].str[3:]                
        df_dxfmaq = pd.merge(df_maq, df_WR, how='left', on='DXF_NAME')
        print('Asignando variable de perforación') 
        df_dxfmaq = df_dxfmaq.apply(functions.caja_perforacion, axis=1)
        print('Calculando perímetros de piezas con archivo MAQ')
        df_dxfmaq = df_dxfmaq.apply(functions.perimetro, axis=1)
        df_dxfmaq['DXF_PERIM'] = np.where(df_dxfmaq['DXF_PERIM'].isnull(), df_dxfmaq['Peri_calc'], df_dxfmaq['DXF_PERIM'])
        df_dxfmaq['DXF_AREA'] = np.where(df_dxfmaq['DXF_AREA'].isnull(), df_dxfmaq['Area_calc'], df_dxfmaq['DXF_AREA'])
        df_dxfmaq = df_dxfmaq[['Desenho_Name', 'Desenho_Ruta', 'DXF_NAME', 'DXF_AREA', 'DXF_PERIM', 'Perforacion']]
        buffer = pd.merge(df_zferlist[df_zferlist.columns[0:12]], df_WR.rename({'DXF_NAME': 'Desenho_Name'}, axis=1), on='Desenho_Name', how='left')
        df_dxfmaq = pd.merge(df_dxfmaq, buffer[['Desenho_Name', 'DXF_AREA', 'DXF_PERIM']].rename({'DXF_AREA': 'ABase', 'DXF_PERIM': 'PerBase'}, axis=1))
        df_dxfmaq = df_dxfmaq.drop_duplicates().fillna(0)
       

        print('Calculando dimensiones de cajas')
        df_dxfmaq = df_dxfmaq.apply(functions.definicion_caja, axis=1)
            
        def def_cr(x):
            try:
                df = df_dbzfer[df_dbzfer['ZFER'] == x['ZFER']]
                df0 = df[df['POS'] == '0100'].reset_index()
                df1 = df[df['POS'] == '0200'].reset_index()
                df2 = df[df['POS'] == '0300'].reset_index()
                df3 = df[df['POS'] == '0400'].reset_index()
                if len(df3) > 0:
                    if len(df1) > 0:
                        if df3['Desenho_Name'].values[0] == df2['Desenho_Name'].values[0] and df2['Desenho_Name'].values[0] == df1['Desenho_Name'].values[0]:
                            x['CurvadoRecortado'] = 0
                        else:
                            x['CurvadoRecortado'] = 1
                    else:
                        if df3['Desenho_Name'].values[0] == df2['Desenho_Name'].values[0] and df2['Desenho_Name'].values[0] == df0['Desenho_Name'].values[0]:
                            x['CurvadoRecortado'] = 0
                        else:
                            x['CurvadoRecortado'] = 1
                else:
                    if len(df2) > 0:
                        if len(df1) == 0:
                            if df2['Desenho_Name'].values[0] == df0['Desenho_Name'].values[0]:
                                x['CurvadoRecortado'] = 0
                            else:
                                x['CurvadoRecortado'] = 1
                        else:
                            if df2['Desenho_Name'].values[0] == df1['Desenho_Name'].values[0]:
                                x['CurvadoRecortado'] = 0
                            else:
                                x['CurvadoRecortado'] = 1
                    else:
                        if len(df1) > 0:
                            if df1['Desenho_Name'].values[0] == df0['Desenho_Name'].values[0]:
                                x['CurvadoRecortado'] = 0
                            else:
                                x['CurvadoRecortado'] = 1
                                
            except:
                print(f"{x['ZFER']} had an error while determinating CR, skipping...")
            finally:
                return x      
        
        def def_chaflan(x):
            try:
                df = df_dbzfer[df_dbzfer['ZFER'] == x['ZFER']]
                df1 = df[df['POS'] == '0200']
                df2 = df[df['POS'] == '0300']
                df3 = df[df['POS'] == '0400']
                
                if x['CodTipoPieza'] in ['01', '02', '03', '04', '05', '06', '07', '08', '11', '12', '19', '20']:
                    if len(df3) > 0:
                        if abs(df2['DXF_WIDTH'].values[0] - df3['DXF_WIDTH'].values[0]) > 2 and abs(df1['DXF_WIDTH'].values[0] - df3['DXF_WIDTH'].values[0]) > 4 and df2['DXF_WIDTH'].values[0] != df1['DXF_WIDTH'].values[0]:
                            x['ChaflanPQT'] = 1
                    else:
                        if len(df2) > 0:
                            if abs(df1['DXF_WIDTH'].values[0] - df2['DXF_WIDTH'].values[0]) > 2:
                                x['ChaflanPQT'] = 1
                else:
                    if len(df3) > 0:
                        if abs(df2['DXF_HEIGHT'].values[0] - df3['DXF_HEIGHT'].values[0]) > 2 and abs(df1['DXF_HEIGHT'].values[0] - df3['DXF_HEIGHT'].values[0]) > 4 and df2['DXF_HEIGHT'].values[0] != df1['DXF_HEIGHT'].values[0]:
                            x['ChaflanPQT'] = 1
                    else:
                        if len(df2) > 0:
                            if abs(df1['DXF_HEIGHT'].values[0] - df2['DXF_HEIGHT'].values[0]) > 2:
                                x['ChaflanPQT'] = 1
            except:
                print(f"{x['ZFER']} had an error while determinating chamfering, skipping...")
            finally:
                return(x)
            
        # Desarrollo de la tabla con archivos MAQ y PER
        df_dxfperf = df_dxfmaq[['DXF_NAME', 'Desenho_Name', 'Perforacion', 'Caja', 'w_caja', 'h_caja']]
        df_dbzfer = pd.merge(df_zferlist, df_dxfperf, on='Desenho_Name', how='left').drop_duplicates()
        # Agregando info de curvado recortado
        df_cr = df_dbzfer[df_dbzfer['POS'] == '0100'][['ZFER', 'Desenho_Name', 'POS']]
        print('Asignando variable de curvado recortado')
        df_cr = df_cr.apply(def_cr, axis=1)
        
        # Agregando info de chaflanes
        df_chaflan = df_dbzfer[df_dbzfer['POS'] == '0200'][['ZFER', 'POS', 'DXF_HEIGHT', 'DXF_WIDTH', 'CodTipoPieza']]
        print('Asignando columna de chaflán')
        df_chaflan = df_chaflan.apply(def_chaflan, axis=1)
        
        df_dbzfer = pd.merge(df_dbzfer, df_cr[['ZFER', 'CurvadoRecortado']], on='ZFER', how='left')
        df_dbzfer = pd.merge(df_dbzfer, df_chaflan[['ZFER', 'ChaflanPQT']], on='ZFER', how='left')
        # Creando la tabla y limpiando los nombres de todas las variables
        df_dbzfer = df_dbzfer.rename({'CLV_MODEL': 'ClaveModelo', 'Desenho_Name': 'DesenhoName', 'Desenho_ZTipo': 'ZTipo', 'POS': 'Pos', 
                          'DXF_NAME': 'ArchivoMecanizado', 'DXF_AREA': 'AreaReal', 'DXF_PERIM': 'PerimetroReal', 'CodTipoPieza': 'CodPieza',
                          'DXF_HEIGHT': 'Alto', 'DXF_WIDTH': 'Largo'}, axis=1)
        df_dbzfer = df_dbzfer.fillna({'Acero': 0, 'Al': 0, 'Boro': 0, 'TNT': 0, 'PVTE': 0, 'Caja': 0, 'Perforacion': 0, 'CurvadoRecortado': 0, 'ChaflanPQT': 0, 'ChaflanPintura': 0, 'TQ': 0})
    return df_dbzfer
df_dbzfer = main()
df_dbzfer.to_csv(r'\\192.168.2.2\general\Smart Factory\ZFER_CHAR_CO.csv')
