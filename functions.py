"""
The use of this code is exclusive for AGP Glass and cannot be sold or distributed to other companies.
Unauthorized distribution of this code is a violation of AGP intellectual property.

Author: Juan Pablo Rodriguez Garcia (jpgarcia@agpglass.com)
"""
import parameters
import pandas as pd
class Functions:
    def __init__(self, sf_conn):
        self.df_avances = pd.read_sql(parameters.queries['query_avances'], sf_conn)

    def agregar_pasadas(self, x):
        '''
        Esta función agrega los tipos de acabado faltantes a partir del canto inicial:
            Canto C -> Se le agrega canto P.
            Brillo -> Se le agrega canto P y canto C.
            Bisel -> Se le agrega canto P y canto C.
        '''
        if x['BrilloC']:
            x['CantoC'] = True
            x['CantoP'] = True
        elif x['Bisel']:
            x['CantoC'] = True
            x['CantoP'] = True
            x['BrilloC'] = True
        elif x['BrilloP']:
            x['CantoC'] = True
            x['CantoP'] = True
        elif x['CantoC']:
            x['CantoP'] = True
        cambios_herramienta = 0
        for z in [x['BrilloC'], x['CantoC'], x['CantoP'], x['Bisel'], x['BrilloP']]:
            if z:
                cambios_herramienta+=1
        x['Cambios'] = cambios_herramienta
        return x
    
    def definir_cantos(self, df_cambio):
        '''
        Esta función se encarga de definir el canto más predominante en cada vidrio para asignarselo y retornarlo
        como un mismo dataframe
        '''
        df = df_cambio.copy()
        df['Perimetro'] = (df['ANCHO']*2 + df['LARGO']*2)*(1-0.089)
        df['Area'] = (df['ANCHO'] * df['LARGO'])/1000000
        df['Bisel'] = df.apply(lambda x: bool((x['ClaveModelo'] == '01VEXT' and 'Bisel' in x['BordePintura']) 
                                            or (x['ZFER'] in parameters.zfer_biseles and x['ClaveModelo'] == '01VEXT') 
                                            and x['Operacion1'] == 'MECANIZADO'), axis=1)
        df['BrilloC'] = df.apply(lambda x: bool((x['ClaveModelo'] == '01VEXT' or x['Operacion2'] == 'SERIGRAFIA') 
                                                and 'Brillante' in x['BordePintura'] and 'Bisel' not in x['BordePintura'] 
                                                and x['Operacion1'] == 'MECANIZADO' and not x['Bisel']), axis=1)
        df['BrilloP'] = df.apply(lambda x: bool('Brillante' in x['BordePaquete'] 
                                                and (x['ClaveModelo'] == '01VEXT' or x['Operacion2'] == 'SERIGRAFIA') 
                                                and x['Operacion1'] == 'MECANIZADO' and not x['Bisel']), axis=1)        
        df['CantoC'] = df.apply(lambda x: bool((x['ClaveModelo'] == '01VEXT' or x['Operacion2'] == 'SERIGRAFIA') 
                                            and not x['BrilloC'] and not x['BrilloP'] and not x['Bisel'] 
                                            and not x['Operacion1'] == 'MECANIZADO'), axis=1)
        df['CantoP'] = df.apply(lambda x: bool((x['ClaveModelo'] != '01VEXT' and  x['Operacion2'] != 'SERIGRAFIA') 
                                            and x['Operacion1'] == 'MECANIZADO') and not x['Bisel'], axis=1)
        return df  
    
    def tiempo_acabado(self, x):
        '''
        Realiza el cálculo del tiempo a partir de las caracterísiticas en el canto 
        del vidrio
        '''
        avance = self.df_avances[self.df_avances['Referencia'] == x['CLASE']].copy()
        tiempo = 0
        minor_count = [x['BrilloC'], x['BrilloP'], x['Bisel'], x['CantoC'], x['CantoP']].count(True)
        if x['Area'] < 0.023:
            tiempo += round((x['Perimetro']*minor_count)/avance['AvanceArea0018'])
        elif x['Area'] < 0.031 and x['Area'] >= 0.023:
            tiempo += round((x['Perimetro']*minor_count)/avance['AvanceArea0023'])
        elif x['Area'] < 0.040 and x['Area'] >= 0.031:
            tiempo += round((x['Perimetro']*minor_count)/avance['AvanceArea0031'])
        elif x['Area'] < 0.06:
            tiempo += round((x['Perimetro']*minor_count)/avance['AvanceArea0040'])
        if x['BrilloC']:
            tiempo += round((x['Perimetro']/avance['AvanceBrilloC']).values[0], 2)
        if x['BrilloP']:
            tiempo += round((x['Perimetro']/avance['AvanceBrilloPlano']).values[0], 2)
        if x['Bisel']:
            tiempo += round(x['Perimetro']/avance['AvanceBisel'].values[0], 2)
        if x['CantoC']:
            tiempo += round(x['Perimetro']/avance['AvanceCantoC'].values[0], 2)
        if x['CantoP']:
            tiempo += round(x['Perimetro']/avance['AvanceCantoPlano'].values[0], 2)
        if minor_count:
            x['Operacion1'] = 'MECANIZADO'
            x['Tiempo'] = tiempo + 3 + x['Cambios']*0.25
        return x

    def tiempo_caja(self, x):
        '''
        Suma al tiempo de las piezas un tiempo adicional si la pieza contiene una caja
        en su geometría. Este tiempo se suma al mecanizado del canto del vidrio
        '''
        pass