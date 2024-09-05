"""
The use of this code is exclusive for AGP Glass and cannot be sold or distributed to other companies.
Unauthorized distribution of this code is a violation of AGP intellectual property.

Author: Juan Pablo Rodriguez Garcia (jpgarcia@agpglass.com)
"""
import parameters
import pandas as pd
class Functions:
    def __init__(self, sf_conn):
        with sf_conn.connect() as connection:
            self.df_avances = pd.read_sql(parameters.queries['query_avances'], connection)

    def agregar_pasadas(self, df):
        '''
        Esta función agrega los tipos de acabado faltantes a partir del canto inicial, definiendo el número de pasadas:
            Chaflan -> 2 Desbaste plano, 2 Chaflan P1, 1 Chaflan P2
            Bisel -> 2 Desbaste plano, 1 Bisel P1, 1 Bisel P2, 1 Bisel Brillo
            BrilloC -> 2 Desbaste plano, 1 acabado C, 1 Brillo
            BrilloP -> 2 Desbaste plano, 1 acabado plano, 1 Brillo 
            CantoC -> 2 Desbaste plano, 1 acabado C
            CantoP -> 2 Desbaste plano, 1 acabado plano
        '''
        df.loc[(df['C_Chaflan'] == 1), ['Desbaste', 'Chaflan1', 'Chaflan2']] = pd.DataFrame([{'Desbaste': 2, 'Chaflan1': 2, 'Chaflan2': 1}] * len(df))
        df.loc[(df['C_Bisel'] == 1), ['Desbaste', 'BiselP1', 'BiselP2', 'BiselBrillo']] = pd.DataFrame([{'Desbaste': 2, 'BiselP1': 1, 'BiselP2': 2, 'BiselBrillo': 1}] * len(df))
        df.loc[(df['C_BrilloC'] == 1), ['Desbaste', 'AcabadoC', 'BrilloC']] = pd.DataFrame([{'Desbaste': 2, 'AcabadoC': 1, 'BrilloC': 1}] * len(df))
        df.loc[(df['C_BrilloP'] == 1), ['Desbaste', 'AcabadoPlano', 'BrilloP']] = pd.DataFrame([{'Desbaste': 2, 'AcabadoPlano': 1, 'BrilloP': 1}] * len(df))
        df.loc[(df['C_CantoC'] == 1), ['Desbaste', 'AcabadoC']] = pd.DataFrame([{'Desbaste': 2, 'AcabadoC': 1}] * len(df))
        df.loc[(df['C_CantoP'] == 1), ['Desbaste', 'AcabadoPlano']] = pd.DataFrame([{'Desbaste': 2, 'AcabadoPlano': 1}] * len(df))
        df = df.fillna(0)
        return df
    
    def definir_cantos(self, df_cambio):
        '''
        Esta función se encarga de definir el canto más predominante en cada vidrio para asignarselo y retornarlo
        como un mismo dataframe
        '''
        df = df_cambio.copy()
        df['Perimetro'] = (df['ANCHO']*2 + df['LARGO']*2)*(1-0.089) # Cálculo de perímetro
        df['C_Chaflan'] = 0
        df['C_Caja'] = 0
        df['C_Bisel'] = 0
        df['C_BrilloC'] = 0
        df['C_BrilloP'] = 0
        df['C_CantoC'] = 0
        df['C_CantoP'] = 0
        
        # Definición de cajas
        df.loc[df['ENG_GeometricDiffs'].str.contains('01'), 'C_Caja'] = 1
        
        # Características para paquetes
        ## Definición de cantos planos para paquetes
        df.loc[df['ClaveModelo'] != '01VEXT', 'C_CantoP'] = 1 
        ## Definición de chaflan para parabrisas (El 02VP01 es plano y el resto chaflán, la pintura es lo que diga el borde paquete)
        df.loc[(df['ENG_GeometricDiffs'].str.contains('02')) &
               (df['PartShort'] == 'PBS') &
               (df['POSICION'] != 100) &
               (df['POSICION'] != 200) &
               (df['POSICION']%2 == 0), 'C_Chaflan'] = 1
        ## Definición de chaflan para laterales (El chaflan inicia desde el 02VP01)
        df.loc[(df['ENG_GeometricDiffs'].str.contains('02')) &
               (df['PartShort'].str.contains('L')) &
               (df['POSICION'] != 100) &
               (df['POSICION']%2 == 0), 'C_Chaflan'] = 1
        
        # Características para pinturas
        ## Biseles
        df.loc[(df['POSICION'] == 100) & (df['BordePintura'].str.contains('Bisel')), 'C_Bisel'] = 1
        ## Brillo canto C
        df.loc[(df['POSICION'] == 100) & (df['BordePintura'].str.contains('Canto C Brillante')), 'C_BrilloC'] = 1
        ## Brillo Plano
        df.loc[(df['POSICION'] == 100) & (df['BordePintura'].str.contains('Brillante')) &
               (df['C_BrilloC'] == False) & (df['C_Bisel'] == False), 'C_BrilloP'] = 1
        ## Canto C
        df.loc[(df['POSICION'] == 100) & (df['BordePintura'].str.contains('Canto C Mate')), 'C_CantoC'] = 1
        ## Canto plano
        df.loc[(df['POSICION'] == 100) & (df['C_Bisel'] == False) & (df['C_BrilloC'] == False) & (df['C_BrilloP'] == False) &
               (df['C_CantoC'] == False), 'C_CantoP'] = 1
        return df
    
    def tiempo_acabado(self, x):
        '''
        Realiza el cálculo del tiempo a partir de las caracterísiticas en el canto 
        del vidrio
        '''
        avance = self.df_avances[self.df_avances['Referencia'] == x['CLASE']].copy()
        if not len(avance):
            print(f"{x['ZFER']} - {x['CLASE']}")
            return x
        tiempo = 0
        
        if x['Desbaste']:
            tiempo += round((x['Perimetro']/avance['DesbastePlano']).values[0], 2) * x['Desbaste']
        if x['AcabadoC']:
            tiempo += round(x['Perimetro']/avance['AcabadoC'].values[0], 2) * x['AcabadoC']
        if x['BrilloC']:
            tiempo += round(x['Perimetro']/avance['BrilloC'].values[0], 2) * x['BrilloC']
        if x['AcabadoPlano']:
            tiempo += round(x['Perimetro']/avance['AcabadoPlano'].values[0], 2) * x['AcabadoPlano']
        if x['BrilloP']:
            tiempo += round(x['Perimetro']/avance['BrilloPlano'].values[0], 2) * x['BrilloP']
        
        # Definición de biseles
        if x['PartShort'] in ['PBS', 'POS']:
            if x['BiselP1']:
                tiempo += round(x['LARGO']/avance['BiselP1'].values[0], 2) * x['BiselP1']
            if x['BiselP2']:
                tiempo += round(x['LARGO']/avance['BiselP2'].values[0], 2) * x['BiselP2']
            if x['BiselBrillo']:
                tiempo += round(x['LARGO']/avance['BiselBrillo'].values[0], 2) * x['BiselBrillo']
        else:
            if x['BiselP1']:
                tiempo += round(2*x['Perimetro']/(3*avance['BiselP1']).values[0], 2) * x['BiselP1']
            if x['BiselP2']:
                tiempo += round(2*x['Perimetro']/(3*avance['BiselP2']).values[0], 2) * x['BiselP2']
            if x['BiselBrillo']:
                tiempo += round(2*x['Perimetro']/(3*avance['BiselBrillo']).values[0], 2) * x['BiselBrillo']
        
        # Definición de chaflanes y cajas
        if len(x['PartShort']):
            if x['PartShort'] == 'PBS':
                if x['Chaflan1']:
                    tiempo += round((x['LARGO']/avance['Chaflan1']).values[0], 2) * x['Chaflan1']
                if x['Chaflan2']:
                    tiempo += round(x['LARGO']/avance['Chaflan2'].values[0], 2) * x['Chaflan2']
                if x['C_Caja']:
                    tiempo += 5       
            elif x['PartShort'][0] == 'L':
                if x['Chaflan1']:
                    tiempo += round(x['Perimetro']/(3*avance['Chaflan1']).values[0], 2) * x['Chaflan1']
                if x['Chaflan2']:
                    tiempo += round(x['Perimetro']/(3*avance['Chaflan2']).values[0], 2) * x['Chaflan2']
                if x['C_Caja']:
                    tiempo += 3
            elif x['PartShort'] == 'POS':
                if x['C_Caja']:
                    tiempo += 4
            
        x['Tiempo'] = tiempo # Corrección para el montaje de las herramientas y la pieza
        return x