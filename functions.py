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
        Esta función agrega los tipos de acabado faltantes a partir del canto inicial, definiendo el número de pasadas:
            Chaflan -> 2 Desbaste plano, 2 Chaflan P1, 1 Chaflan P2
            Bisel -> 2 Desbaste plano, 1 Bisel P1, 1 Bisel P2, 1 Bisel Brillo
            BrilloC -> 2 Desbaste plano, 1 acabado C, 1 Brillo
            BrilloP -> 2 Desbaste plano, 1 acabado plano, 1 Brillo 
            CantoC -> 2 Desbaste plano, 1 acabado C
            CantoP -> 2 Desbaste plano, 1 acabado plano
        '''
        if x['C_Chaflan']:
            x['Desbaste'] = 2
            x['Chaflan1'] = 2
            x['Chaflan2'] = 1
        elif x['C_Bisel']:
            x['Desbaste'] = 2
            x['BiselP1'] = 1
            x['BiselP2'] = 1
            x['BiselBrillo'] = 1
        elif x['C_BrilloC']:
            x['Desbaste'] = 2
            x['AcabadoC'] = 1
            x['BrilloC'] = 1
        elif x['C_BrilloP']:
            x['Desbaste'] = 2
            x['AcabadoPlano'] = 1
            x['BrilloPlano'] = 1
        elif x['C_CantoC']:
            x['Desbaste'] = 2
            x['AcabadoC'] = 1
        elif x['C_CantoP']:
            x['Desbaste'] = 2
            x['AcabadoPlano'] = 1
            
        cambios_herramienta = 0
        for z in [x['C_Chaflan'], x['C_Bisel'], x['C_BrilloC'], x['C_BrilloP'], x['C_CantoC'], x['C_CantoP']]:
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
        df['Perimetro'] = (df['ANCHO']*2 + df['LARGO']*2)*(1-0.089) # Cálculo de perímetro
        df['C_Chaflan'] = 0
        df['C_Bisel'] = 0
        df['C_BrilloC'] = 0
        df['C_BrilloP'] = 0
        df['C_CantoC'] = 0
        df['C_CantoP'] = 0
        # Características para paquetes
        # Definición de cantos planos para paquetes
        df.loc[df['ClaveModelo'] != '01VEXT', 'C_CantoP'] = 1 
        # Definición de chaflan para parabrisas (El 02VP01 es plano y el resto chaflán, la pintura es lo que diga el borde paquete)
        df.loc[(df['ENG_GeometricDiffs'].str.contains('02')) &
               (df['PartShort'] == 'PBS') &
               (df['POSICION'] != 100) &
               (df['POSICION'] != 200) &
               (df['POSICION']%2 == 0), 'C_Chaflan'] = 1
        # Definición de chaflan para laterales (El chaflan inicia desde el 02VP01)
        df.loc[(df['ENG_GeometricDiffs'].str.contains('02')) &
               (df['PartShort'].str.contains('L')) &
               (df['POSICION'] != 100) &
               (df['POSICION']%2 == 0), 'C_Chaflan'] = 1
        
        # Características para pinturas
        # Biseles
        df.loc[((df['POSICION'] == 100) & (df['BordePintura'].str.contains('Bisel'))) |
               ((df['ZFER'].isin(parameters.zfer_biseles)) & (df['POSICION'] == 100)),
                'C_Bisel'] = 1
        # Brillo canto C
        df.loc[(df['POSICION'] == 100) & (df['BordePintura'].str.contains('Canto C Brillante')), 'C_BrilloC'] = 1
        # Brillo Plano
        df.loc[(df['POSICION'] == 100) & (df['BordePintura'].str.contains('Brillante')) &
               (df['C_BrilloC'] == False) & (df['C_Bisel'] == False), 'C_BrilloP'] = 1
        # Canto C
        df.loc[(df['POSICION'] == 100) & (df['BordePintura'].str.contains('Canto C Mate')), 'C_CantoC'] = 1
        # Canto plano
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
        minor_count = [x['BrilloC'], x['BrilloP'], x['Bisel'], x['CantoC'], x['CantoP']].count(True)
        if x['Area'] < 0.023:
            tiempo += round(((x['Perimetro']*minor_count)/avance['AvanceArea0018']).values[0], 2)
        elif x['Area'] < 0.031 and x['Area'] >= 0.023:
            tiempo += round(((x['Perimetro']*minor_count)/avance['AvanceArea0023']).values[0], 2)
        elif x['Area'] < 0.040 and x['Area'] >= 0.031:
            tiempo += round(((x['Perimetro']*minor_count)/avance['AvanceArea0031']).values[0], 2)
        elif x['Area'] < 0.06:
            tiempo += round(((x['Perimetro']*minor_count)/avance['AvanceArea0040']).values[0], 2)
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
            x['Tiempo'] = tiempo + 3 + x['Cambios']*0.25
        return x

    def tiempo_caja(self, x):
        '''
        Suma al tiempo de las piezas un tiempo adicional si la pieza contiene una caja
        en su geometría. Este tiempo se suma al mecanizado del canto del vidrio
        '''
        pass