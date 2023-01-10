import pandas as pd
import numpy as np
# Defining the function for the analysis of Flat rejections
def flat_rejections(df_ryrp):
    df_ryr_plano = df_ryrp[np.where(df_ryrp['TIPO'] == 'RECHAZO PLAN', True, False)] 
    

