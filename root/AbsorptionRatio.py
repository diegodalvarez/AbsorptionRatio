# -*- coding: utf-8 -*-
"""
Created on Wed Aug 21 10:26:34 2024

@author: Diego
"""

import numpy as np
import pandas as pd

class AbsorptionRatio:
    
    def get_absorption_ratio(self, df: pd.DataFrame, n_components: int, lookback_window: int = 500) -> pd.DataFrame: 
        
        df_ratio = pd.DataFrame()
        for i in range(lookback_window, len(df)):
            
            df_tmp = df.iloc[i-lookback_window:i]
            eigenvalues, eigenvectors = np.linalg.eigh((df_tmp - df_tmp.mean()).cov())
            sorted_indices = np.argsort(eigenvalues)[::-1]
            pca_variance = sum(eigenvalues[sorted_indices][0:n_components])
            
            total_variance = df_tmp.var().sum()
            absorption_ratio = pca_variance / total_variance    
            
            date = (df_tmp.query(
                "Date == Date.max()").
                index[0])
        
            df_ratio_tmp = (pd.DataFrame({
                "Date": [date],
                "AR": [absorption_ratio]}))
        
            df_ratio = pd.concat([df_ratio, df_ratio_tmp])
            
        return df_ratio