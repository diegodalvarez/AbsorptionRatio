# -*- coding: utf-8 -*-
"""
Created on Wed Aug 21 10:06:14 2024

@author: Diego
"""

import os
import pandas as pd
import datetime as dt
import yfinance as yf

class DataCollect:
    
    def __init__(self):
        
        self.parent_path = os.path.abspath(os.path.join(os.getcwd(), os.pardir))
        self.data_path = os.path.join(self.parent_path, "data")
        
        if os.path.exists(self.data_path) == False: os.makedirs(self.data_path)
        
        self.start_date = dt.date(year = 2000, month = 1, day = 1)
        self.end_date = dt.date.today()
        
    def get_tickers(self) -> pd.DataFrame:
    
        path = os.path.join(self.data_path, "tickers.csv")
        try:
    
            df_tickers = pd.read_csv(filepath_or_buffer = path, index_col = 0)
            print("Found Data")
    
        except:
    
            print("collecting data")
    
            df_tickers = (pd.read_html(
                "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")
                [0]
                [["Symbol", "Security", "GICS Sector", "GICS Sub-Industry"]].
                reset_index().
                melt(id_vars = "Symbol").
                rename(columns = {"Symbol": "symbol"}).
                assign(variable = lambda x: x.variable.str.lower().str.replace(" ", "_").str.replace("-", "_")).
                pivot(index = "symbol", columns = "variable", values = "value").
                drop(columns = ["index"]).
                reset_index())
    
            print("saving data")
            df_tickers.to_csv(path_or_buf = path)
    
        return df_tickers
    
    def get_spx_names(self) -> pd.DataFrame: 
        
        tickers_path = os.path.join(self.data_path, "tickers.csv")
        tickers = (pd.read_csv(
            filepath_or_buffer = tickers_path).
            symbol.
            drop_duplicates().
            to_list())
        
        path = os.path.join(self.data_path, "equities.parquet")
        try:
    
            df = pd.read_parquet(path = path, engine = "pyarrow")
            print("found data")
    
        except:
    
            print("collecting data")
            df = (yf.download(
                tickers = tickers,
                start = self.start_date,
                end = self.end_date).
                reset_index().
                melt(id_vars = "Date"))
    
            print("saving data")
            df.to_parquet(path = path, engine = "pyarrow")
    
        return df
    
    def get_spx_rtn(self) -> pd.DataFrame:
        
        spx_path = os.path.join(self.data_path, "Indices.parquet")
        
        try:
            
            df = pd.read_parquet(path = spx_path, engine = "pyarrow")
            print("found data")
            
        except:
            
            print("collecting data")
            df = (yf.download(
                tickers = ["SPY", "AGG", "TLT"],
                start = self.start_date,
                end = self.end_date))
            
            print("Saving data")
            df.to_parquet(path = spx_path, engine = "pyarrow")

        return df   
    
def main():
    
    DataCollect().get_tickers()
    DataCollect().get_spx_names()
    DataCollect().get_spx_rtn()
    
if __name__ == "__main__": main()
        
        