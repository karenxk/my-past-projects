# -*- coding: utf-8 -*-
"""
    CDL
    ===
    This module contains function that works on data from CDL primarily it 
    extracts ohlcv data on coinbase

"""

# IMPORT PACKAGES --------------------------------------------------------------

import pandas as pd
import requests
import json
import numpy as np
import datetime
import pytz

# LOCAL FUNCTIONS --------------------------------------------------------------- 


def fetch_data(symbol,tf):
    """
    Description
    ----------
    generate ohlcv data from csv files
    Parameters
    ----------       
        symbol: (str)
            currency symbol, valid form: 'BTC-USD'
        tf: (int)
            timeframe, valid values are: 86400,3600,60 etc.
    Return
    ----------
        temp: (dataframe)
            ohlcv dataframe
            

    """
    url_form = 'https://api.pro.coinbase.com/products/{}/candles?granularity={}'
    url = url_form.format(symbol,tf)
    response = requests.get(url)
    if response.status_code == 200:  # check to make sure the response from server is good
        data = pd.DataFrame(json.loads(response.text), columns=['unix', 'low', 'high', 'open', 'close', 'amount'])
        data['timestamp'] = data['unix'] * 1000
        data['created_at'] = pd.to_datetime(datetime.datetime.now(tz=pytz.utc))
        data['datetime'] = pd.to_datetime(data['timestamp'], unit='ms')  # convert to a readable date
        data['volume'] = data['amount'] * data['close']      # multiply the BTC volume by closing price to approximate fiat volume
        data['symbol'] = symbol.lower()
        data['exchange'] = 'coinbase'
        # if we failed to get any data, print an error...otherwise write the file
        if data is None:
            print("Did not return any data from Coinbase for " + symbol)
        else:
            return data[['created_at','timestamp','datetime','symbol','exchange','open','high','low','close','volume']]

    else:
        print("Did not receieve OK response from Coinbase API for " + symbol)
        
        
def generate_ohlcv(symbol_list, tf):
    """
    Description
    ----------
    generate ohlcv data from csv files
    Parameters
    ----------       
        symbol_list: (list)
            currency symbol
        tf: (int)
            timeframe, valid values are: 86400,3600,60 etc.
    Return
    ----------
        temp: (dataframe)
            ohlcv dataframe
            

    """
    df = pd.DataFrame()
    for i in symbol_list:
        temp = fetch_data(i,tf)
        df = df.append(temp)
        
    mi= str(df['datetime'].min().date()).replace('-','')
    ma= str(df['datetime'].max().date()).replace('-','')
    
    if tf == 86400:
        fq = '1d'
    elif tf == 3600:
        fq = '1h'
    elif tf == 60:
        fq = '1m'
        
    df.to_csv('./ex_data/cdl_ohlcv-'+ fq + '_coinbase_'+ mi + '-'+ ma + '.csv.gz', compression = 'gzip')
    
    d_g = df.groupby(['exchange','symbol'])
    t = pd.DataFrame(d_g.agg({'datetime':[np.min,np.max,'count']}))
    t.reset_index(inplace = True)
    t.info()
    t.columns = ['exchange','symbol','start_date','end_date','rows']
    t.to_csv('./ex_data/meta_ohlcv-'+ fq + '_coinbase_'+ mi + '-'+ ma + '.csv')
    
    return df

# EXECUTING ---------------------------------------------------------------------    
symbol_list = ['bch-usd','xrp-usd','bsv-usd','xtz-usd','eth-usd','btc-usd','miota-usd','bnb-usd','usdt-usd','ltc-usd','dot-usd','eos-usd','xlm-usd','link-usd','ada-usd','uni-usd','xdc-usd']
tf = 3600
a = generate_ohlcv(symbol_list, tf)
