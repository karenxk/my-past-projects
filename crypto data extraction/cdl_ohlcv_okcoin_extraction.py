# -*- coding: utf-8 -*-
"""
    CDL
    ===
    This module contains function that works on data from CDL primarily it 
    extracts ohlcv data on Okcoin

"""

# IMPORT PACKAGES --------------------------------------------------------------

import pandas as pd
import ssl # we need to import this library and tweak one setting due to fact we use HTTPS certificate(s)
import datetime
import pytz
import psycopg2
import gzip
import numpy as np

# LOCAL FUNCTIONS --------------------------------------------------------------- 
def convert_date(x):
    """
    Description
    ----------
    Convert date str to 24 hr format
    Parameters
    ----------
        x: (str)
            datetime str e.g. '2020-11-20 12-AM'     

    Return
    ----------
        x: (datetime)
            24 hr datetime
            
    """
    if x[-2:] == 'AM':
        if x[11:13] == '12':
            x = x[:11] + '00:00:00' 
        else:
            x = x[:13] + ':00:00'
    elif x[-2:] == 'PM':
        if x[11:13] == '12':
            x = x[:13] + ':00:00' 
        else:
            x = x[:11] + str(int(x[11:13])+12) +':00:00'
    return x

def collect_ohlcv(url_f, symbol, freq):
    """
    Description
    ----------
    Collect cdl ohlcv data from cdl website
    Parameters
    ----------
        url_f: (str)
            url format, like: "https://www.cryptodatadownload.com/cdd/Okcoin_{}_{}.csv"       
        symbol: (str)
            currency symbol, valid values are: 'BTCUSD','ETHUSD','LTCUSD','XRPUSD'
        freq: (str)
            valid values are: 'd','1h'
    Return
    ----------
        temp: (dataframe)
            ohlcv dataframe
            

    """
    #
    ssl._create_default_https_context = ssl._create_unverified_context
    # retrieve data from website 
    url = url_f.format(symbol, freq)
    print("scraping url: ",url)
    data = pd.read_csv(url,skiprows= 1 )
    
    if data.empty:
        return data
    # format data
    data.columns = ['unix','datetime','symbol','open','high','low','close','amount','volume']
    data['created_at'] = pd.to_datetime(datetime.datetime.now(tz=pytz.utc))
    data['timestamp'] = data['unix']*1000
    if data['datetime'][0][-2:] in ['AM','PM']:
        data['datetime'] = data['datetime'].apply(lambda x: convert_date(x))
        data['datetime']= pd.to_datetime(data['datetime'])
    else:
        data['datetime']= pd.to_datetime(data['datetime'])
    data['exchange']= 'okcoin'
    data['symbol']= data['symbol'].apply(lambda x: (x[:3] + '-' + x[-3:]).lower())
    data['volume']= data['amount']* data['close']
    
    return data[['created_at','timestamp','datetime','symbol','exchange','open','high','low','close','volume']]

   
def generate_ohlcv(url_f, symbol_list, freq):
    """
    Description
    ----------
    generate ohlcv daily data from csv files
    Parameters
    ----------       
        symbol_list: (list)
            currency symbol
        freq: (str)
            valid values are: 'd','1h', etc.
    Return
    ----------
        temp: (dataframe)
            ohlcv dataframe
            

    """
    df = pd.DataFrame()
    for i in symbol_list:
        temp = collect_ohlcv(url_f, i, freq)
        df = df.append(temp)
        
    mi= str(df['datetime'].min().date()).replace('-','')
    ma= str(df['datetime'].max().date()).replace('-','')
    
    if freq == 'd':
        fq = '1d'
    elif freq == '1h':
        fq = '1h'
        
    df.to_csv('./ex_data/cdl_ohlcv-'+ fq + '_okcoin_'+ mi + '-'+ ma + '.csv.gz', compression = 'gzip')
    
    d_g = df.groupby(['exchange','symbol'])
    t = pd.DataFrame(d_g.agg({'datetime':[np.min,np.max,'count']}))
    t.reset_index(inplace = True)
    t.info()
    t.columns = ['exchange','symbol','start_date','end_date','rows']
    t.to_csv('./ex_data/meta_ohlcv-'+ fq + '_okcoin_'+ mi + '-'+ ma + '.csv')
    
    return df

symbol_list=['BTCUSD','ETHUSD','LTCUSD',]   # XRP currently not available         
url_f = "https://www.cryptodatadownload.com/cdd/Okcoin_{}_{}.csv"
freq = '1h'

a = generate_ohlcv(url_f,symbol_list, freq)


# LOADING TO DATABASE ----------------------------------------------------------

# Connect to SQL Server
conn = psycopg2.connect(
    host="localhost",
    database="cdl",
    user="postgres",
    password="3434")

cur = conn.cursor()

# Create Table
cur.execute('''
            CREATE TABLE okcoin_ohlcv(
                    id serial PRIMARY KEY NOT NULL,
                    created_at timestamptz NOT NULL,
                    timestamp int8 NOT NULL,
                    datetime timestamptz NOT NULL,
                    symbol varchar(100) NOT NULL,
                    exchange varchar(100) NOT NULL,
                    open float8 NULL,
                    high float8 NULL,
                    low float8 NULL,
                    close float8 NULL,
                    volume float8 NULL                                                        
)
    ''')

conn.commit()
            
# load csv to sql database
with gzip.open('./ex_data/cdl_ohlcv_1h_okcoin.csv.gz', 'rb') as f:
    # Notice that we don't need the `csv` module.
    next(f) # Skip the header row.
    cur.copy_from(f, 'okcoin_ohlcv', sep=',')
    
conn.commit()





