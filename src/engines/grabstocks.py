# -*- coding: utf-8 -*-
"""
Created on Sat Mar 13 23:00:55 2021

@author: Sebastian
"""

import urllib
import logging
import csv
import pandas as pd
import sys
import datetime
import dataobjects.stock



def grabStock(gc, stock):

    logger = logging.getLogger(__name__)
    
    try:
        logger.info(f'Loading Stock {stock.NameShort}, ISIN: {stock.ISIN}')
        # get timestamp of latest point
        qry = f'select time, close from StockValues where ISIN = \'{stock.ISIN}\' order by time desc limit 1'
        res = gc.influxClient.query(qry)
        #print (type(res))
        #print(res)
        
        startDate = "01.01.1972"
        endDate = datetime.datetime.now().strftime("%d.%m.%Y")
        
        if len(res) > 0:
            ts = res['StockValues'].index[0]
            ts = ts - datetime.timedelta(days=2)
            startDate = ts.strftime("%d.%m.%Y")
        
        logger.debug(f'StartDate: {startDate}, EndDate: {endDate}')
        
        # url = "https://www.comdirect.de/inf/kursdaten/historic.csv?DATETIME_TZ_END_RANGE_FORMATED=13.03.2021&DATETIME_TZ_START_RANGE_FORMATED=23.11.2011&ID_NOTATION=55081566&INTERVALL=16&WITH_EARNINGS=true"
        df = pd.DataFrame()
        
        try:
            for offset in range(0, 999):
                logger.debug(f'Offset: {offset}')
                url = f'https://www.comdirect.de/inf/kursdaten/historic.csv?DATETIME_TZ_END_RANGE_FORMATED={endDate}&DATETIME_TZ_START_RANGE_FORMATED={startDate}&ID_NOTATION={stock.ComdirectId}&INTERVALL=16&OFFSET={offset}&WITH_EARNINGS=false'
                df1 = pd.read_csv(url, encoding='ANSI', sep=';', skiprows=1)
                logger.debug(f'Rows loaded: {len(df1.index)}')
                df = df.append(df1)
                
        except urllib.error.HTTPError as e:
            if e.code == 404:
                logger.debug(f"Reached end for stock {stock.NameShort}")
            else:
                logger.exception('Crash!', exc_info=e)

        df['TimeStamp'] = pd.to_datetime(df['Datum'], format='%d.%m.%Y')
        df['Eröffnung'] = (df['Eröffnung'].replace('\.','', regex=True).replace(',','.', regex=True).astype(float))
        df['Hoch'] = (df['Hoch'].replace('\.','', regex=True).replace(',','.', regex=True).astype(float))
        df['Tief'] = (df['Tief'].replace('\.','', regex=True).replace(',','.', regex=True).astype(float))
        df['Schluss'] = (df['Schluss'].replace('\.','', regex=True).replace(',','.', regex=True).astype(float))
        df['Volumen'] = (df['Volumen'].replace('\.','', regex=True).replace(',','.', regex=True).astype(float))
        #df['ISIN'] = 'WKN_4711'
        df = df.set_index('TimeStamp')
        df = df.drop('Datum', axis=1)
        df.rename(columns={'Eröffnung': 'open','Hoch': 'high', 'Tief': 'low', 'Schluss':'close', 'Volumen':'volume'}, inplace=True)
        
        with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # more options can be specified also
            pass
            #print(df.dtypes)
            #print(df)
        
        # now write to influx
        timeValues  = df[ ['high','low', 'open', 'close', 'volume'] ]
        timeValues.index = df.index

        logger.debug(f'Writing {len(df.index)} rows to InfluxDB for stock {stock.Name}')
        gc.influxClient.write_points(df, "StockValues", {'ISIN': stock.ISIN, 'Name': stock.NameShort}, protocol='line')
        
    
    except Exception as e:
        logger.exception('Crash!', exc_info=e)
        gc.numErrors += 1
        gc.errMsg += f'Crash grabbing stock {stock.ISIN}; '
