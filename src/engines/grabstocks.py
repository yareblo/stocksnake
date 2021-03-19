# -*- coding: utf-8 -*-
"""
Created on Sat Mar 13 23:00:55 2021

@author: Sebastian
"""

import urllib
import logging
import csv
import pandas as pd
from influxdb import DataFrameClient


def urlTest(gc):

    logger = logging.getLogger(__name__)
    
    try:
        # url = "https://www.comdirect.de/inf/kursdaten/historic.csv?DATETIME_TZ_END_RANGE_FORMATED=13.03.2021&DATETIME_TZ_START_RANGE_FORMATED=23.11.2011&ID_NOTATION=55081566&INTERVALL=16&WITH_EARNINGS=true"
        df = pd.DataFrame()
        
        try:
            for offset in range(0, 999):
                logger.debug(f'Offset: {offset}')
            
                url = f'https://www.comdirect.de/inf/kursdaten/historic.csv?DATETIME_TZ_END_RANGE_FORMATED=13.03.2021&DATETIME_TZ_START_RANGE_FORMATED=03.02.2020&ID_NOTATION=55081566&INTERVALL=16&OFFSET={offset}&WITH_EARNINGS=false'
            
                df1 = pd.read_csv(url, encoding='ANSI', sep=';', skiprows=1)
                df = df.append(df1)
                #print(df1)
                
                
        except urllib.error.HTTPError as e:
            if e.code == 404:
                logger.debug("Reached end")
            else:
                logger.exception('Crash!', exc_info=e)

        #print(df)
        df['TimeStamp'] = pd.to_datetime(df['Datum'])
        df['ISIN'] = 'WKN_4711'
        df = df.set_index('TimeStamp')
        df = df.drop('Datum', axis=1)
        df.rename(columns={'Eröffnung': 'open','Hoch': 'high', 'Tief': 'low', 'Schluss':'close', 'Volumen':'volume'}, inplace=True)
        
        with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # more options can be specified also
            pass
            #print(df)
        
        # now write to influx
            
        timeValues  = df[ ['high','low', 'open', 'close', 'volume'] ]
        #timeValues.index = df[['TimeStamp']]
        timeValues.index = df.index
        #tags        = { 'col1': df[['col1']], 'col2': df[['col2']], 'col3':df[['col3']] }
        #tags        = { 'ISIN': df[['ISIN']].Index.drop }
        
        #print(tags)
            
        dbConnDF = DataFrameClient(gc.influx_host, gc.influx_port, '', '', gc.influx_db)
        # dbConnDF.write_points(gc.influx_db, 'tbName', timeValues, tags = tags)
        dbConnDF.write_points(df, gc.influx_db, protocol='line')
        
    
    except Exception as e:
        logger.exception('Crash!', exc_info=e)
