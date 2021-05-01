# -*- coding: utf-8 -*-
"""
Created on Sat Mar 13 23:00:55 2021

@author: Sebastian
"""

import urllib
import logging
import csv
import pandas as pd
import numpy as np
import sys
import datetime
from dataobjects.stock import Stock
import random


def getFirstDate(gc, isin):
    
    qry = f'select time, close from StockValues where ISIN = \'{isin}\' order by time asc limit 1'
    res = gc.influxClient.query(qry)
    
    if len(res) > 0:
        return res['StockValues'].index[0]
    
    msg = f"No first date found for {isin}"
    gc.numWarnings += 1
    gc.warnMsg += msg + "; "
    return None

def getLastDate(gc, isin):
    
    qry = f'select time, close from StockValues where ISIN = \'{isin}\' order by time desc limit 1'
    res = gc.influxClient.query(qry)
    
    if len(res) > 0:
        return res['StockValues'].index[0]
    
    msg = f"No last date found for {isin}"
    gc.numWarnings += 1
    gc.warnMsg += msg + "; "
    
    return None


def grabStock(gc, stock):
    """This loads the prices of the stock and stores it in influxdb. It queries 
    the latest available price and loads the data between the available price
    and now."""
    logger = logging.getLogger(__name__)
    
    try:
       
        logger.info(f'Loading Stock {stock.NameShort}, ISIN: {stock.ISIN}')
        gc.writeJobStatus("Running", statusMessage=f'Loading Stock {stock.NameShort}, ISIN: {stock.ISIN}')
        
        startDate = "01.01.1900"
        endDate = datetime.datetime.now().strftime("%d.%m.%Y")
        
        if (gc.influx_version == 1):
            # get timestamp of latest point
            qry = f'select time, close from StockValues where ISIN = \'{stock.ISIN}\' order by time desc limit 1'
            res = gc.influxClient.query(qry)
            
            if len(res) > 0:
                ts = res['StockValues'].index[0]
                ts = ts - datetime.timedelta(days=2)
                startDate = ts.strftime("%d.%m.%Y")
            
        else:
            qry = f'from(bucket: \"{gc.influx_db}\") \
                        |> range(start: 0)  \
                        |> filter(fn: (r) => \
                            r.ISIN == \"{stock.ISIN}\" and r._field == \"close\")   \
                        |> sort(columns: ["_time"], desc: true) \
                        |> first()'
            res = gc.influx_query_api.query_data_frame(qry)
            
            if len(res) > 0:
                ts = res.loc[0]['_time']
                ts = ts - datetime.timedelta(days=2)
                startDate = ts.strftime("%d.%m.%Y")
                #print(startDate)
        
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

        if (len(df.index) == 0):
            msg = f"No Prices found for Stock {stock.NameShort}, ISIN: {stock.ISIN}"
            logger.error(msg)
            gc.errMsg += (msg + "; ")
            gc.writeJobStatus("Running", statusMessage=f'Loading Stock {stock.NameShort}, ISIN: {stock.ISIN} - ERROR')
            return

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
        
        saveStock(gc, df, stock)
        
        # if (gc.influx_version == 1):
        #     gc.influxClient.write_points(df, "StockValues", {'ISIN': stock.ISIN, 'Name': stock.NameShort}, protocol='line', batch_size=500)
        # else:
        #     df['ISIN'] = stock.ISIN
        #     df['Name'] = stock.Name
        #     gc.influx_write_api.write(gc.influx_db, gc.influx_org, record=df, data_frame_measurement_name="StockValues", data_frame_tag_columns=['ISIN', 'Name'])
        
        gc.writeJobStatus("Running", statusMessage=f'Loading Stock {stock.NameShort}, ISIN: {stock.ISIN} - DONE')
    
    except Exception as e:
        logger.exception(f'Crash grabbing stock {stock.ISIN}', exc_info=e)
        gc.numErrors += 1
        gc.errMsg += f'Crash grabbing stock {stock.ISIN}; '
        
        
def createTestStocks(gc):
    """This will create the following test stocks:
        ISIN: TEST0001: All values are 100
        ISIN: TEST0002: All values are 200
        
        ISIN: TEST0003: linear increase from 100 to 200
        """
    logger = logging.getLogger(__name__)
    
    try:
        msg = "Starting createTestStocks"
        logger.debug(msg)
        gc.writeJobStatus("Running", statusMessage=msg)
        
        random.seed(4711)

        start_short = '2021-03-01'
        start_long = '2000-01-01'
        end = '2021-03-31'
        
        s = Stock('TEST0001')
        s.NameShort = "TestStock 100"
        s.ComdirectId = -1
        gc.ses.add(s)
        gc.ses.commit()
        rng = pd.bdate_range(start_short, end)
        df = pd.DataFrame({'open': 100.0, 'high': 100.0, 'low': 100.0, 'close': 100.0, 'volume': 100.0 }, index = rng) 
        
        saveStock(gc, df, s)
        # if (gc.influx_version == 1):
        #     gc.influxClient.write_points(df, "StockValues", {'ISIN': s.ISIN, 'Name': s.NameShort}, protocol='line')
        # else:
        #     df['ISIN'] = s.ISIN
        #     df['Name'] = s.NameShort
        #     gc.influx_write_api.write(gc.influx_db, gc.influx_org, record=df, data_frame_measurement_name="StockValues", data_frame_tag_columns=['ISIN', 'Name'])
 
        s = Stock('TEST0002')
        s.NameShort = "TestStock 200"
        s.ComdirectId = -1
        gc.ses.add(s)
        gc.ses.commit()
        df = pd.DataFrame({'open': 200.0, 'high': 200.0, 'low': 200.0, 'close': 200.0, 'volume': 200.0 }, index = rng) 
        
        saveStock(gc, df, s)
        # if (gc.influx_version == 1):
        #     gc.influxClient.write_points(df, "StockValues", {'ISIN': s.ISIN, 'Name': s.NameShort}, protocol='line')
        # else:
        #     df['ISIN'] = s.ISIN
        #     df['Name'] = s.NameShort
        #     gc.influx_write_api.write(gc.influx_db, gc.influx_org, record=df, data_frame_measurement_name="StockValues", data_frame_tag_columns=['ISIN', 'Name'])
 

        s = Stock('TEST0003')
        s.NameShort = "TestStock linear ascending"
        s.ComdirectId = -1
        gc.ses.add(s)
        gc.ses.commit()
        val = np.arange(100.0, 100.0 + len(rng), 1.0)
        df = pd.DataFrame({'open': val, 'high': val, 'low': val, 'close': val, 'volume': val }, index = rng) 
        
        saveStock(gc, df, s)
        # if (gc.influx_version == 1):
        #     gc.influxClient.write_points(df, "StockValues", {'ISIN': s.ISIN, 'Name': s.NameShort}, protocol='line')
        # else:
        #     df['ISIN'] = s.ISIN
        #     df['Name'] = s.NameShort
        #     gc.influx_write_api.write(gc.influx_db, gc.influx_org, record=df, data_frame_measurement_name="StockValues", data_frame_tag_columns=['ISIN', 'Name'])
   
        interestStock(gc, "TEST0010", 1, start_long, end, 0, 0.0)
        interestStock(gc, "TEST0011", 5, start_long, end, 0, 0.0)
        interestStock(gc, "TEST0012", 10, start_long, end, 0, 0.0)
        interestStock(gc, "TEST0013", -1, start_long, end, 0, 0.0)
        interestStock(gc, "TEST0014", -5, start_long, end, 0, 0.0)
        interestStock(gc, "TEST0015", -10, start_long, end, 0, 0.0)
        
        interestStock(gc, "TEST0020", 1, start_long, end, 5, 0.01)
        interestStock(gc, "TEST0021", 5, start_long, end, 5, 0.01)
        interestStock(gc, "TEST0022", 10, start_long, end, 5, 0.01)
        interestStock(gc, "TEST0023", -1, start_long, end, 5, 0.01)
        interestStock(gc, "TEST0024", -5, start_long, end, 5, 0.01)
        interestStock(gc, "TEST0025", -10, start_long, end, 5, 0.02)

    
        gc.writeJobStatus("Running", statusMessage=msg + " - DONE")
        logger.debug(msg + " - DONE")
        
        #sys.exit()
        
    except Exception as e:
        logger.exception('Crash!', exc_info=e)
        gc.numErrors += 1
        gc.errMsg += "Crash createTestStocks; "
        
        
        
def interestStock(gc, isin, int_rate, start, end, jitter, momentum):
    
    s = Stock(isin)
    s.NameShort = f"TestStock interest {int_rate}%, jitter {jitter}, momentum {momentum}"
    s.ComdirectId = -1
    gc.ses.add(s)
    gc.ses.commit()
    rng = pd.bdate_range(start, end)
    val = [100.0] * len(rng)
    pos = val[0]
    for i in range(len(val)):
        if (jitter > 0):
            pos0 = 100 * (1+int_rate/100) ** ((rng[i] - rng[0]).days / 365.25) # value without jitter
            d = pos0 - pos
            #step = random.uniform(-jitter + d*momentum, jitter + d*momentum)
            step = random.uniform(-min(jitter - d*momentum, pos-0.1), (jitter + d*momentum))
            pos = pos + step
            val[i] = pos
        else:
            val[i] = 100 * (1+int_rate/100) ** ((rng[i] - rng[0]).days / 365.25)
        
        
    df = pd.DataFrame({'open': val, 'high': val, 'low': val, 'close': val, 'volume': val }, index = rng) 
    
    saveStock(gc, df, s)
        # if (gc.influx_version == 1):
        #     gc.influxClient.write_points(df, "StockValues", {'ISIN': s.ISIN, 'Name': s.NameShort}, protocol='line')
        # else:
        #     df['ISIN'] = s.ISIN
        #     df['Name'] = s.NameShort
        #     gc.influx_write_api.write(gc.influx_db, gc.influx_org, record=df, data_frame_measurement_name="StockValues", data_frame_tag_columns=['ISIN', 'Name'])
 

        
def saveStock(gc, df, s):
    """Saves the content of Dataframe DF to the InfluxDB for stock s"""
    
    loc = locals()
    logger = logging.getLogger(__name__)
    
    try:
        msg = f"Starting saveStock for {s.ISIN}"
        logger.debug(msg)
        gc.writeJobStatus("Running", statusMessage=msg)
        
        if (gc.influx_version == 1):
            gc.influxClient.write_points(df, "StockValues", {'ISIN': s.ISIN, 'Name': s.NameShort}, protocol='line')
        else:
            df['ISIN'] = s.ISIN
            df['Name'] = s.NameShort
            
            x = 0
            step = 200
            for df_chunk in gc.chunk(df, step):
                logger.debug(f"Saving {x} of {len(df.index)}...")
                x += step
                gc.influx_write_api.write(gc.influx_db, gc.influx_org, record=df_chunk, data_frame_measurement_name="StockValues", data_frame_tag_columns=['ISIN', 'Name'])
 
    
    
        gc.writeJobStatus("Running", statusMessage=msg + " - DONE")
        logger.debug(msg + " - DONE")
    except Exception as e:
        logger.exception(f'Crash saveStock with {loc}!', exc_info=e)
        gc.numErrors += 1
        gc.errMsg += f"Crash saveStock with {loc}; "
        
        

