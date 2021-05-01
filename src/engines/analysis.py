# -*- coding: utf-8 -*-
"""
Created on Sat Apr  3 20:11:12 2021

@author: Sebastian
"""

import logging
import engines.scaffold
import pandas as pd
import datetime

import sys


def buildDepot(gc, df_trans, df_distri, myDepot):

    logger = logging.getLogger(__name__)

    try:
        logger.debug(f'Building depot {myDepot}')
        gc.writeJobStatus("Running", statusMessage=f'Building depot {myDepot}')
        df_full = None

        df_trans['ISIN'] = df_trans['ISIN'].str.strip()
        df_trans['Depot'] = df_trans['Depot'].str.strip()
        df_trans = df_trans[(df_trans['Depot'] == myDepot)]

        df_distri['ISIN'] = df_distri['ISIN'].str.strip()
        df_distri['Depot'] = df_distri['Depot'].str.strip()
        df_distri = df_distri[(df_distri['Depot'] == myDepot)]

        for myISIN in df_trans['ISIN'].unique():
            df = buildDepotStock(gc, df_trans, df_distri, myDepot, myISIN)

            if df_full is None:
                df_full = df
            else:
                df_full = df_full.join(df, how="outer")

        # add a summary column
        df_full['Value-total']= df_full[list(df_full.filter(regex='Value'))].sum(axis=1)
        df_full['Invested-total']= df_full[list(df_full.filter(regex='Invested'))].sum(axis=1)
        df_full['target-total']= df_full[list(df_full.filter(regex='target'))].sum(axis=1)

        # add percent-columns
        for c in list(df_full.filter(regex='Value')):
            isin = c.split("-")[1]
            df_full[f"perc-{isin}"] = df_full[c] / df_full['Value-total']
            # calculate delta to target
            df_full[f"delta-{isin}"] = (df_full[f"perc-{isin}"] - df_full[f"target-{isin}"])* df_full['Value-total']
    
        # Saving
        # remove rows with nas
        logger.debug(f"Deleting Depot {myDepot}")
        gc.writeJobStatus("Running", statusMessage=f"Deleting Depot {myDepot}")
        
        if (gc.influx_version == 1):
            gc.influxClient.delete_series(tags={'Depot': myDepot})
        else:
            endDateString = datetime.datetime.now().isoformat("T") + "Z"
            del_api = gc.influxClient.delete_api()
            del_api.delete("1900-01-01T00:00:00Z", endDateString, f'Depot="{myDepot}"', bucket=gc.influx_db, org=gc.influx_org)
        
        
        logger.debug(f"Saving Depot {myDepot}")
        gc.writeJobStatus("Running", statusMessage=f"Saving Depot {myDepot}")
        
        saveDepot(gc, df_full, myDepot)
    
        return df_full
    
    except Exception as e:
        logger.exception(f"Crash building depot for depot {myDepot}", exc_info=e)
        gc.numErrors += 1
        gc.errMsg += f"Crash building depot for depot {myDepot}; "


def buildDepotStock(gc, df_trans, df_distri, myDepot, myISIN):
    
    logger = logging.getLogger(__name__)
    
    try:
        logger.debug(f'Building value list for depot {myDepot} and stock {myISIN}')
        gc.writeJobStatus("Running", statusMessage=f'Building value list for depot {myDepot} and stock {myISIN}')

        engines.scaffold.addStock(gc, myISIN)

        # convert date to timestamp
        df_trans['TimeStamp'] = pd.to_datetime(df_trans['Datum'], format='%d.%m.%Y')
        df_trans = df_trans.set_index('TimeStamp')
        df_trans = df_trans.drop('Datum', axis=1)
        
        # Filter for depot and ISIN and cumulate
        df_trans = df_trans[(df_trans['Depot'] == myDepot) & (df_trans['ISIN'] == myISIN)]
        df_trans[f'NumStock-{myISIN}'] = df_trans['Anzahl'].cumsum()
        df_trans[f'Invested-{myISIN}'] = df_trans['Wert'].cumsum()
        
        
        # add target percentage
        
        # convert date to timestamp
        df_distri['TimeStamp'] = pd.to_datetime(df_distri['Datum'], format='%d.%m.%Y')
        df_distri = df_distri.set_index('TimeStamp')
        df_distri = df_distri.drop('Datum', axis=1)
        
        # Filter for depot and ISIN and cumulate
        df_distri = df_distri[(df_distri['Depot'] == myDepot) & (df_distri['ISIN'] == myISIN)]
        
        
        # merge with data
        # get stock data
        startDate = df_trans.index.min()
        startDate = startDate - datetime.timedelta(days=2)

        logger.debug(f"Query-Date for ISIN {myISIN}: {startDate}")
        
        #df_stock = gc.influxClient.query(f'select "close" from "StockValues" where "ISIN" = \'{myISIN}\' AND Time >= \'{startDateString}\' ')
        
        #print(df_stock)
        #sys.exit()

        #df_stock = gc.influxClient.query(f'select close from StockValues where "ISIN" = \'{myISIN}\' AND Time >= \'{startDateString}\' ')['StockValues']
        df_stock = loadStock(gc, myISIN, startDate)
        
        # join cumulated numbers
        df_stock.index = df_stock.index.tz_convert(None)
        # join transactions
        df_full = df_stock.join(df_trans[[f'NumStock-{myISIN}', f'Invested-{myISIN}']], how="outer")
        # join target distributions
        df_full = df_full.join(df_distri[['Ziel']], how="outer")
        
        df_full = df_full.fillna(method='ffill')
        df_full[f'Value-{myISIN}'] = df_full['close'] * df_full[f'NumStock-{myISIN}']
        df_full = df_full.rename(columns={'close': f'close-{myISIN}', 'Ziel': f'target-{myISIN}'})
        
        
        
        return df_full
    
    except Exception as e:
        logger.exception(f"Crash building depot for depot {myDepot}, stock {myISIN}", exc_info=e)
        gc.numErrors += 1
        gc.errMsg += f"Crash building depot for depot {myDepot}, stock {myISIN}; "


def loadStock(gc, myISIN, startDate):
    """loads a stock starting from a specific start date"""
    
    loc = locals()
    logger = logging.getLogger(__name__)
    
    try:
        msg = f"Starting loadStock with {loc}"
        logger.debug(msg)
        gc.writeJobStatus("Running", statusMessage=msg)
        
        startDateString = startDate.isoformat("T") + "Z"
        if (gc.influx_version == 1):
            df_stock = gc.influxClient.query(f'select close from StockValues where "ISIN" = \'{myISIN}\' AND Time >= \'{startDateString}\' ')['StockValues']
            return df_stock
        else:
            qry = f'from(bucket: \"{gc.influx_db}\") \
                        |> range(start: {startDateString})  \
                        |> filter(fn: (r) => \
                            r.ISIN == \"{myISIN}\" and r._field == \"close\")   \
                        |> keep(columns: ["_time","_value"])'
                            
            
            #print(qry)
                            
            res = gc.influx_query_api.query_data_frame(qry)
            res = res.rename(columns={'_value': 'close'})
            res = res.set_index('_time')
            res = res.drop(columns=['result', 'table'])
            
            #print(res)
            return(res)
            
    
    
        gc.writeJobStatus("Running", statusMessage=msg + " - DONE")
        logger.debug(msg + " - DONE")
    except Exception as e:
        logger.exception(f'Crash xxx with {loc}!', exc_info=e)
        gc.numErrors += 1
        gc.errMsg += f"Crash xxx with {loc}; "
    
    
def saveDepot(gc, df_full, myDepot):
    """CHANGE ME"""
    
    loc = locals()
    logger = logging.getLogger(__name__)
    
    try:
        msg = f"Starting saveDepot with {loc}"
        logger.debug(msg)
        gc.writeJobStatus("Running", statusMessage=msg)
        
   
        if (gc.influx_version == 1):
            gc.influxClient.write_points(df_full, "Depots", {'Depot': myDepot}, protocol='line')
        else:
            df_full['Depot'] = myDepot
            
            x = 0
            step = 200
            for df_chunk in gc.chunk(df_full, step):
                logger.debug(f"Saving {x} of {len(df_full.index)}...")
                x += step
                gc.influx_write_api.write(gc.influx_db, gc.influx_org, record=df_chunk, data_frame_measurement_name="Depots", data_frame_tag_columns=['Depot'])
    
    
        gc.writeJobStatus("Running", statusMessage=msg + " - DONE")
        logger.debug(msg + " - DONE")
    except Exception as e:
        logger.exception(f'Crash saveDepot with {loc}!', exc_info=e)
        gc.numErrors += 1
        gc.errMsg += f"Crash saveDepot with {loc}; "
    
    