# -*- coding: utf-8 -*-
"""
Created on Sat Mar 21 20:48:22 2021

@author: Sebastian
"""

import sys
import argparse
import common.globalcontainer as glob
import logging
import os
import datetime

import engines.grabstocks
import engines.scaffold
from dataobjects.stock import Stock

import pandas as pd


def buildDepot(df_trans, df_distri, myDepot):
    
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
            df = buildDepotStock(df_trans, df_distri, myDepot, myISIN)
            
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
        gc.influxClient.delete_series(tags={'Depot': myDepot})
        logger.debug(f"Saving Depot {myDepot}")
        gc.writeJobStatus("Running", statusMessage=f"Saving Depot {myDepot}")
        gc.influxClient.write_points(df_full, "Depots", {'Depot': myDepot}, protocol='line')
    
        return df_full
    
    except Exception as e:
        logger.exception(f"Crash building depot for depot {myDepot}", exc_info=e)
        gc.numErrors += 1
        gc.errMsg += f"Crash building depot for depot {myDepot}; "


def buildDepotStock(df_trans, df_distri, myDepot, myISIN):
    
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
        startDateString = startDate.isoformat("T") + "Z"
        logger.debug(f"Query-Date for ISIN {myISIN}: {startDateString}")
        
        #df_stock = gc.influxClient.query(f'select "close" from "StockValues" where "ISIN" = \'{myISIN}\' AND Time >= \'{startDateString}\' ')
        
        #print(df_stock)
        #sys.exit()
        
        df_stock = gc.influxClient.query(f'select close from StockValues where "ISIN" = \'{myISIN}\' AND Time >= \'{startDateString}\' ')['StockValues']
        
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



ap = argparse.ArgumentParser()
ap.add_argument("-c", "--config", required=False, help="path to config file")

args = vars(ap.parse_args())

configFile = args["config"]
if (configFile == None):
    configFile = "config.cfg"

gc = glob.GlobalContainer(configFile, "AnalyzeStocks")
logger = logging.getLogger(__name__)

try:
    gc.writeJobStatus("Running", StartDate=datetime.datetime.now(), statusMessage="Started")
    l = logging.root.level
    logger.setLevel(logging.DEBUG)
    logger.info('------------------------------ START ------------------------------')
    logger.info(f'Running script:    {os.path.abspath(__file__)}')
    logger.info(f'Debug-Mode:        {gc.debug_mode}')
    ts = os.path.getmtime(__file__)
    logger.info(f"Last modified on:  {datetime.datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f'Logfile:           {gc.log_path}')
    logger.info(f"Loglevel:          {l}")
    logger.setLevel(l)
        
    # Build Portfolio
    
    df_trans = pd.read_excel("../data/Transactions.ods", engine = "odf")
    df_trans['ISIN'] = df_trans['ISIN'].str.strip()
    df_trans['Depot'] = df_trans['Depot'].str.strip()
    
    df_distri = pd.read_excel("../data/TargetDistribution.ods", engine = "odf")
    df_distri['ISIN'] = df_distri['ISIN'].str.strip()
    df_distri['Depot'] = df_distri['Depot'].str.strip()

    #print(df_distri)
    
    #buildDepotStock(df_trans, df_distri, "Einzelwerte", "DE000UNSE018")
    #sys.exit()
    
    for myDepot in df_trans['Depot'].unique():
        df_full = buildDepot(df_trans, df_distri, myDepot)
        df_full.to_excel(f"../data/Depot-{myDepot}.xls")
    
    #df = df_full[(df_full.index > "2018-12-20") & (df_full.index < "2018-12-28")]
    
    with pd.option_context('display.max_rows', None, 'display.max_columns', None): 
        pass
        #print(df)
    
    
    #sys.exit()
    
    
    # Process Notes
    #
    engines.scaffold.loadNotes(gc, "../data/Notizen.ods")

    
    # Correlation Matrix
    #
    #
    dfs = []
    
    for s in gc.ses.query(Stock).all():
        df = gc.influxClient.query(f'select close from StockValues where "ISIN" = \'{s.ISIN}\'')['StockValues']
        #print(df)
        logger.info(f'Stock {s.NameShort} has {len(df.index)} Rows')
        r = [s, df]
        dfs.append(r)
    
    
    df_full = dfs[0][1]
    
    #df_full = df_full.join(dfs[1][1], how="outer", rsuffix = "bl")
    
    for d in dfs:
        nc = f'-{d[0].ISIN}'
        d[1].rename(columns={"close": nc})
        df_full = df_full.join(d[1], how="outer", rsuffix = nc)
    
    #print(df_full)
    with pd.option_context('display.max_rows', None, 'display.max_columns', None): 
        df_corr =df_full.corr()
        # print(df_corr)
        #print(df_corr.stack().reset_index().sort_values(0, ascending=False))
    
    
    
    
    if (gc.numErrors == 0):
        gc.writeJobStatus("Completed", EndDate=datetime.datetime.now(), SuccessDate=datetime.datetime.now(), statusMessage="Completed OK")
    else:
        gc.writeJobStatus("ERROR", EndDate=datetime.datetime.now(), statusMessage=gc.errMsg)
        logger.error("")
        logger.error(f'Number of Errors: {gc.numErrors}')
        logger.error(f'ErrorMessage:     {gc.errMsg}')
        logger.error("")
        
    l = logging.root.level
    logger.setLevel(logging.DEBUG)
    logger.info('------------------------------ END ------------------------------')
    logger.setLevel(l)
    
except Exception as e:
    logger.exception('Crash!', exc_info=e)
