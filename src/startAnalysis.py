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
import engines.analysis
from dataobjects.stock import Stock

import pandas as pd



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
    logger.info(f'Config-File:       {configFile}')
    logger.info(f'Data-Root:         {gc.data_root}')
    logger.info(f'Bucket:            {gc.influx_db}')
    logger.info(f'MySQL-Schema:      {gc.mysql_db}')
    logger.info(f'Logfile:           {gc.log_path}')
    logger.info(f"Loglevel:          {l}")
    logger.info(f"Arguments:         {args}")
    logger.setLevel(l)
    gc.writeJobMessage("INFO", "Script", "startAnalysis", "Started")
        
    # Build Portfolio
    
    df_trans = pd.read_excel(os.path.join(gc.data_root, "Transactions.ods"), engine = "odf")
    df_trans['ISIN'] = df_trans['ISIN'].str.strip()
    df_trans['Depot'] = df_trans['Depot'].str.strip()
    
    df_distri = pd.read_excel(os.path.join(gc.data_root, "TargetDistribution.ods"), engine = "odf")
    df_distri['ISIN'] = df_distri['ISIN'].str.strip()
    df_distri['Depot'] = df_distri['Depot'].str.strip()

    #print(df_distri)
    
    #buildDepotStock(df_trans, df_distri, "Einzelwerte", "DE000UNSE018")
    #sys.exit()
    
    for myDepot in df_trans['Depot'].unique():
        df_full = engines.analysis.buildDepot(gc, df_trans, df_distri, myDepot)
        # df_full.to_excel(os.path.join(gc.data_root, f"Depot-{myDepot}.ods"), engine = "odf")
    
    #df = df_full[(df_full.index > "2018-12-20") & (df_full.index < "2018-12-28")]
    
    with pd.option_context('display.max_rows', None, 'display.max_columns', None): 
        pass
        #print(df)
    
    
    #KPI-Berechnungen
    engines.analysis.calcABeckKPI(gc)
    
    
    #Korrelationsmatrix pro Depot
    
    
    #sys.exit()
    
    
    # Process Notes
    #
    #engines.scaffold.loadNotes(gc, "../data/Notizen.ods")

    
    # Correlation Matrix
    #
    #
    # dfs = []
    
    # for s in gc.ses.query(Stock).all():
    #     df = gc.influxClient.query(f'select close from StockValues where "ISIN" = \'{s.ISIN}\'')['StockValues']
    #     #print(df)
    #     logger.info(f'Stock {s.NameShort} has {len(df.index)} Rows')
    #     r = [s, df]
    #     dfs.append(r)
    
    
    # df_full = dfs[0][1]
    
    # #df_full = df_full.join(dfs[1][1], how="outer", rsuffix = "bl")
    
    # for d in dfs:
    #     nc = f'-{d[0].ISIN}'
    #     d[1].rename(columns={"close": nc})
    #     df_full = df_full.join(d[1], how="outer", rsuffix = nc)
    
    # #print(df_full)
    # with pd.option_context('display.max_rows', None, 'display.max_columns', None): 
    #     df_corr =df_full.corr()
    #     # print(df_corr)
    #     #print(df_corr.stack().reset_index().sort_values(0, ascending=False))
    
    
    if (gc.numErrors == 0):
        gc.writeJobStatus("Completed", EndDate=datetime.datetime.now(), SuccessDate=datetime.datetime.now(), statusMessage="Completed OK")
        gc.writeJobMessage("INFO", "Script", "startAnalysis", "Completed OK")
    else:
        gc.writeJobStatus("ERROR", EndDate=datetime.datetime.now(), statusMessage=gc.errMsg)
        logger.error("")
        logger.error(f'Number of Errors: {gc.numErrors}')
        logger.error(f'ErrorMessage:     {gc.errMsg}')
        logger.error("")
        gc.writeJobMessage("ERROR", "Script", "startAnalysis", f"Completed with error: {gc.errMsg}")
        
    l = logging.root.level
    logger.setLevel(logging.DEBUG)
    logger.info('------------------------------ END ------------------------------')
    logger.setLevel(l)
    
except Exception as e:
    logger.exception('Crash!', exc_info=e)
    gc.writeJobStatus("CRASH", EndDate=datetime.datetime.now(), statusMessage=gc.errMsg)
    
