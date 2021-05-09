# -*- coding: utf-8 -*-
"""
Created on Sat Mar 13 20:48:22 2021

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


ap = argparse.ArgumentParser()
ap.add_argument("-c", "--config", required=False, help="path to config file", default='config.cfg')
ap.add_argument("-ri", "--resetInflux", required=False, help="reset influx db", default='n')
ap.add_argument("-rm", "--resetMySQL", required=False, help="reset mysql db", default='n')

args = vars(ap.parse_args())

configFile = args["config"]
gc = glob.GlobalContainer(configFile, "LoadStocks")
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
    logger.info(f'Logfile:           {gc.log_path}')
    logger.info(f"Loglevel:          {l}")
    logger.info(f"Arguments:         {args}")
    logger.setLevel(l)
    
    if (args['resetMySQL'].lower() == 'y'):
        gc.resetMySQLDatabases()
        
    if (args['resetInflux'].lower() == 'y'):
        gc.resetInfluxDatabases()
   
    # Fill base-Table with ISINS
    engines.scaffold.loadStocks(gc, os.path.join(gc.data_root, "ISINS.csv"))
    
    # Get Metadata for Stocks
    for s in gc.ses.query(Stock).all():
        gc.writeJobStatus("Running", statusMessage=f'Enriching Stock {s.Name}')
        cId = engines.scaffold.enrichStock(gc, s)
    
    # Get Stock Prices
    for s in gc.ses.query(Stock).all():
        gc.writeJobStatus("Running", statusMessage=f'Grabbing Stock {s.Name}')
        engines.grabstocks.grabStock(gc, s)
    
    
    # Fill/Update Notes Table
    #engines.scaffold.loadNotes(gc, os.path.join(gc.data_root, "Notizen.ods"))
    
    
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
    gc.writeJobStatus("CRASH", EndDate=datetime.datetime.now(), statusMessage=gc.errMsg)
    
