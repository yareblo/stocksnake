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
ap.add_argument("-c", "--config", required=False, help="path to config file")

args = vars(ap.parse_args())

configFile = args["config"]
if (configFile == None):
    configFile = "config.cfg"

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
    logger.info(f'Logfile:           {gc.log_path}')
    logger.info(f"Loglevel:          {l}")
    logger.setLevel(l)
    
    #gc.resetMySQLDatabases()
    #gc.resetInfluxDatabases()
    
    engines.scaffold.loadStocks(gc, "../data/ISINS.csv")
    
    for s in gc.ses.query(Stock).all():
        gc.writeJobStatus("Running", statusMessage=f'Enriching Stock {s.Name}')
        cId = engines.scaffold.enrichStock(gc, s)
    
    for s in gc.ses.query(Stock).all():
        gc.writeJobStatus("Running", statusMessage=f'Grabbing Stock {s.Name}')
        engines.grabstocks.grabStock(gc, s)
    
    if (gc.numErrors == 0):
        gc.writeJobStatus("Completed", EndDate=datetime.datetime.now(), statusMessage="Completed OK")
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
