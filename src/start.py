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
import dataobjects.stock


ap = argparse.ArgumentParser()
ap.add_argument("-c", "--config", required=False, help="path to config file")

args = vars(ap.parse_args())

configFile = args["config"]
if (configFile == None):
    configFile = "config.cfg"

gc = glob.GlobalContainer(configFile)

logger = logging.getLogger(__name__)

try:

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
    
    #gc.resetDatabases()
    
    vw = dataobjects.stock.Stock("Volkswagen VZ", "DE0007664039")
    vw.ComdirectId = "176173"
    
    acn = dataobjects.stock.Stock("Accenture", "IE00B4BNMY34")
    acn.ComdirectId = "55081566"
    
    hd = dataobjects.stock.Stock("Heidelberger Druck", "DE0007314007")
    hd.ComdirectId = "164941"
    
    engines.grabstocks.urlTest(gc, acn) # Accenture
    engines.grabstocks.urlTest(gc, vw)  # VW VZ-Aktie
    engines.grabstocks.urlTest(gc, hd)  # Heidelberg Druck
    
    
    
        
    
    l = logging.root.level
    logger.setLevel(logging.DEBUG)
    logger.info('------------------------------ END ------------------------------')
    logger.setLevel(l)
    
except Exception as e:
    logger.exception('Crash!', exc_info=e)
