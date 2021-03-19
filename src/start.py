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
    
    engines.grabstocks.urlTest(gc)
    
    
    
    l = logging.root.level
    logger.setLevel(logging.DEBUG)
    logger.info('------------------------------ END ------------------------------')
    logger.setLevel(l)
    
except Exception as e:
    logger.exception('Crash!', exc_info=e)
