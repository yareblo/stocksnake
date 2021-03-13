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

    logger.info('-----------------------------------------------------------')
    logger.info("Started")
    logger.info("Running script:    %s", os.path.abspath(__file__))
    logger.info("Debug-Mode:        %s", gc.debug_mode)
    ts = os.path.getmtime(__file__)
    logger.info("Last modified on:  %s", datetime.datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S'))
    logger.info("Logfile:           %s", gc.log_path)
    
    
    engines.grabstocks.urlTest(gc)
    
except Exception as e:
    logger.exception('Crash!', exc_info=e)
