# -*- coding: utf-8 -*-
"""
Created on Fri Apr  2 12:04:47 2021

@author: Sebastian
"""


import logging

def addStock(gc, ISIN):
    """CHANGE ME"""
    logger = logging.getLogger(__name__)
    
    try:
        msg = f"Starting xxx with {ISIN}"
        logger.debug(msg)
        gc.writeJobStatus("Running", statusMessage=msg)
        
   
    
    
        gc.writeJobStatus("Running", statusMessage=msg + " - DONE")
        logger.debug(msg + " - DONE")
    except Exception as e:
        logger.exception('Crash!', exc_info=e)
        gc.numErrors += 1
        gc.errMsg += "Crash loading Notes; "