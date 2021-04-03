# -*- coding: utf-8 -*-
"""
Created on Fri Apr  2 16:38:07 2021

@author: Sebastian
"""

import logging
import cexprtk


class Resolver(object):

    
    gc = None
    
    
    def __init__(self, globalContainer):        
        self.gc = globalContainer
        
        
        
        
    def solve(self, expr, isin, date, values=None):
        """This function will solve the Expression expr with values values"""
        logger = logging.getLogger(__name__)
        ret = None
        
        try:
            msg = f"Starting solve with {expr}, {isin}, {date}, {values}"
            logger.debug(msg)
            self.gc.writeJobStatus("Running", statusMessage=msg)
            
            if (values is not None):
                ret = cexprtk.evaluate_expression(expr, values)
        
        
            self.gc.writeJobStatus("Running", statusMessage=msg + " - DONE")
            logger.debug(msg + " - DONE")
        except Exception as e:
            logger.exception('Crash!', exc_info=e)
            self.gc.numErrors += 1
            self.gc.errMsg += "Crash loading Notes; "
        
        return ret