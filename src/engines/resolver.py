# -*- coding: utf-8 -*-
"""
Created on Fri Apr  2 16:38:07 2021

@author: Sebastian
"""

import logging
import cexprtk
import xirr
import sys
import engines.scaffold



class Resolver(object):

    
    gc = None
    
    
    def __init__(self, globalContainer):        
        self.gc = globalContainer
        
        
        
        
    def solve(self, expr, isin, date, values=None):
        """This function will solve the Expression expr with values values"""
        logger = logging.getLogger(__name__)
        ret = None
        
        try:
            msg = f"Starting solve {expr} with {isin}, {date}, {values}"
            logger.debug(msg)
            self.gc.writeJobStatus("Running", statusMessage=msg)
            
            if (values is not None):
                ret = cexprtk.evaluate_expression(expr, values)
        
        
            self.gc.writeJobStatus("Running", statusMessage=msg + " - DONE")
            logger.debug(msg + " - DONE")
        except Exception as e:
            logger.exception('Crash!', exc_info=e)
            self.gc.numErrors += 1
            self.gc.errMsg += "Crash solving {expr} with {isin}, {date}, {values}; "
        
        return ret
    
    
    def getStockVal(self, date, isin, val_type, level=1):
        """Gets the value of stock isis close to date:
            val_type_ open, high, low, close, volume
            """
        loc = locals()
        logger = logging.getLogger(__name__)
        
        try:
            #msg = f"Starting getStockVal with {loc}"
            #logger.debug(msg)
            #self.gc.writeJobStatus("Running", statusMessage=msg)
            date = date.tz_localize(None)

            qry = f'select time, {val_type} from StockValues where ISIN = \'{isin}\' and time >= \'{date}\' order by time asc limit 1' 
            res = self.gc.influxClient.query(qry)
            if len(res) > 0:
                return res['StockValues'][f'{val_type}'][0]

            # Reason 1: Isin was not loaded. if we are in first level, see if there are any values, and if not, just cteare stock and load
            if (level==1):
                engines.scaffold.addStock(self.gc, isin)
                return self.getStockVal(date, isin, val_type, level=level+1)

            msg = f"No value {val_type} found for {isin} at date {date}"
            self.gc.numWarnings += 1
            self.gc.warnMsg += msg + "; "
        
            return -1
        
            #self.gc.writeJobStatus("Running", statusMessage=msg + " - DONE")
            #logger.debug(msg + " - DONE")
        except Exception as e:
            logger.exception(f'Crash getStockVal with {loc}!', exc_info=e)
            self.gc.numErrors += 1
            self.gc.errMsg += f"Crash getStockVal with {loc}; "
                
    
    
    def xirrCashflow(self, df_, df_trans):
        """This function will calculate the xirr of a cashflow with the following transactions:
            Dataframe:
            date (index)
            isin
            numstocks
            """
        logger = logging.getLogger(__name__)
        ret = None
        
        try:
            msg = f"Starting calculate xirr of cashflow"
            logger.debug(msg)
            self.gc.writeJobStatus("Running", statusMessage=msg)
                    
            # convert transactions to tupel-list
            l = {}
            # loop through transactions and get stock value 
            for i, row in df_trans.iterrows():
                # get stock value for isin at time x
                sv = self.getStockVal(i, row['isin'], 'close')
                v = sv * row['numstocks']
                if i in l:
                    l[i] = l[i] + v
                else:
                    l[i] = v
        
            return xirr.xirr(l)
        
            self.gc.writeJobStatus("Running", statusMessage=msg + " - DONE")
            logger.debug(msg + " - DONE")
        except Exception as e:
            logger.exception('Crash!', exc_info=e)
            self.gc.numErrors += 1
            self.gc.errMsg += "Crash xirrCashflow; "
        
        return ret
    