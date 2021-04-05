# -*- coding: utf-8 -*-
"""
Created on Sat Apr  3 13:46:06 2021

@author: Sebastian
"""


import sys
sys.path.append('..\\src')


import unittest
import common.globalcontainer as glob
from dataobjects.stock import Stock
import engines.scaffold
import engines.analysis
import pandas as pd
import datetime
import logging
import random


class TestGC(unittest.TestCase):
    
    gc=None
    
    def step001(self):
        """
        Test general GlobalContainer-Functions
        """
       
        self.gc.resetMySQLDatabases()
        self.gc.resetInfluxDatabases()
        
        self.assertEqual(self.gc.jobName, "TestRun")
        
        res = self.gc.ses.query(Stock).all()
        
        self.assertIsNotNone(self.gc.influxClient)
        df = self.gc.influxClient.query("select * from StockValues")

        self.assertEqual(len(df),0)
        
    def step002(self):
        """Create Test Tata"""
        engines.grabstocks.createTestStocks(self.gc)
        
       
    def step003(self):
        """Create Test XIRR of Cashflows"""
        
        # date
        # isin
        # numstocks
        
        r = engines.resolver.Resolver(self.gc)
        
        idx = [datetime.date(2010, 1, 1), datetime.date(2017, 1, 1)]
        d11 = {'isin': ['TEST0011', 'TEST0011'], 'numstocks': [10, -10]}
        df_trans_11 = pd.DataFrame(d11, index=idx)
        z = r.xirrCashflow(self.gc, df_trans_11)
        self.assertEqual(0.05, round(z,3), "xirr of TEST0011 not correct")

        d15 = {'isin': ['TEST0015', 'TEST0015'], 'numstocks': [10, -10]}
        df_trans_15 = pd.DataFrame(d15, index=idx)
        z = r.xirrCashflow(self.gc, df_trans_15)
        self.assertEqual(-0.1, round(z,3), "xirr of TEST0015 not correct")

        idx = [datetime.date(2010, 1, 1), datetime.date(2010, 1, 1), datetime.date(2017, 1, 1), datetime.date(2017, 1, 1)]
        dmix = {'isin': ['TEST0015', 'TEST0011', 'TEST0015', 'TEST0011'], 'numstocks': [10, 10, -10, -10]}
        df_trans_mix = pd.DataFrame(dmix, index=idx)
        z = r.xirrCashflow(self.gc, df_trans_mix)
        self.assertEqual(0.032, round(z,3), "xirr of Mix not correct")
  

    def step004(self):
        """Monte-Carlo of MSCI"""
        logger = logging.getLogger(__name__)
        
        r = engines.resolver.Resolver(self.gc)
            
        isin = "DAX"
        s = self.gc.ses.query(Stock).filter(Stock.ISIN == isin)[0]
        engines.grabstocks.grabStock(self.gc, s)
        
        engines.scaffold.addStock(self.gc, isin)
        
        start_date = engines.grabstocks.getFirstDate(self.gc, isin)
        end_date = engines.grabstocks.getLastDate(self.gc, isin)

        logger.info(f"Range for {isin}: {start_date} to {end_date}")

        l = []
        
        for i in range(2000):
            logger.info(f"Loop {i}")
            
            tf = random.randint(5 * 365, 10 * 365) # interval
            #end_date_rng = end_date - datetime.timedelta(days=tf) # latest end date
            
            off = random.randint(0, ((end_date-start_date).days - tf))
            
            random_start_date = start_date + datetime.timedelta(days = off)
            random_end_date = random_start_date + datetime.timedelta(days=tf)

            idx = [random_start_date, random_end_date]
            d = {'isin': [isin, isin], 'numstocks': [10, -10]}
            df_trans = pd.DataFrame(d, index=idx)
            z = r.xirrCashflow(self.gc, df_trans)
            l.append(z)
            logger.info(f"Start {random_start_date}, End: {random_end_date}, xirr: {z}")
            
        print(l)
        
        logger.info(f"Range for {isin}: {start_date} to {end_date}")
        
        import matplotlib.pyplot as plt
        import numpy as np
  #      %matplotlib inline
        
 #       np.random.seed(42)
 #       x = np.random.normal(size=1000)
        
        plt.hist(l, density=True, bins=100)  # density=False would make counts
        plt.ylabel('Probability')
        plt.xlabel('Data');




        
    def step999(self):
        
        self.assertEqual(self.gc.numErrors, 0, f"Error-Message: {self.gc.errMsg}")
        
        self.gc.eng.dispose()
        
        
        
    def _steps(self):
        for name in dir(self): # dir() result is implicitly sorted
            if name.startswith("step"):
                num = int(name[4:])
                yield name, num, getattr(self, name) 

    def test_steps(self):
        
        self.gc = glob.GlobalContainer("config-test.cfg", "TestRun")
        
        try:
            for name, num, step in self._steps():
                if(num > 3):
                    step()
                    
        except Exception as e:

            self.fail("{} failed ({}: {})".format(step, type(e), e))
        finally:
            print("####################### Disposing Engine #######################")
            self.gc.ses.commit()
            self.gc.ses.close()
            self.gc.eng.dispose()
                



if __name__ == '__main__':

    
    unittest.main()

