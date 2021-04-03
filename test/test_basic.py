# -*- coding: utf-8 -*-
"""
Created on Sat Apr  3 13:46:06 2021

@author: Sebastian
"""


import sys
sys.path.append('../src')


import unittest
import common.globalcontainer as glob
from dataobjects.stock import Stock
import engines.scaffold
import pandas as pd


class TestGC(unittest.TestCase):
    
    gc=None
    
    def step001(self):
        """
        Test general GlobalContainer-Functions
        """
        self.gc = glob.GlobalContainer("config-test.cfg", "TestRun")
        
        self.gc.resetMySQLDatabases()
        self.gc.resetInfluxDatabases()
        
        self.assertEqual(self.gc.jobName, "TestRun")
        
        res = self.gc.ses.query(Stock).all()
        
        self.assertIsNotNone(self.gc.influxClient)
        df = self.gc.influxClient.query("select * from StockValues")

        self.assertEqual(len(df),0)
        
    def step002(self):
        """Stock Enrichment"""
        
        # Aktie Volkswagen
        s = Stock("DE0007664039")
        engines.scaffold.enrichStock(self.gc, s)
        
        self.assertEqual("766403", s.WKN)
        self.assertEqual("176173", s.ComdirectId)
        self.assertEqual("Aktie", s.StockType)
        
        
        # Fond Arero
        s = Stock("LU0360863863")
        engines.scaffold.enrichStock(self.gc, s)
        
        self.assertEqual("DWS0R4", s.WKN)
        self.assertEqual("120490287", s.ComdirectId)
        self.assertEqual("Fonds", s.StockType)
        
        
        # ETF
        s = Stock("DE000ETFL284")
        engines.scaffold.enrichStock(self.gc, s)
        
        self.assertEqual("ETFL28", s.WKN)
        self.assertEqual("29495143", s.ComdirectId)
        self.assertEqual("ETF", s.StockType)
        
    def step003(self):
        """Stock Grabbing"""
        engines.grabstocks.createTestStocks(self.gc)
        
        for s in self.gc.ses.query(Stock).all():
            self.gc.writeJobStatus("Running", statusMessage=f'Grabbing Stock {s.Name}')
            engines.grabstocks.grabStock(self.gc, s)
            
            #Load and check if something was loaded
            df_stock = self.gc.influxClient.query(f'select close from StockValues where "ISIN" = \'{s.ISIN}\'')['StockValues']
            l = len(df_stock.index)
            self.assertTrue(l > 10, f"{l} is not enough prices for {s.ISIN}")


    def step004(self):
        """Depot Building"""
        df_trans = pd.read_excel(f"{self.gc.data_root}Transactions.ods", engine = "odf")
        df_trans['ISIN'] = df_trans['ISIN'].str.strip()
        df_trans['Depot'] = df_trans['Depot'].str.strip()
        
        df_distri = pd.read_excel(f"{self.gc.data_root}TargetDistribution.ods", engine = "odf")
        df_distri['ISIN'] = df_distri['ISIN'].str.strip()
        df_distri['Depot'] = df_distri['Depot'].str.strip()

        
        
    def step999(self):
        
        self.assertEqual(self.gc.numErrors, 0)
        
        self.gc.eng.dispose()
        
        
        
    def _steps(self):
        for name in dir(self): # dir() result is implicitly sorted
            if name.startswith("step"):
                yield name, getattr(self, name) 

    def test_steps(self):
        for name, step in self._steps():
            try:
                step()
            except Exception as e:
                print("####################### Disposing Engine #######################")
                self.gc.eng.dispose()
                self.fail("{} failed ({}: {})".format(step, type(e), e))
                
        print("####################### Disposing Engine #######################")
        self.gc.eng.dispose()


if __name__ == '__main__':

    
    unittest.main()

