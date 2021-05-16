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
import engines.analysis
import pandas as pd


class TestGC(unittest.TestCase):
    
    gc=None
    
    def step001(self):
        """
        Test general GlobalContainer-Functions
        """
        
        self.gc.logger.info("####################### Step 01: Initialize #######################")
        self.gc.resetMySQLDatabases()
        self.gc.resetInfluxDatabases()
        
        self.assertEqual(self.gc.jobName, "TestRun")
        
        res = self.gc.ses.query(Stock).all()
        
        self.assertIsNotNone(self.gc.influxClient)
        # df = self.gc.influxClient.query("select * from StockValues")
        q = f"from(bucket: \"{self.gc.influx_db}\") |> range(start: 0) |> count()"
        df = self.gc.influx_query_api.query_data_frame(q)
        # print(df)

        #self.assertEqual(len(df),0, f"Bucket {self.gc.influx_db} not empty")
        
    def step002(self):
        """Stock Enrichment"""
        self.gc.logger.info("####################### Step 02: Stock Enrichment #######################")
        
        # Aktie Volkswagen
        s = Stock("DE0007664039")
        engines.scaffold.enrichStock(self.gc, s)
        
        self.assertEqual("766403", s.WKN)
        self.assertEqual("176173", s.ComdirectId)
        self.assertEqual("Aktie", s.StockType)
        self.assertEqual("Xetra", s.Marketplace)
        
        
        # Fond Arero
        s = Stock("LU0360863863")
        engines.scaffold.enrichStock(self.gc, s)
        
        self.assertEqual("DWS0R4", s.WKN)
        self.assertEqual("120490287", s.ComdirectId)
        self.assertEqual("Fonds", s.StockType)
        self.assertEqual("gettex", s.Marketplace)
        
        
        # ETF
        s = Stock("DE000ETFL284")
        engines.scaffold.enrichStock(self.gc, s)
        
        self.assertEqual("ETFL28", s.WKN)
        self.assertEqual("29495143", s.ComdirectId)
        self.assertEqual("ETF", s.StockType)
        
        # Cash
        s = Stock("CASH")
        engines.scaffold.enrichStock(self.gc, s)
        
        self.assertEqual("Cash", s.WKN, "Check WKN")
        self.assertEqual("-1", s.ComdirectId, "Check ComdirectId")
        self.assertEqual("Cash", s.StockType, "Check Type")
        
        
        # Non-Existing Stock
        s = Stock("DE000A141DW0")
        engines.scaffold.enrichStock(self.gc, s)
        
        self.assertEqual(None, s.WKN)
        self.assertEqual(None, s.ComdirectId)
        self.assertEqual(None, s.StockType)
        
        
        
    def step003(self):
        """Stock Grabbing"""
        self.gc.logger.info("####################### Step 03: Stock Grabbing #######################")
        
        engines.grabstocks.createTestStocks(self.gc)
        
        for s in self.gc.ses.query(Stock).all():
            self.gc.writeJobStatus("Running", statusMessage=f'Grabbing Stock {s.Name}')
            engines.grabstocks.grabStock(self.gc, s)
            
            #Load and check if something was loaded
            if (self.gc.influx_version == 1):
                df_stock = self.gc.influxClient.query(f'select close from StockValues where "ISIN" = \'{s.ISIN}\'')['StockValues']
                l = len(df_stock.index)
            else:
                qry = f'from(bucket: \"{self.gc.influx_db}\") \
                        |> range(start: 1900-01-01T00:00:00.000000000Z)  \
                        |> filter(fn: (r) => \
                            r.ISIN == \"{s.ISIN}\" and r._field == \"close\")'
                df_stock = self.gc.influx_query_api.query_data_frame(qry)
                l = len(df_stock.index)
            
            self.assertTrue(l > 10, f"{l} is not enough prices for {s.ISIN}")


    def step004(self):
        """Depot Building"""
        self.gc.logger.info("####################### Step 04: Depot Building #######################")
        
        df_trans = pd.read_excel(f"{self.gc.data_root}Transactions.ods", engine = "odf")
        df_trans['ISIN'] = df_trans['ISIN'].str.strip()
        df_trans['Depot'] = df_trans['Depot'].str.strip()
        
        df_distri = pd.read_excel(f"{self.gc.data_root}TargetDistribution.ods", engine = "odf")
        df_distri['ISIN'] = df_distri['ISIN'].str.strip()
        df_distri['Depot'] = df_distri['Depot'].str.strip()
        
        for myDepot in df_trans['Depot'].unique():
            df_full = engines.analysis.buildDepot(self.gc, df_trans, df_distri, myDepot)
            df_full.to_excel(f"{self.gc.data_root}Depot-{myDepot}.ods", engine = "odf")

        # Check some Results
        if (self.gc.influx_version == 1):
            qry = f'select * from Depots where Depot = \'Dep_01\' order by time desc limit 1'
            res = self.gc.influxClient.query(qry)['Depots']
        else:
            qry = f'from(bucket: \"{self.gc.influx_db}\") \
                        |> range(start: 1900-01-01T00:00:00.000000000Z)  \
                        |> filter(fn: (r) => \
                            r._measurement == \"Depots\" and \
                            r.Depot == \"Dep_01\")  \
                        |> pivot(rowKey:["_time"], \
                                 columnKey: ["_field"], \
                                 valueColumn: "_value") \
                        |> sort(columns:[\"_time\"], desc: true)'
                            
            #print(qry)
                            
            res = self.gc.influx_query_api.query_data_frame(qry)
            
            res = res.set_index('_time')
            res = res.drop(columns=['result', 'table'])
            
            with pd.option_context('display.max_rows', None, 'display.max_columns', None): 
                #print(res)
                pass
        
        self.assertEqual(3975, res.iloc[0]['Invested-total'], "Total investment in depot Dep_01")
        self.assertEqual(4000, res.iloc[0]['Value-total'], "Total value in depot Dep_01")
        self.assertEqual(400, round(res.iloc[0]['delta-TEST0001'], 4), "Difference TEST0001 to ideal distribution")

        
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
        self.gc.logger.info("####################### STARTING #######################")
        
        try:
            for name, num, step in self._steps():
                if(num > -1):
                    step()
                    
        except Exception as e:
            self.fail("{} failed ({}: {})".format(step, type(e), e))
                
        finally:
            print("####################### Disposing Engine #######################")
            self.gc.ses.commit()
            self.gc.ses.close()
            self.gc.eng.dispose()
            
            self.gc.logger.info("####################### END #######################")


if __name__ == '__main__':

    
    unittest.main()

