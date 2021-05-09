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
from pytz import UTC

import os

from influxdb_client import InfluxDBClient, WriteOptions


class TestGC(unittest.TestCase):
    
    gc=None
    
    def step001(self):
        """
        Test general GlobalContainer-Functions
        """
        pass
        #s = engines.scaffold.addStock(self.gc, "IE00B6R52259")
        #s = engines.scaffold.addStock(self.gc, "LU0323577923")
        #engines.scaffold.getFondDistributions(self.gc, s)
        
        try:
            ts = datetime.datetime.strptime('2018-07-09 00:00:00', '%Y-%m-%d %H:%M:%S')
            engines.analysis.loadStock(self.gc, 'LU1681045370', ts)
            sys.exit()
            
            # https://github.com/influxdata/influxdb-client-python#queries
            
            print (self.gc.influxClient)
            
            query_api = self.gc.influxClient.query_api()
            
            write_api = self.gc.influxClient.write_api(write_options=WriteOptions(batch_size=500,
                                                          flush_interval=10_000,
                                                          jitter_interval=2_000,
                                                          retry_interval=5_000,
                                                          max_retries=5,
                                                          max_retry_delay=30_000,
                                                          exponential_base=2)) 
            
            _now = datetime.datetime.now(UTC)
            
            _data_frame = pd.DataFrame(data=[["coyote_creek", random.uniform(1.5, 1.9)], ["coyote_creek", random.uniform(1.5, 1.9)]],
                                       index=[_now, _now - datetime.timedelta(hours = random.uniform(0.5, 0.9))],
                                       columns=["location", "water_level"])
            
            print(_data_frame.to_string())
    
            write_api.write("TestBucket", "SKO", record=_data_frame, data_frame_measurement_name='h2o_feet',
                                data_frame_tag_columns=['location'])
            
            
            write_api.close()
            
            
            data_frame = query_api.query_data_frame('from(bucket: "TestBucket") '
                                                    '|> range(start: -10m) '
                                                    '|> filter(fn: (r) => r["_measurement"] == "cpu") '
                                                    '|> filter(fn: (r) => r["_field"] == "usage_idle") '
                                                    '|> filter(fn: (r) => r["cpu"] == "cpu-total") '
                                                    '|> filter(fn: (r) => r["host"] == "h2934423.stratoserver.net") '
                                                    '|> yield(name: "mean")')
      
            print(data_frame.to_string())
            
        except Exception as e:
            self.logger.exception('Crash!', exc_info=e)
            sys.exit(99)

    def step002(self):
        """
        Text XIRR
        """
        pass
        df_trans = pd.read_excel(os.path.join(self.gc.data_root, "Transactions-XIRR.ods"), engine = "odf")
        df_trans['ISIN'] = df_trans['ISIN'].str.strip()
        df_trans['Depot'] = df_trans['Depot'].str.strip()
    
        df_distri = pd.read_excel(os.path.join(self.gc.data_root, "TargetDistribution.ods"), engine = "odf")
        df_distri['ISIN'] = df_distri['ISIN'].str.strip()
        df_distri['Depot'] = df_distri['Depot'].str.strip()
        
        df_full = engines.analysis.buildDepot(self.gc, df_trans, df_distri, "XIRR")

        
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
                if(num > 1):
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

