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
        pass
        s = engines.scaffold.addStock(self.gc, "IE00B6R52259")
        
        #s = engines.scaffold.addStock(self.gc, "LU0323577923")
        
        
        engines.scaffold.getFondDistributions(self.gc, s)
        




        
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
                if(num > -3):
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

