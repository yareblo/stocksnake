# -*- coding: utf-8 -*-
"""
Created on Sat Mar 20 21:41:15 2021

@author: Sebastian
"""

from sqlalchemy import *
from common.base import Base
import datetime


class Stock(Base):
    __tablename__ = "Stock"

    Id = Column(Integer, primary_key = True)
    
    Name = Column(String(256))
    NameShort = Column(String(256))
    WKN = Column(String(256))
    ISIN = Column(String(256), index=True)
    
    StockType = Column(String(256))
    StockSubType = Column(String(256))
    Currency = Column(String(256))
    
    Industry = Column(String(256))
    
    PreferredMarketplace = Column(String(256))
    Marketplace = Column(String(256))
    
    ComdirectId = Column(String(256))
    
    def __init__(self, ISIN):
        self.ISIN = ISIN

    # def __init__(self, Name, ISIN):
    #     self.Name = Name
    #     self.ISIN = ISIN
        