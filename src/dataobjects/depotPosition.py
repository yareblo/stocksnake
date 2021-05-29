# -*- coding: utf-8 -*-
"""
Created on Sun Mar 14 17:38:17 2021

@author: Sebastian
"""

from sqlalchemy import *
from common.base import Base
import datetime


class DepotPosition(Base):
    __tablename__ = "DepotPositions"

    Id = Column(Integer, primary_key = True)
    Depot = Column(String(256))
    ISIN = Column(String(32))
    Name = Column(String(256))
    NumStock = Column(Float)
    CurPrice = Column(Float)
    CurValue = Column(Float)
    BuyPrice = Column(Float)
    BuyValue = Column(Float)
    
    Perc = Column(Float)
    PercTarget = Column(Float)
    PercDiff = Column(Float)
    CurValTarget = Column(Float)
    CurValDiff = Column(Float)
    
    GainDayPerc = Column(Float)
    GainDayAbs = Column(Float)
    GainAllPerc = Column(Float)
    GainAllAbs = Column(Float)
    XIRR90 = Column(Float)
    XIRR180 = Column(Float)
    XIRR_1Y = Column(Float)
    XIRR_3Y = Column(Float)
    XIRR_5Y = Column(Float)
    NumTransactions = Column(Float)
    DateLastTransaction = Column(DateTime)
    AvgDiffTransactions = Column(Float)  # Average Days between transactions
    DateLastUpdate = Column(DateTime)
    




    def __init__(self, Depot, ISIN):
        self.Depot = Depot
        self.ISIN = ISIN
