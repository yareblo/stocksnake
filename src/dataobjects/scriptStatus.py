# -*- coding: utf-8 -*-
"""
Created on Sun Mar 21 19:06:34 2021

@author: Sebastian
"""

from sqlalchemy import *
from common.base import Base
import datetime


class ScriptStatus(Base):
    __tablename__ = "ScriptStatus"

    Id = Column(Integer, primary_key = True)
    
    Name = Column(String(256), index=True)  
    StatusDateTime = Column(DateTime)  
    StartDateTime = Column(DateTime)
    EndDateTime = Column(DateTime)  
    
    ProjectedEndofStepDateTime = Column(DateTime)  
    ProjectedEndDateTime = Column(DateTime)  
    LastSuccessDateTime = Column(DateTime)  

    Status = Column(String(256))            # Running, Completed, Error
    StatusMessage = Column(String(2048))  
    
    ErrorNumbers = Column(Integer)
    ErrorMessage = Column(String(4096))  
    
    WarningNumbers = Column(Integer)
    WarningMessage = Column(String(4096))  


    def __init__(self, name):
        self.Name = name
        self.StatusDateTime = datetime.datetime.now()
