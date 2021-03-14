# -*- coding: utf-8 -*-
"""
Created on Sun Mar 14 17:38:17 2021

@author: Sebastian
"""

from sqlalchemy import *
from common.base import Base
import datetime


class LogEntry(Base):
    __tablename__ = "Log"

    Id = Column(Integer, primary_key = True)
    
    LogLevel = Column(Integer)
    LogLevelName = Column(String(32))
    LogMsg = Column(String(4096))
    LogTime = Column(DateTime)
    LogUser = Column(String(256))


    def __init__(self, level, levelName, message, user):
        self.LogLevel = level
        self.LogLevelName = levelName
        self.LogMsg = message
        self.LogUser = user
        self.LogTime = datetime.datetime.now()
