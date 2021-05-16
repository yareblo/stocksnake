# -*- coding: utf-8 -*-
"""
Created on Sun Mar 14 17:38:17 2021

@author: Sebastian
"""

from sqlalchemy import *
from common.base import Base
import datetime


class LogMessage(Base):
    __tablename__ = "LogMessage"

    Id = Column(Integer, primary_key = True)
    LogTime = Column(DateTime)
    RunId = Column(String(256))
    LogType = Column(String(32))
    LogObject = Column(String(256))
    LogObjectId = Column(String(256))
    LogMsg = Column(String(4096))



    def __init__(self, runId, logType, logObject, logObjectId, message):
        self.LogTime = datetime.datetime.now()
        self.RunId = runId
        self.LogType = logType
        self.LogObject = logObject
        self.LogObjectId = logObjectId
        self.LogMsg = message
