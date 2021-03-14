# -*- coding: utf-8 -*-
"""
Created on Sun Mar 14 19:28:04 2021

@author: Sebastian
"""

import logging
import dataobjects.logentry


class LogDBHandler(logging.Handler):
    '''
    Customized logging handler that puts logs to the database.
    pymssql required
    '''
    def __init__(self, ses):
        logging.Handler.__init__(self)
        self.ses = ses
        

    def emit(self, record):
        
        try:
            log = dataobjects.logentry.LogEntry(record.levelno, record.levelname, record.msg, record.name)
            self.ses.add(log)
            self.ses.commit()
        
            print(record)        

        
        except Exception as e:
            logger = logging.getLogger(__name__)
            logger.exception('Crash!', exc_info=e)
