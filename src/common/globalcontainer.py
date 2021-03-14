# -*- coding: utf-8 -*-
"""
Created on Sat Mar 13 21:19:33 2021

@author: Sebastian
"""


import configparser as cfp
import os
import logging
import logging.handlers
import sys
import common.loghandler as lh

import common.base
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound



class GlobalContainer(object):

    
    log_root = "logs"
    log_level = 30
    log_size = 5 * 1024 * 1024
    log_number = 10
    debug_mode = "True"
    
    influx_url = "http://localhost:8086"
    influx_host = "localhost"
    influx_port = "8086"
    influx_db = "stock"
    
    mysql_host = "localhost"
    mysql_user = "stocksnake_usr"
    mysql_password = "*secret*"
    mysql_db = "stocksnake"
    
    logger = None
    
    log_path = None
    
    def __init__(self, configPath):
        
        try:
            log_screen = True
            
            self.readConfig(configPath)
            self.writeConfig(configPath)
        
            # set up logging
            # Root-Logger
            log_folder = self.log_root
            if not os.path.exists(log_folder):
                os.makedirs(log_folder)
    
            self.log_path = log_folder + "/log.log"
    
            logger = logging.getLogger()
            logger.setLevel(self.log_level)
            
            logger.handlers = []
            formatter_file = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            formatter_screen = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', "%H:%M:%S")        
            
            fh = logging.handlers.RotatingFileHandler(self.log_path, maxBytes = self.log_size, backupCount = self.log_number)
            fh.setFormatter(formatter_file)
                    
            sh = logging.StreamHandler()
            sh.setFormatter(formatter_screen)
            
            logger.addHandler(fh)
            
            if (log_screen):
                logger.addHandler(sh)
                
            self.logger = logging.getLogger(__name__)
            
            self.prepareMySQLDatabase()
            
            # Add Database Handler
            logdb = lh.LogDBHandler(self.ses)
            
            logger.addHandler(logdb)
            
            
        except Exception as e:
            self.logger.exception('Crash! - STOPPING -', exc_info=e)
            sys.exit(99)
    
    
    def writeConfig(self, configPath):
        
        try:
            config = cfp.ConfigParser()
            
            config['Basic'] = {'LogRoot': self.log_root,
                               'LogLevel': self.log_level,
                               'LogSize': self.log_size,
                               'LogNumber': self.log_number,
                               'Debug': self.debug_mode,}
            
            config['InfluxDB'] = {'Host': self.influx_host,
                                  'Port': self.influx_port,
                                  'Database': self.influx_db}
            
            config['MySQL'] = {'Host': self.mysql_host,
                               'User': self.mysql_user,
                               'Password': self.mysql_password,
                               'Database': self.mysql_db}
        
            with open(configPath, 'w') as configfile:
                config.write(configfile)
                
        except Exception as e:
            self.logger.exception('Crash!', exc_info=e)
    
    
    def readConfig(self, configPath):
    
        try:
            configParser = cfp.RawConfigParser()   
            configParser.read(configPath)
            
            self.log_root = configParser.get('Basic', 'LogRoot', fallback = self.log_root)
            self.log_level = int(configParser.get('Basic', 'LogLevel', fallback = self.log_level))
            self.log_size = int(configParser.get('Basic', 'LogSize', fallback = self.log_size))
            self.log_number = int(configParser.get('Basic', 'LogNumber', fallback = self.log_number))
            self.debug_mode = configParser.get('Basic', 'Debug', fallback = self.debug_mode).lower() in ['true', '1', 't', 'y', 'yes']
            
            self.influx_host = configParser.get('InfluxDB', 'Host', fallback = self.influx_host)
            self.influx_port = int(configParser.get('InfluxDB', 'Port', fallback = self.influx_port))
            self.influx_db = configParser.get('InfluxDB', 'Database', fallback = self.influx_db)
            
            self.mysql_host = configParser.get('MySQL', 'Host', fallback = self.mysql_host)
            self.mysql_user = configParser.get('MySQL', 'User', fallback = self.mysql_user)
            self.mysql_password = configParser.get('MySQL', 'Password', fallback = self.mysql_password)
            self.mysql_db = configParser.get('MySQL', 'Database', fallback = self.mysql_db)
            
        except Exception as e:
            self.logger.exception('Crash!', exc_info=e)
    
    
    def prepareMySQLDatabase(self):
        
        try:
            # prepare database
#            self.eng = create_engine(f'mysql+mysqlconnector://pybackup:47!!TorfBat7en@localhost/{db_schema}')
            cmd = (f'mysql+mysqlconnector://{self.mysql_user}:{self.mysql_password}@{self.mysql_host}/{self.mysql_db}')
            self.logger.debug(cmd)
            self.eng = create_engine(cmd)
            Base = declarative_base()
           
            #base.Base.metadata.bind = eng        
            #base.Base.metadata.create_all()  
            common.base.Base.metadata.create_all(self.eng, checkfirst=True)
        
            Session = sessionmaker(bind=self.eng)
            self.ses = Session()    
        except Exception as e:
            self.logger.exception('Crash!', exc_info=e)
            sys.exit(99)
    

    
    