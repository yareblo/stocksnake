# -*- coding: utf-8 -*-
"""
Created on Sat Mar 13 21:19:33 2021

@author: Sebastian
"""


import configparser as cfp
import os
import logging
import logging.handlers



class GlobalContainer(object):

    
    log_root = "logs"
    log_level = 30
    log_size = 5 * 1024 * 1024
    log_number = 10
    debug_mode = "True"
    
    influx_url = "http://localhost:8086"
    influx_db = "stock"
    
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
            
            config['InfluxDB'] = {'URL': self.influx_url,
                                  'Database': self.influx_db}
        
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
            
            self.influx_url = configParser.get('InfluxDB', 'URL', fallback = self.influx_url)
            self.influx_db = configParser.get('InfluxDB', 'Database', fallback = self.influx_db)
            
        except Exception as e:
            self.logger.exception('Crash!', exc_info=e)
    
    