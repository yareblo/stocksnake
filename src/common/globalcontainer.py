# -*- coding: utf-8 -*-
"""
Created on Sat Mar 13 21:19:33 2021

@author: Sebastian
"""


import configparser as cfp

class GlobalContainer(object):

    
    log_root = ""
    log_level = ""
    
    influx_url = "http://localhost:8086"
    influx_db = "stock"
    
    
    def __init__(self, configPath):
        
        self.readConfig(configPath)
        self.writeConfig(configPath)
    
    
    
    
    def writeConfig(self, configPath):
        
        config = cfp.ConfigParser()
        
        config['Basic'] = {'LogRoot': self.log_root,
                           'LogLevel': self.log_level,}
        
        config['InfluxDB'] = {'URL': self.influx_url,
                              'Database': self.influx_db}
    
        with open(configPath, 'w') as configfile:
            config.write(configfile)
    
    
    def readConfig(self, configPath):
    
        configParser = cfp.RawConfigParser()   
        configParser.read(configPath)
        
        self.log_root = configParser.get('Basic', 'LogRoot', fallback = self.log_root)
        self.log_level = configParser.get('Basic', 'LogLevel', fallback = self.log_level)
        
        self.influx_url = configParser.get('InfluxDB', 'URL', fallback = self.influx_url)
        self.influx_db = configParser.get('InfluxDB', 'Database', fallback = self.influx_db)
        
        
    
    