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
import datetime

import common.base
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound

from influxdb import DataFrameClient
from influxdb_client import InfluxDBClient, WriteOptions, BucketRetentionRules
from influxdb_client.client.write_api import SYNCHRONOUS, WriteType
from influxdb_client.client.write.retry import WritesRetry

from dataobjects.scriptStatus import ScriptStatus
from engines.resolver import Resolver


class GlobalContainer(object):

    
    log_root = "logs"
    log_level = 30
    log_size = 5 * 1024 * 1024
    log_number = 10
    debug_mode = "True"
    
    influx_url = "http://localhost:8086"
    influx_host = "localhost"
    influx_port = "8086"
    influx_user = ""
    influx_pwd = ""
    influx_token = "*token*"
    influx_db = "stock"
    influx_org = "My-Org"
    influx_version = 2
    influxClient = None
    influx_query_api = None
    influx_write_api = None
    
    mysql_host = "localhost"
    mysql_user = "stocksnake_usr"
    mysql_password = "*secret*"
    mysql_db = "stocksnake"
    ses = None
    
    logger = None
    log_path = None
    
    jobName = "unknown"
    runId = "None"
    
    numErrors = 0
    errMsg = ""
    
    numWarnings = 0
    warnMsg = ""
    
    resolver = None
    
    data_root = "../data/"
    
    def __init__(self, configPath, job):
        
        try:
            log_screen = True
            self.jobName = job
            self.resolver = Resolver(self)
            self.runId = f"{job}-{datetime.datetime.now().strftime('%Y-%m-%d_%H:%M%S')}"
            
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
            
            # Connect to MySQL
            self.connectMySQLDatabase()
            
            # Add Database Handler
            logdb = lh.LogDBHandler(self.ses)
            
            logger.addHandler(logdb)
            
            # Connect to Influx
            self.connectInfluxDatabase()
            
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
                                  'Token': self.influx_token,
                                  'Organization': self.influx_org,
                                  'Version': self.influx_version,
                                  'Database-Bucket': self.influx_db}
            
            config['MySQL'] = {'Host': self.mysql_host,
                               'User': self.mysql_user,
                               'Password': self.mysql_password,
                               'Database': self.mysql_db}
            
            config['Data'] = {'DataRoot': self.data_root,
                               }
        
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
            self.influx_db = configParser.get('InfluxDB', 'Database-Bucket', fallback = self.influx_db)
            self.influx_version = int(configParser.get('InfluxDB', 'Version', fallback = self.influx_version))
            self.influx_token = configParser.get('InfluxDB', 'Token', fallback = self.influx_token)
            self.influx_org = configParser.get('InfluxDB', 'Organization', fallback = self.influx_org)
            
            self.mysql_host = configParser.get('MySQL', 'Host', fallback = self.mysql_host)
            self.mysql_user = configParser.get('MySQL', 'User', fallback = self.mysql_user)
            self.mysql_password = configParser.get('MySQL', 'Password', fallback = self.mysql_password)
            self.mysql_db = configParser.get('MySQL', 'Database', fallback = self.mysql_db)
            
            self.data_root = configParser.get('Data', 'DataRoot', fallback = self.data_root)
            
            
            
            
        except Exception as e:
            self.logger.exception('Crash!', exc_info=e)
    
    
    def connectMySQLDatabase(self):
        
        try:
            # prepare database
#            self.eng = create_engine(f'mysql+mysqlconnector://pybackup:47!!TorfBat7en@localhost/{db_schema}')
            cmd = (f'mysql+mysqlconnector://{self.mysql_user}:{self.mysql_password}@{self.mysql_host}/{self.mysql_db}')
            self.logger.debug(cmd)
            self.eng = create_engine(cmd)
            #Base = declarative_base()
           
            #base.Base.metadata.bind = eng        
            #base.Base.metadata.create_all()  
            common.base.Base.metadata.create_all(self.eng, checkfirst=True)
        
            Session = sessionmaker(bind=self.eng)
            self.ses = Session()    
        except Exception as e:
            self.logger.exception('Crash!', exc_info=e)
            sys.exit(99)
        
        
    def connectInfluxDatabase(self):
        
        try:
            # prepare database
            self.logger.debug(f'Connecting to Influx with: Host:{self.influx_host}, Port: {self.influx_port}, User: {self.influx_user}, DB: {self.influx_db}')
            if (self.influx_version == 1):
                pass
                self.influxClient = DataFrameClient(self.influx_host, self.influx_port, self.influx_user, self.influx_pwd, self.influx_db)
                
            elif (self.influx_version == 2):
                
                
                retries = WritesRetry(total=20, backoff_factor=1, exponential_base=1)
                
                self.influxClient = InfluxDBClient(url=f"http://{self.influx_host}:{self.influx_port}", 
                                                   token=self.influx_token, org=self.influx_org, retries=retries, timeout=180_000)
                
                self.influx_query_api = self.influxClient.query_api()
            
                self.influx_write_api = self.influxClient.write_api(write_options=WriteOptions(batch_size=500, write_type=WriteType.synchronous,
                                                          flush_interval=10_000,
                                                          jitter_interval=2_000,
                                                          retry_interval=30_000,
                                                          max_retries=25,
                                                          max_retry_delay=60_000,
                                                          exponential_base=2)) 
                #self.influx_write_api = self.influxClient.write_api(write_options=SYNCHRONOUS)
                
            
        except Exception as e:
            self.logger.exception('Crash!', exc_info=e)
            sys.exit(99)

    def resetDatabases(self):
        try:
            self.logger.warning("Resetting Databases")
            
            self.resetMySQLDatabases()
            self.resetInfluxDatabases()
            
        except Exception as e:
            self.logger.exception('Crash!', exc_info=e)
    
    def resetMySQLDatabases(self):
        try:
            self.logger.warning("Resetting MySQL-Database")
            
            #Base = declarative_base()
            common.base.Base.metadata.drop_all(self.eng, checkfirst=True)
            common.base.Base.metadata.create_all(self.eng, checkfirst=True)
            
        except Exception as e:
            self.logger.exception('Crash!', exc_info=e)

            
    def resetInfluxDatabases(self):
        try:
            self.logger.warning("Resetting Influx-Database")
            
            if (self.influx_version == 1):
                self.influxClient.drop_database(self.influx_db)
                self.influxClient.create_database(self.influx_db)
            else:
                
                with InfluxDBClient(url=f"http://{self.influx_host}:{self.influx_port}", 
                                                   token=self.influx_token, org=self.influx_org, timeout=180_000) as client:
                
                    buckets_api = client.buckets_api()
                
                    my_bucket = buckets_api.find_bucket_by_name(self.influx_db)
                
                    if (my_bucket is not None):
                        buckets_api.delete_bucket(my_bucket)
                    
                    org_name = self.influx_org
                    org = list(filter(lambda it: it.name == org_name, self.influxClient.organizations_api().find_organizations()))[0]
                    retention_rules = BucketRetentionRules(type="forever", every_seconds=0, shard_group_duration_seconds=60*60*24*90)  #3600*24*365*200
                    created_bucket = buckets_api.create_bucket(bucket_name = self.influx_db, retention_rules=retention_rules, org_id = org.id)
            
        except Exception as e:
            self.logger.exception('Crash!', exc_info=e)
            sys.exit(-99)
            
            
    def writeJobStatus(self, Status, StartDate=None, EndDate=None, statusMessage=None, SuccessDate=None):
        try:
            jobStatus = None
            res = self.ses.query(ScriptStatus).filter(ScriptStatus.Name == self.jobName)
                
            if (res.count() == 0):
                self.logger.debug(f'ScriptStatus {self.jobName} not found, creating...')
                jobStatus = ScriptStatus(self.jobName)

                self.ses.add(jobStatus)
                self.ses.commit()
            else:
                jobStatus = res.first()
            
            jobStatus.StatusDateTime = datetime.datetime.now()
            jobStatus.Status = Status
            
            if SuccessDate is not None:
                jobStatus.LastSuccessDateTime = SuccessDate 
            
            if StartDate is not None:
                jobStatus.StartDateTime = StartDate 
                
            if EndDate is not None:
                jobStatus.EndDateTime = EndDate
                
            if statusMessage is not None:
                jobStatus.StatusMessage = statusMessage
                
            jobStatus.ErrorNumbers = self.numErrors
            jobStatus.ErrorMessage = self.errMsg
            
            jobStatus.WarningNumbers = self.numWarnings
            jobStatus.WarningMessage = self.warnMsg
            
            self.ses.add(jobStatus)
            self.ses.commit()
            
        except Exception as e:
            self.logger.exception('Crash!', exc_info=e)
            
            
    def chunk(self, seq, size):
        return (seq[pos:pos + size] for pos in range(0, len(seq), size))

    
    def writeJobMessage(self, logType, logObject, logObjectId, message):
        try:
            jobMessage = (self.runId, logType, logObject, logObjectId, message)
            
            self.ses.add(jobMessage)
            self.ses.commit()
            
        except Exception as e:
            self.logger.exception('Crash!', exc_info=e)
            
            
            
            
            
    def iQuery(self, qry):
        """Executes the flow query against the innodb"""
    
        loc = locals()
        logger = logging.getLogger(__name__)
        res = None
        
        try:
            msg = f"Starting iQuery with {loc}"
            logger.debug(msg)
            self.writeJobStatus("Running", statusMessage=msg)
            
            with InfluxDBClient(url=f"http://{self.influx_host}:{self.influx_port}", 
                                                   token=self.influx_token, org=self.influx_org, timeout=180_000) as client:
                res = client.query_api().query_data_frame(qry)

            self.writeJobStatus("Running", statusMessage=msg + " - DONE")
            logger.debug(msg + " - DONE")
            
            return res
            
        except Exception as e:
            logger.exception(f'Crash iQuery with {loc}!', exc_info=e)
            self.numErrors += 1
            self.errMsg += f"Crash iQuery with {loc}; "
                
            
            