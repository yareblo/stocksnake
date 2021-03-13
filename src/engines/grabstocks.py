# -*- coding: utf-8 -*-
"""
Created on Sat Mar 13 23:00:55 2021

@author: Sebastian
"""

import urllib.request
import logging


def urlTest(gc):

    logger = logging.getLogger(__name__)
    
    try:
    
        url = "https://www.comdirect.de/inf/kursdaten/historic.csv?DATETIME_TZ_END_RANGE_FORMATED=13.03.2021&DATETIME_TZ_START_RANGE_FORMATED=23.11.2011&ID_NOTATION=55081566&INTERVALL=16&WITH_EARNINGS=true"
        
        
        for offset in range(0, 999):
            print(offset)
        
            url = f'https://www.comdirect.de/inf/kursdaten/historic.csv?DATETIME_TZ_END_RANGE_FORMATED=13.03.2021&DATETIME_TZ_START_RANGE_FORMATED=23.11.1972&ID_NOTATION=55081566&INTERVALL=16&OFFSET={offset}&WITH_EARNINGS=true'
        
        
            data = urllib.request.urlopen(url)
            for line in data: # files are iterable
                print (line)
    
    
    except Exception as e:
        logger.exception('Crash!', exc_info=e)
