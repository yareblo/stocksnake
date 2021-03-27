# -*- coding: utf-8 -*-
"""
Created on Sun Mar 21 13:32:17 2021

@author: Sebastian
"""

from dataobjects.stock import Stock
from csv import reader
import logging
import requests
from bs4 import BeautifulSoup

import os

from common.globalcontainer import GlobalContainer

import sys

import re
import pandas as pd



def loadStocks(gc, path):
    
    logger = logging.getLogger(__name__)
    
    try:
        # open file in read mode
        with open(path, 'r') as read_obj:
            # pass the file object to reader() to get the reader object
            csv_reader = reader(read_obj)
            # Iterate over each row in the csv using reader object
            for row in csv_reader:
                isin = row[0].strip()
                # row variable is a list that represents a row in csv
                res = gc.ses.query(Stock).filter(Stock.ISIN == isin)
                
                if (res.count() == 0):
                    logger.debug(f'ISIN {isin} not found, creating...')
                    s = Stock(isin)
                    gc.ses.add(s)
                    gc.ses.commit()
    
    except Exception as e:
        logger.exception('Crash!', exc_info=e)
        gc.numErrors += 1
        gc.errMsg += "Crash loading Stocks; "


def enrichComId(gc, stock, soup, mkPlace):
    
    logger = logging.getLogger(__name__)
    
    options = soup.find("select", {"id":"marketSelect"}).findAll("option", {"label": mkPlace})
    if (len(options) > 0):
        o = options[0]
        stock.ComdirectId = o['value']
        stock.Marketplace = o.text
        logger.debug(f'Getting ComId {stock.ComdirectId} for place {stock.Marketplace} for ISIN {stock.ISIN}')
        
        gc.ses.add(stock)
        gc.ses.commit()
        return 1
    
    return 0


def enrichStock(gc, stock):
    
    logger = logging.getLogger(__name__)
    
    try:
        page = requests.get(f'https://www.comdirect.de/inf/search/all.html?SEARCH_VALUE={stock.ISIN}')
        if (page.status_code != 200):
            logger.error(f'Could not get Searchpage for {stock.ISIN}')
    
        soup = BeautifulSoup(page.content, 'html.parser')
        
        # Get ComdirectId
        for mp in [stock.PreferredMarketplace, 'Xetra', 'gettex', 'Tradegate', 'Frankfurt']:
            if (enrichComId(gc, stock, soup, mp) > 0):
                break
        
        # Get Name and type
        #
        key = soup.find("meta", {'name':'keywords'})
        arr = key['content'].split(",")
        stock.Name = arr[0].strip()
        stock.Type = arr[1].strip()
        
        # get Short Name
        #
        key = soup.find("div", {'class':'col__content'})
        head = key.find("h1")
        
        sn = os.linesep.join([s for s in head.text.splitlines() if s]).splitlines()[0]
        
        stock.NameShort = sn.strip()

        # Scrape Stammdaten
        #
        tables = soup.find_all("table", {"class": "simple-table"})
        for table in tables:
            body = table.find_all("tr")
            
            all_rows = []
            for row_num in range(len(body)):
                row = []
                for row_item in body[row_num].find_all("td"):
                    # row_item.text removes the tags from the entries
                    # the following regex is to remove \xa0 and \n and comma from row_item.text
                    # xa0 encodes the flag, \n is the newline and comma separates thousands in numbers
                    aa = re.sub("(\xa0)|(\n)|,","",row_item.text)
                    #append aa to row - note one row entry is being appended
                    row.append(aa)
                # append one row to all_rows
                all_rows.append(row)
        
            # df = pd.DataFrame(data=all_rows,columns=headings)
            df = pd.DataFrame(data=all_rows)
            
            # Extract Currency
            #
            dfw = (df.loc[df[0] == "Währung"])
            if (len(dfw.index) >0):
                stock.Currency = dfw.iloc[0][1].strip()

            # Extract WKN
            #
            dfw = (df.loc[df[0] == "WKN"])
            if (len(dfw.index) >0):
                stock.WKN = dfw.iloc[0][1].strip()
                
            # Extract Type
            #
            dfw = (df.loc[df[0] == "Wertpapiertyp"])
            if (len(dfw.index) >0):
                stock.StockType = "Aktie"
                stock.StockSubType = dfw.iloc[0][1].strip()
            dfw = (df.loc[df[0] == "Fondskategorie"])
            if (len(dfw.index) >0):
                stock.StockType = "Fonds"
                stock.StockSubType = dfw.iloc[0][1].strip()
            dfw = (df.loc[df[0] == "Anlagekategorie"])
            if (len(dfw.index) >0):
                stock.StockType = "ETF"
                stock.StockSubType = dfw.iloc[0][1].strip()
      
            # Extract Branche
            #
            dfw = (df.loc[df[0] == "Branche"])
            if (len(dfw.index) >0):
                stock.Industry = dfw.iloc[0][1].strip()
        
        gc.ses.add(stock)
        gc.ses.commit()
        
    
        #logger.error(f'No Comdirect-Id found for ISIN {stock.ISIN}')
    
    except Exception as e:
        logger.exception('Crash!', exc_info=e)
        gc.numErrors += 1
        gc.errMsg += f'Crash enriching Stock {stock.ISIN}; '





# configFile = "config.cfg"
# gc = GlobalContainer(configFile)

# s = Stock('LU0360863863')

# getComdirectId(gc,s)
