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

#from common.globalcontainer import GlobalContainer
import engines.grabstocks

import sys

import re
import pandas as pd
import numpy as np

import math
import cexprtk

from urllib import parse

import datetime


def addStock(gc, isin):
    """This function adds a new Stock with the ISIN to the database, enriches it and grabs all values"""
    logger = logging.getLogger(__name__)
    
    try:
        msg = f"Adding stock <{isin}>"
        logger.debug(msg)
        gc.writeJobStatus("Running", statusMessage=msg)
        
        res = gc.ses.query(Stock).filter(Stock.ISIN == isin)
                
        if (res.count() == 0):
            logger.debug(f'ISIN {isin} not found, creating...')
            s = Stock(isin)
            gc.ses.add(s)
            gc.ses.commit()
            enrichStock(gc, s)
            engines.grabstocks.grabStock(gc, s)
        else:
            s = res[0]
   
        gc.writeJobStatus("Running", statusMessage=msg + " - DONE")
        logger.debug(msg + " - DONE")
        
        return s
    
    except Exception as e:
        logger.exception('Crash!', exc_info=e)
        gc.numErrors += 1
        gc.errMsg += "Crash loading stock {isin}; "
    


def loadNotes(gc, path):
    """This will load the Notes-File and enrich it with the current Stock Price"""
    logger = logging.getLogger(__name__)
    
    try:
        msg = f"Updating notes from {path}"
        logger.debug(msg)
        gc.writeJobStatus("Running", statusMessage=msg)
        
        df_notes = pd.read_excel(path, engine = "odf")
        
        df_notes = df_notes.set_index('Id')
        
        df_notes['ISIN'] = df_notes['ISIN'].str.strip()
        df_notes['Message'] = df_notes['Message'].str.strip()
        df_notes['Condition'] = df_notes['Condition'].str.strip()
        
        df_notes['Value'] = None
        df_notes['ValueDate'] = None
        df_notes['CondResult'] = None

        
        with pd.option_context('display.max_rows', None, 'display.max_columns', None): 
            pass
            #print(df_notes)
        
        # enrich with latest close date
        for index, row in df_notes.iterrows():
            isin = row['ISIN'].strip()
            if (len(isin)==0):
                continue
            addStock(gc, isin)
            qry = f'select time, close from StockValues where ISIN = \'{isin}\' order by time desc limit 1'
            res = gc.influxClient.query(qry)
            if len(res) > 0:
                df_notes.at[index, 'ValueDate'] = res['StockValues'].index[0]
                df_notes.at[index, 'Value'] = res['StockValues'].close[0]
                
            # Evaluate condition
            cond = str(row['Condition'])
            if (len(cond) > 3):
                try:
                    logger.debug(f"Evaluating condition {row['Condition']}")
                    
                    #r = cexprtk.evaluate_expression(cond, {"close" : res['StockValues'].close[0], "B" : 5, "C" : 23})
                    r = gc.resolver.solve(cond, None, None, values={"close" : res['StockValues'].close[0]})
                    df_notes.at[index, 'CondResult'] = r
                    
                    pass
                except Exception as e:
                    logger.exception('Crash!', exc_info=e)
                    gc.errMsg += "Crash evaluating condition {row['Condition']}; "
                    
            
        with pd.option_context('display.max_rows', None, 'display.max_columns', None): 
            pass
            print(df_notes)
        
        # Write Notes to Database
        logger.debug("Writing to Database")
        
        df_notes['Value'] = df_notes['Value'].astype(np.float32)
        df_notes['CondResult'] = df_notes['CondResult'].astype(np.float32)
        df_notes['ValueDate'] = pd.to_datetime(df_notes['ValueDate'])
        df_notes['ValueDate'] = df_notes['ValueDate'].dt.tz_localize(None)
        
        print(df_notes.dtypes)
        
        df_notes.to_sql("notes", gc.eng, if_exists="replace")
        
        #sys.exit()
        
        #df_stock = gc.influxClient.query(f'select close from StockValues where "ISIN" = \'{myISIN}\' AND Time >= \'{startDateString}\' ')['StockValues']
        
        gc.writeJobStatus("Running", statusMessage=msg + " - DONE")
        logger.debug(msg + " - DONE")
    except Exception as e:
        logger.exception('Crash!', exc_info=e)
        gc.numErrors += 1
        gc.errMsg += "Crash loading Notes; "



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


def enrichComId(gc, stock, soup, url, mkPlace):
    
    logger = logging.getLogger(__name__)
    
    try:
        gc.writeJobStatus("Running", statusMessage=f'Get ComId for stock {stock.ISIN}')
        
        option_list = soup.find("select", {"id":"marketSelect"})
        if option_list is not None:
            
            if (mkPlace != "*default*"):
            # if mkPlace is not none, we look for the specific place
                options = option_list.findAll("option", {"label": mkPlace})
                
            else:
            # we take the selected available one
                options = option_list.findAll("option", {"selected": 'selected'})
                
            if (len(options) > 0):
                o = options[0]
                stock.ComdirectId = o['value']
                stock.Marketplace = o.text
                logger.debug(f'Getting ComId {stock.ComdirectId} for place {stock.Marketplace} for ISIN {stock.ISIN}')
                
                gc.ses.add(stock)
                gc.ses.commit()
                return 1
            else:
                return 0

                
        
        logger.warn(f"No Marketplace found for {stock.ISIN}")
        
        # Check if there is no marketselect at all, example IE00B6897102
        # Check for a specific location if there is the ISIN correctly located
        res = soup.select_one("span.key-focus__identifier-type:nth-child(2)").next_sibling
        if res is not None:
            res = res.strip()
            if (res == stock.ISIN):
                # we take the redirect and extract the ID_NOTATION

                p = parse.parse_qs(parse.urlsplit(url).query)
                stock.ComdirectId = p['ID_NOTATION'][0]
                stock.Marketplace = "default"
                logger.debug(f'Getting ComId {stock.ComdirectId} for place {stock.Marketplace} for ISIN {stock.ISIN}')
                
                gc.ses.add(stock)
                gc.ses.commit()
                return 1
    
    except Exception as e:
        logger.exception('Crash!', exc_info=e)
    
    return 0


def enrichStock(gc, stock):
    
    logger = logging.getLogger(__name__)
    
    try:
        gc.writeJobStatus("Running", statusMessage=f'Enriching stock {stock.ISIN}')
        
        page = requests.get(f'https://www.comdirect.de/inf/search/all.html?SEARCH_VALUE={stock.ISIN}')
        if (page.status_code != 200):
            logger.error(f'Could not get Searchpage for {stock.ISIN}')
    
        soup = BeautifulSoup(page.content, 'html.parser')
        
        # Get ComdirectId
        found = False
        for mp in [stock.PreferredMarketplace, 'Xetra', 'gettex', 'Tradegate', 'Frankfurt', '*default*']:
            if (enrichComId(gc, stock, soup, page.url, mp) > 0):
                found=True
                break
        
        if (found == False):
            msg = f"No Comdirect-Id found for ISIN {stock.ISIN}"
            logger.error(msg)
            gc.errMsg += (msg + "; ")
            return
        
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
                    # xa0 encodes the flag, \n is the newline and dot separates thousands in numbers
                    aa = re.sub(r"(\xa0)|(\n)|,","",row_item.text)
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
            res = soup.select_one("span.key-focus__identifier-type:nth-child(1)").next_sibling
            if res is not None:
                stock.WKN = res.strip()
            else:    
                dfw = (df.loc[df[0] == "WKN"])
                if (len(dfw.index) >0):
                    stock.WKN = dfw.iloc[0][1].strip()
                
            # Extract Type
            #
                    
                 #   div.col-4:nth-child(8) > div:nth-child(1) > table:nth-child(2) > tbody:nth-child(1) > tr:nth-child(1) > td:nth-child(2) > a:nth-child(1)
                    
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
        
        # Get 
    
    
        #logger.error(f'No Comdirect-Id found for ISIN {stock.ISIN}')
    
    except Exception as e:
        logger.exception('Crash!', exc_info=e)
        gc.numErrors += 1
        gc.errMsg += f'Crash enriching Stock {stock.ISIN}; '


def getETFDistributions_old(gc, stock):
    """CHANGE ME"""
    
    loc = locals()
    logger = logging.getLogger(__name__)
    
    try:
        msg = f"Starting getCountries with {stock.ISIN}"
        logger.debug(msg)
        gc.writeJobStatus("Running", statusMessage=msg)
        
        
        page = requests.get(f'https://www.comdirect.de/inf/etfs/detail/uebersicht.html?ID_NOTATION={stock.ComdirectId}')
        if (page.status_code != 200):
            logger.error(f'Could not get Searchpage for {stock.ISIN}')
    
        soup = BeautifulSoup(page.content, 'html.parser')
        
        ts = datetime.datetime.now()
        
        # Scrape Verteilungen. Folgende Annahme:
        # 1. Tabelle: Branchen
        # 2. Tabelle: Aktien
        # 3. Tabelle: Länder
        # 4. Tabelle: Währungen
        #
        tbl_content = ['Industry', 'Stock', 'County', 'Currency']
        df_data = pd.DataFrame(columns=['Date', 'ISIN', 'Type', 'Name', 'Percent'])
        #
        tbl_count = -1
        tables = soup.find_all("table", {"class": "table--list"})
        for table in tables:
            tbl_count += 1
            body = table.find_all("tr")
            
            all_rows = []
            for row_num in range(len(body)):
                row = []
                for row_item in body[row_num].find_all("td"):
                    # row_item.text removes the tags from the entries
                    # the following regex is to remove \xa0 and \n and comma from row_item.text
                    # xa0 encodes the flag, \n is the newline and comma separates thousands in numbers
                    aa = re.sub("(\xa0)|(\n)|\.","",row_item.text)
                    #append aa to row - note one row entry is being appended
                    row.append(aa)
                # append one row to all_rows
                all_rows.append(row)
        
            # df = pd.DataFrame(data=all_rows,columns=headings)
            df = pd.DataFrame(data=all_rows)

            df = df.rename(columns={1: "Percent", 2: "Name"})
            
            df['Type'] = tbl_content[tbl_count]
            
            df_data = df_data.append(df, ignore_index = True)
                        
        df_data['ISIN'] = stock.ISIN
        df_data['Date'] = ts
        
        df_data['Percent'] = df_data['Percent'].str.replace(',', '.').str.rstrip('%').astype('float') / 100.0
            
        with pd.option_context('display.max_rows', None, 'display.max_columns', None): 
            print(df_data)
            pass

        df_data.to_sql("etf_details", gc.eng, if_exists="append")
    
        gc.writeJobStatus("Running", statusMessage=msg + " - DONE")
        logger.debug(msg + " - DONE")
    except Exception as e:
        logger.exception(f'Crash getCountries with {stock.ISIN}!', exc_info=e)
        gc.numErrors += 1
        gc.errMsg += f"Crash getCountries with {stock.ISIN}; "


def getSubTable(soup, text):
    """ gets the next table after 'text' and returns it as dataframe """
    
    df = pd.DataFrame()
    
    searchtext = re.compile(text,re.IGNORECASE)
    foundtext = soup.find('h3',text=searchtext) # Find the first <p> tag with the search text
    if (foundtext is not None):
        table = foundtext.findNext('table') # Find the first <table> tag that follows it
        body = table.findAll('tr')
        
        all_rows = []
        for row_num in range(len(body)):
            row = []
            for row_item in body[row_num].find_all("td"):
                
                # Check if we have a span, because this has more informatione
                res = row_item.find('span')
                if ((res is not None) and (res.has_attr('title'))):
                    aa = res['title']
                else:
                    # row_item.text removes the tags from the entries
                    # the following regex is to remove \xa0 and \n and comma from row_item.text
                    # xa0 encodes the flag, \n is the newline and comma separates thousands in numbers
                    aa = re.sub("(\xa0)|(\n)|\.|(\r)","",row_item.text)
                    
                #append aa to row - note one row entry is being appended
                row.append(aa)
            # append one row to all_rows
            all_rows.append(row)
    
        # df = pd.DataFrame(data=all_rows,columns=headings)
        df = pd.DataFrame(data=all_rows)
        df = df.rename(columns={1: "Percent", 2: "Name"})
    
    return df
    

def getFondDistributions(gc, stock):
    """Gets the distributions of a Fond by country, Currency, Position, Industry,... from the comdirect page"""
    
    loc = locals()
    logger = logging.getLogger(__name__)
    
    try:
        msg = f"Starting getFondDistributions with {stock.ISIN}"
        logger.debug(msg)
        gc.writeJobStatus("Running", statusMessage=msg)
        
        
        page = requests.get(f'https://www.comdirect.de/inf/etfs/detail/uebersicht.html?ID_NOTATION={stock.ComdirectId}')
        if (page.status_code != 200):
            logger.error(f'Could not get Searchpage for {stock.ISIN}')
    
        soup = BeautifulSoup(page.content, 'html.parser')
        
        df_data = pd.DataFrame(columns=['Date', 'ISIN', 'Type', 'Name', 'Percent'])
        ts = datetime.datetime.now()
        
        # Positions
        df = getSubTable(soup, "Größte Positionen")
        df['Type'] = "Stock" 
        df_data = df_data.append(df, ignore_index = True)
        
        # Countries
        df = getSubTable(soup, "Länder")
        df['Type'] = "Countries" 
        df_data = df_data.append(df, ignore_index = True)
        
        # Currencies
        df = getSubTable(soup, "Währungen")
        df['Type'] = "Currencies" 
        df_data = df_data.append(df, ignore_index = True)
        
        # Countries
        df = getSubTable(soup, "Bestandteile")
        df['Type'] = "Industries" 
        df_data = df_data.append(df, ignore_index = True)
        
        
        df_data['ISIN'] = stock.ISIN
        df_data['Date'] = ts
        
        df_data['Percent'] = df_data['Percent'].str.replace(',', '.').str.rstrip('%').astype('float') / 100.0

        
        with pd.option_context('display.max_rows', None, 'display.max_columns', None): 
            print(df_data)

        df_data.to_sql("fonds_details", gc.eng, if_exists="append")
    
        gc.writeJobStatus("Running", statusMessage=msg + " - DONE")
        logger.debug(msg + " - DONE")
    except Exception as e:
        logger.exception(f'Crash getCountries with {stock.ISIN}!', exc_info=e)
        gc.numErrors += 1
        gc.errMsg += f"Crash getCountries with {stock.ISIN}; "


# configFile = "config.cfg"
# gc = GlobalContainer(configFile)

# s = Stock('LU0360863863')

# getComdirectId(gc,s)
