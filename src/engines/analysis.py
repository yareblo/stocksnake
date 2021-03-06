# -*- coding: utf-8 -*-
"""
Created on Sat Apr  3 20:11:12 2021

@author: Sebastian
"""

import logging
import engines.scaffold
import pandas as pd
import datetime
import xirr
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS

from sqlalchemy import and_

from dataobjects.depotPosition import DepotPosition
from dataobjects.stock import Stock

from pytz import UTC

import sys, os, math


def buildDepot(gc, df_trans, df_distri, myDepot):

    logger = logging.getLogger(__name__)

    try:
        logger.debug(f'Building depot {myDepot}')
        gc.writeJobStatus("Running", statusMessage=f'Building depot {myDepot}')
        df_full = None

        df_trans['ISIN'] = df_trans['ISIN'].str.strip()
        df_trans['Depot'] = df_trans['Depot'].str.strip()
        df_trans = df_trans[(df_trans['Depot'] == myDepot)]

        df_distri['ISIN'] = df_distri['ISIN'].str.strip()
        df_distri['Depot'] = df_distri['Depot'].str.strip()
        df_distri = df_distri[(df_distri['Depot'] == myDepot)]

        df_isins1 = pd.DataFrame()
        df_isins2 = pd.DataFrame()
        df_isins1['ISIN'] = df_trans['ISIN']
        df_isins2['ISIN'] = df_distri['ISIN']
        
        df_isins1 = df_isins1.append(df_isins2)

        for myISIN in df_isins1['ISIN'].unique():
            df = buildDepotStock(gc, df_trans, df_distri, myDepot, myISIN)

            if df_full is None:
                df_full = df
            else:
                df_full = df_full.join(df, how="outer")

        # fill nans
        df_full = df_full.fillna(method='ffill', limit=2)

        # add a summary column
        df_full['Value-total']= df_full[list(df_full.filter(regex='Value'))].sum(axis=1)
        df_full['Invested-total']= df_full[list(df_full.filter(regex='Invested'))].sum(axis=1)
        df_full['target-total']= df_full[list(df_full.filter(regex='target'))].sum(axis=1)

        # add percent-columns
        for c in list(df_full.filter(regex='Value')):
            isin = c.split("-")[1]
            df_full[f"perc-{isin}"] = df_full[c] / df_full['Value-total']
            # calculate delta to target
            df_full[f"delta-{isin}"] = (df_full[f"perc-{isin}"] - df_full[f"target-{isin}"])* df_full['Value-total']
    
         #Problem: There are sometimes rows where not all stocks have a value. Those rows should be removed. 
         #   But not stupidly blind, because there might be a ramp-up
    
        ### Fill Depot Position
        totRow = {'ISIN':'total'}
        df_isinstot = df_isins1.append(totRow, ignore_index=True)

        for myISIN in df_isinstot['ISIN'].unique():
            
            res = gc.ses.query(DepotPosition).filter(and_(DepotPosition.Depot == myDepot, DepotPosition.ISIN == myISIN))
            if (res.count() == 0):
                pos = DepotPosition(myDepot, myISIN)
                gc.ses.add(pos)
                gc.ses.commit()
            else:
                pos = res.first()
    
            res = gc.ses.query(Stock).filter(Stock.ISIN == myISIN)
            if (res.count() > 0):
                pos.Name = res[0].Name
    
            if (myISIN != 'total'):
                pos.NumStock = float(df_full[f'NumStock-{myISIN}'].iloc[-1])
                pos.CurPrice = float(df_full[f'close-{myISIN}'].iloc[-1])

                if (df_full[f'NumStock-{myISIN}'].iloc[-1] != 0):
                    pos.BuyPrice = float(df_full[f'Invested-{myISIN}'].iloc[-1] / df_full[f'NumStock-{myISIN}'].iloc[-1])
                else:
                    pos.BuyPrice = 0
            else:
                pos.Name = "Total"
                
            pos.CurValue = float(df_full[f'Value-{myISIN}'].iloc[-1])
                
            pos.BuyValue = float(df_full[f'Invested-{myISIN}'].iloc[-1])
            
            # pos.Percentage = Column(Float)
            pos.Perc = float(df_full[f"perc-{myISIN}"].iloc[-1])
            pos.PercTarget = float(df_full[f"target-{myISIN}"].iloc[-1])
            
            if (math.isnan(pos.PercTarget)):
                pos.PercTarget=0
            
            pos.PercDiff = float(pos.Perc - pos.PercTarget)
            pos.CurValTarget = float(df_full['Value-total'].iloc[-1] * pos.PercTarget)
            
            pos.CurValDiff = float((pos.Perc - pos.PercTarget)* df_full['Value-total'].iloc[-1])
            
            
            if (len(df_full.index)>1):
                if (df_full[f'Value-{myISIN}'].iloc[-2] != 0):
                    pos.GainDayPerc = float((df_full[f'Value-{myISIN}'].iloc[-1] / df_full[f'Value-{myISIN}'].iloc[-2]) -1)
                else:
                    pos.GainDayPerc = 0
                pos.GainDayAbs = float(df_full[f'Value-{myISIN}'].iloc[-1] - df_full[f'Value-{myISIN}'].iloc[-2]) 
            else:
                pos.GainDayPerc = 0
                pos.GainDayAbs = 0
                
            pos.GainAllPerc = float((df_full[f'Value-{myISIN}'].iloc[-1] / df_full[f'Invested-{myISIN}'].iloc[-1]) -1)
            pos.GainAllAbs = float(df_full[f'Value-{myISIN}'].iloc[-1] - df_full[f'Invested-{myISIN}'].iloc[-1])
            pos.XIRR90 = float(calcXIRR(gc, df_full, myDepot, 90, valColumn = f'Value-{myISIN}', investColumn = f'Invested-{myISIN}', save=False))
            pos.XIRR180 = float(calcXIRR(gc, df_full, myDepot, 180, valColumn = f'Value-{myISIN}', investColumn = f'Invested-{myISIN}', save=False))
            pos.XIRR_1Y = float(calcXIRR(gc, df_full, myDepot, 365, valColumn = f'Value-{myISIN}', investColumn = f'Invested-{myISIN}', save=False))
            pos.XIRR_3Y = float(calcXIRR(gc, df_full, myDepot, 3*365, valColumn = f'Value-{myISIN}', investColumn = f'Invested-{myISIN}', save=False))
            pos.XIRR_5Y = float(calcXIRR(gc, df_full, myDepot, 5*365, valColumn = f'Value-{myISIN}', investColumn = f'Invested-{myISIN}', save=False))
            
            if (myISIN != 'total'):
                df_trans_isin = df_trans[(df_trans['Depot'] == myDepot) & (df_trans['ISIN'] == myISIN)]
            else:
                df_trans_isin = df_trans
            
            pos.NumTransactions = len(df_trans_isin.index)
            
            pos.DateLastTransaction = df_trans_isin['Datum'].iloc[-1].to_pydatetime()
            #print (pos.DateLastTransaction)
            #print (type(pos.DateLastTransaction))
            #sys.exit()
            
            df_trans_isin['time-diff'] = df_trans_isin['Datum'].diff()
            #print(df_trans_isin)
            #print(df_trans_isin['time-diff'].mean())
            pos.AvgDiffTransactions = float(df_trans_isin['time-diff'].mean() / datetime.timedelta(days=1))  # Average Days between transactions
            if (math.isnan(pos.AvgDiffTransactions)):
                pos.AvgDiffTransactions = 0
            
            pos.DateLastUpdate = datetime.datetime.now()
            
            gc.ses.add(pos)
            gc.ses.commit()
            
            
    
        ### Saving
        # remove rows with nas
        logger.debug(f"Deleting Depot {myDepot}")
        gc.writeJobStatus("Running", statusMessage=f"Deleting Depot {myDepot}")
        
        if (gc.influx_version == 1):
            gc.influxClient.delete_series(tags={'Depot': myDepot})
        else:
            endDateString = datetime.datetime.now().isoformat("T") + "Z"
            del_api = gc.influxClient.delete_api()
            del_api.delete("1900-01-01T00:00:00Z", endDateString, f'Depot="{myDepot}" and _measurement="Depots"', bucket=gc.influx_db, org=gc.influx_org)
        
        
        logger.debug(f"Saving Depot {myDepot}")
        gc.writeJobStatus("Running", statusMessage=f"Saving Depot {myDepot}")
        
        saveDepot(gc, df_full, myDepot)
        
        calcXIRR(gc, df_full, myDepot, 5*365)
        calcXIRR(gc, df_full, myDepot, 3*365)
        calcXIRR(gc, df_full, myDepot, 365)
        calcXIRR(gc, df_full, myDepot, 180)
        calcXIRR(gc, df_full, myDepot, 90)
        
        logger.debug(f'Building depot {myDepot} - DONE')
    
        return df_full
    
    except Exception as e:
        logger.exception(f"Crash building depot for depot {myDepot}", exc_info=e)
        gc.numErrors += 1
        gc.errMsg += f"Crash building depot for depot {myDepot}; "
        logger.error("################################### STOPPING!! ###############################")
        sys.exit()


def buildDepotStock(gc, df_trans, df_distri, myDepot, myISIN):
    
    logger = logging.getLogger(__name__)
    
    try:
        logger.debug(f'Building value list for depot {myDepot} and stock {myISIN}')
        gc.writeJobStatus("Running", statusMessage=f'Building value list for depot {myDepot} and stock {myISIN}')

        engines.scaffold.addStock(gc, myISIN)
        
        # Filter for depot and ISIN 
        df_trans = df_trans[(df_trans['Depot'] == myDepot) & (df_trans['ISIN'] == myISIN)]

        # if there is no transaction, we add an empty transaction. Reason: Thete might be an isin in the distribution, but not in the transactions
        if (len(df_trans.index) == 0):
            df_trans = df_trans.append({'Datum': '01.01.2020','Depot': myDepot, 'ISIN': myISIN, 'Anzahl': 0, 'Wert': 0}, ignore_index=True)

        # convert date to timestamp
        df_trans['TimeStamp'] = pd.to_datetime(df_trans['Datum'], format='%d.%m.%Y')
        df_trans = df_trans.set_index('TimeStamp')
        df_trans = df_trans.drop('Datum', axis=1)
        
        # Cumulate Anzahl and Wert
        df_trans[f'NumStock-{myISIN}'] = df_trans['Anzahl'].cumsum()
        df_trans[f'Invested-{myISIN}'] = df_trans['Wert'].cumsum()
        
        # add target percentage
        
        # convert date to timestamp
        df_distri['TimeStamp'] = pd.to_datetime(df_distri['Datum'], format='%d.%m.%Y')
        df_distri = df_distri.set_index('TimeStamp')
        df_distri = df_distri.drop('Datum', axis=1)
        
        # Filter for depot and ISIN
        df_distri = df_distri[(df_distri['Depot'] == myDepot) & (df_distri['ISIN'] == myISIN)]
        
        # merge with data
        # get stock data
        startDate = df_trans.index.min()
        startDate = startDate - datetime.timedelta(days=2)

        logger.debug(f"Query-Date for ISIN {myISIN}: {startDate}")
        
        # Load stock values starting from the transaction value
        df_stock = loadStock(gc, myISIN, startDate)
        
        # join cumulated numbers
        df_stock.index = df_stock.index.tz_convert(None)
        # join transactions
        df_full = df_stock.join(df_trans[[f'NumStock-{myISIN}', f'Invested-{myISIN}']], how="outer")
        # join target distributions
        df_full = df_full.join(df_distri[['Ziel']], how="outer")
        
        df_full = df_full.fillna(method='ffill')
        df_full[f'Value-{myISIN}'] = df_full['close'] * df_full[f'NumStock-{myISIN}']
        df_full = df_full.rename(columns={'close': f'close-{myISIN}', 'Ziel': f'target-{myISIN}'})
        
        #Save DF_full to excel for analysis. we have a crash!
        df_full.to_excel(os.path.join(gc.data_root, f"Debug-{myDepot}-{myISIN}.ods"), engine = "odf")
        
        ### Write Depot Position
        # res = gc.ses.query(DepotPosition).filter(and_(DepotPosition.Depot == myDepot, DepotPosition.ISIN == myISIN))
        # if (res.count() == 0):
        #     pos = DepotPosition(myDepot, myISIN)
        #     gc.ses.add(pos)
        #     gc.ses.commit()
        # else:
        #     pos = res.first()

        # res = gc.ses.query(Stock).filter(Stock.ISIN == myISIN)
        # if (res.count() > 0):
        #     pos.Name = res[0].Name

        # pos.NumStock = float(df_full[f'NumStock-{myISIN}'].iloc[-1])
        # pos.CurPrice = float(df_full[f'close-{myISIN}'].iloc[-1])
        # pos.CurValue = float(df_full[f'Value-{myISIN}'].iloc[-1])
        
        # if (df_full[f'NumStock-{myISIN}'].iloc[-1] != 0):
        #     pos.BuyPrice = float(df_full[f'Invested-{myISIN}'].iloc[-1] / df_full[f'NumStock-{myISIN}'].iloc[-1])
        # else:
        #     pos.BuyPrice = 0
            
        # pos.BuyValue = float(df_full[f'Invested-{myISIN}'].iloc[-1])
        
        # # pos.Percentage = Column(Float)
        # if (len(df_full.index)>1):
        #     if (df_full[f'Value-{myISIN}'].iloc[-2] != 0):
        #         pos.GainDayPerc = float((df_full[f'Value-{myISIN}'].iloc[-1] / df_full[f'Value-{myISIN}'].iloc[-2]) -1)
        #     else:
        #         pos.GainDayPerc = 0
        #     pos.GainDayAbs = float(df_full[f'Value-{myISIN}'].iloc[-1] - df_full[f'Value-{myISIN}'].iloc[-2]) 
        # else:
        #     pos.GainDayPerc = 0
        #     pos.GainDayAbs = 0
            
        # pos.GainAllPerc = float((df_full[f'Value-{myISIN}'].iloc[-1] / df_full[f'Invested-{myISIN}'].iloc[-1]) -1)
        # pos.GainAllAbs = float(df_full[f'Value-{myISIN}'].iloc[-1] - df_full[f'Invested-{myISIN}'].iloc[-1])
        # pos.XIRR90 = float(calcXIRR(gc, df_full, myDepot, 90, valColumn = f'Value-{myISIN}', investColumn = f'Invested-{myISIN}', save=False))
        # pos.XIRR180 = float(calcXIRR(gc, df_full, myDepot, 180, valColumn = f'Value-{myISIN}', investColumn = f'Invested-{myISIN}', save=False))
        # pos.XIRR_1Y = float(calcXIRR(gc, df_full, myDepot, 365, valColumn = f'Value-{myISIN}', investColumn = f'Invested-{myISIN}', save=False))
        # pos.XIRR_3Y = float(calcXIRR(gc, df_full, myDepot, 3*365, valColumn = f'Value-{myISIN}', investColumn = f'Invested-{myISIN}', save=False))
        # pos.XIRR_5Y = float(calcXIRR(gc, df_full, myDepot, 5*365, valColumn = f'Value-{myISIN}', investColumn = f'Invested-{myISIN}', save=False))
        # pos.NumTransactions = len(df_trans.index)
        # pos.DateLastTransaction = df_trans['Datum'].iloc[-1]
        # #pos.AvgDiffTransactions = Column(Float)  # Average Days between transactions
        # pos.DateLastUpdate = datetime.datetime.now()
        
        # gc.ses.add(pos)
        # gc.ses.commit()
        
        return df_full
    
    except Exception as e:
        logger.exception(f"Crash building depot for depot {myDepot}, stock {myISIN}", exc_info=e)
        gc.numErrors += 1
        gc.errMsg += f"Crash building depot for depot {myDepot}, stock {myISIN}; "
        logger.error("################################### STOPPING!! ###############################")
        sys.exit()


def createFlatStock(gc, startDate, value):
    rng = pd.bdate_range(startDate, datetime.datetime.now())
    df = pd.DataFrame({'close': value }, index = rng) 
    df.index = df.index.tz_localize('utc')
    print(df)
    
    return df



def loadStock(gc, myISIN, startDate):
    """loads a stock starting from a specific start date"""
    
    loc = locals()
    logger = logging.getLogger(__name__)
    
    try:
        msg = f"Starting loadStock with {loc}"
        logger.debug(msg)
        gc.writeJobStatus("Running", statusMessage=msg)
        
        # if (myISIN.lower() == "cash"):
        #     # we create an euro stock
        #     sys.exit()
        #     return createFlatStock(gc, startDate, 1.0)
            
        
        startDateString = startDate.isoformat("T") + "Z"
        if (gc.influx_version == 1):
            df_stock = gc.influxClient.query(f'select close from StockValues where "ISIN" = \'{myISIN}\' AND Time >= \'{startDateString}\' ')['StockValues']
            return df_stock
        else:
            qry = f'from(bucket: \"{gc.influx_db}\") \
                        |> range(start: {startDateString})  \
                        |> filter(fn: (r) => r["_measurement"] == \"StockValues\") \
                        |> filter(fn: (r) => r["ISIN"] == \"{myISIN}\") \
                        |> filter(fn: (r) => r["_field"] == "close") \
                        |> keep(columns: ["_time","_value"])'
                            
            #print(qry)
                        
            with InfluxDBClient(url=f"http://{gc.influx_host}:{gc.influx_port}", 
                                                   token=gc.influx_token, org=gc.influx_org, timeout=180_000) as client:
                res = client.query_api().query_data_frame(qry)

            if ((res is not None) and (len(res.index) > 0)):
                res = res.rename(columns={'_value': 'close'})
                res = res.set_index('_time')
                res = res.drop(columns=['result', 'table'])
            else:
                res = createFlatStock(gc, startDate, 0.0)
                logger.error(f'No values found for {myISIN}')
            
            #print(res)
            return res
            
    
    
        gc.writeJobStatus("Running", statusMessage=msg + " - DONE")
        logger.debug(msg + " - DONE")
    except Exception as e:
        logger.exception(f'Crash loadStock with {loc}!', exc_info=e)
        gc.numErrors += 1
        gc.errMsg += f"Crash loadStock with {loc}; "
    
    
def saveDepot(gc, df_full, myDepot):
    """CHANGE ME"""
    
    loc = locals()
    logger = logging.getLogger(__name__)
    
    try:
        msg = f"Starting saveDepot with {loc}"
        logger.debug(msg)
        gc.writeJobStatus("Running", statusMessage=msg)
        
   
        if (gc.influx_version == 1):
            gc.influxClient.write_points(df_full, "Depots", {'Depot': myDepot}, protocol='line')
        else:
            df_full['Depot'] = myDepot
            
            x = 0
            step = 500
            for df_chunk in gc.chunk(df_full, step):
                logger.debug(f"Saving {x} of {len(df_full.index)}...")
                x += step
                
                with InfluxDBClient(url=f"http://{gc.influx_host}:{gc.influx_port}", 
                                                   token=gc.influx_token, org=gc.influx_org, timeout=180_000) as client:
                    
                    with client.write_api(write_options=SYNCHRONOUS) as write_api:
                
                        write_api.write(gc.influx_db, gc.influx_org, record=df_chunk, data_frame_measurement_name="Depots", data_frame_tag_columns=['Depot'])
                        write_api.close()
    
    
        gc.writeJobStatus("Running", statusMessage=msg + " - DONE")
        logger.debug(msg + " - DONE")
    except Exception as e:
        logger.exception(f'Crash saveDepot with {loc}!', exc_info=e)
        gc.numErrors += 1
        gc.errMsg += f"Crash saveDepot with {loc}; "
    
    
def calcXIRR(gc, df_full, myDepot, days, valColumn = 'Value-total', investColumn = 'Invested-total', save=True):
    """calculates the XIRR of the depot for the last x days"""
    
    loc = locals()
    logger = logging.getLogger(__name__)
    
    try:
        msg = f"Starting calcXIRR with {loc}"
        logger.debug(msg)
        gc.writeJobStatus("Running", statusMessage=msg)
        
        # get value at start date
        startDate = datetime.datetime.now() - datetime.timedelta(days=days)
        df = df_full.loc[(df_full.index >= startDate)]
        df = df[[valColumn, investColumn]]
        
        l_xirr = {}
        
        startVal=0
        if (len(df.index) > 0):
            startDate = df.index[0]
            startVal = df.iloc[0][valColumn]
        
        l_xirr[startDate] = startVal
        
        # get transactions
        df_diff = df.diff().dropna()
        df_diff = df_diff[df_diff[investColumn] != 0]

        for i, row in df_diff.iterrows():
            l_xirr[i] = row[investColumn]  
        
        # get value at end
        l_xirr[df.index[-1]] = -df.iloc[-1][valColumn]
        
        x = -1
        
        if ((df.index[-1] - startDate).days > 30):
        
            x = xirr.xirr(l_xirr)
            
            logger.debug(f"XIRR of depot {myDepot}: {x}")
       
            # Save
            now=pd.Timestamp(datetime.datetime.now(UTC)).replace(hour=0, minute=0, second=0, microsecond=0)
            
            df_res = pd.DataFrame(index=[now], columns=["KPI-Value", "KPI", "Depot"], data=[[x, f"XIRR-{days}", myDepot]])
            #print(df_res)
            if ((x != float("inf")) and (x != float("-inf")) and (save==True)):
                gc.influx_write_api.write(gc.influx_db, gc.influx_org, record=df_res, data_frame_measurement_name="KPIs", data_frame_tag_columns=['Depot', 'KPI'])
                gc.influx_write_api.close()
        else:
            logger.warn(f"Not calculating xirr due to short timespan of {(df.index[-1] - startDate).days} days")
    
        gc.writeJobStatus("Running", statusMessage=msg + " - DONE")
        logger.debug(msg + " - DONE")
        
        if (x == float("inf")):
            x = 9999
            
        if (x == float("-inf")):
            x = -9999
        
        return x
        
    except Exception as e:
        logger.exception(f'Crash calcXIRR with {loc}!', exc_info=e)
        gc.numErrors += 1
        gc.errMsg += f"Crash calcXIRR with {loc}; "
        
        
        
        
def calcABeckKPI(gc):
    """calculates the XIRR of the depot for the last x days"""
    
    loc = locals()
    logger = logging.getLogger(__name__)
    
    try:
        msg = f"Starting calcABeckKPI with {loc}"
        logger.debug(msg)
        gc.writeJobStatus("Running", statusMessage=msg)
        
        # Diese ISIN scheint zu passen, da gibt's auch eine ISIN
        myISIN = "IE00B0M62Q58"
        engines.scaffold.addStock(gc, myISIN)
        
        # get all time high of MSCI World
        
        qry = f'from(bucket: \"{gc.influx_db}\") \
                |> range(start: 0)  \
                |> filter(fn: (r) => r["_measurement"] == \"StockValues\") \
                |> filter(fn: (r) => r["ISIN"] == \"{myISIN}\") \
                |> filter(fn: (r) => r["_field"] == "close") \
                |> max()'
        
        res = gc.iQuery(qry)
        
        allHigh = -1
        if len(res) > 0:
            allHigh = res.loc[0]['_value']
        
        # with pd.option_context('display.max_rows', None, 'display.max_columns', None): 
        #     print(res)
        
        # get last value
        
        qry = f'from(bucket: \"{gc.influx_db}\") \
                |> range(start: 0)  \
                |> filter(fn: (r) => r["_measurement"] == \"StockValues\") \
                |> filter(fn: (r) => r["ISIN"] == \"{myISIN}\") \
                |> filter(fn: (r) => r["_field"] == "close") \
                |> sort(columns: ["_time"], desc: true) \
                |> first()'
        
        res = gc.iQuery(qry)
        
        lastVal = -1
        if len(res) > 0:
            lastVal = res.loc[0]['_value']
        
        logger.debug(f"AllTimeHigh of MSCI World: {allHigh}")
        logger.debug(f"LastVal of MSCI World: {lastVal}")
        
        perc = -1
        if ((allHigh > 0) and (lastVal > 0)):
            perc = lastVal / allHigh
        
        logger.debug(f"Percent: {perc}")
   
        # Save
        now=pd.Timestamp(datetime.datetime.now(UTC)).replace(hour=0, minute=0, second=0, microsecond=0)
        
        df_res = pd.DataFrame(index=[now], columns=["KPI-Value", "KPI", "Depot"], data=[[perc, f"ABeckKPI", "All"]])
        #print(df_res)
        #if ((x != float("inf")) and (x != float("-inf"))):
        if (perc > 0):
            gc.influx_write_api.write(gc.influx_db, gc.influx_org, record=df_res, data_frame_measurement_name="KPIs", data_frame_tag_columns=['Depot', 'KPI'])
            gc.influx_write_api.close()
    
        gc.writeJobStatus("Running", statusMessage=msg + " - DONE")
        logger.debug(msg + " - DONE")
    except Exception as e:
        logger.exception(f'Crash calcABeckKPI with {loc}!', exc_info=e)
        gc.numErrors += 1
        gc.errMsg += f"Crash calcABeckKPI with {loc}; "
        
        
        
        