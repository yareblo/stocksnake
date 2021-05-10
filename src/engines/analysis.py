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

from pytz import UTC

import sys


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
    
        # Saving
        # remove rows with nas
        logger.debug(f"Deleting Depot {myDepot}")
        gc.writeJobStatus("Running", statusMessage=f"Deleting Depot {myDepot}")
        
        if (gc.influx_version == 1):
            gc.influxClient.delete_series(tags={'Depot': myDepot})
        else:
            endDateString = datetime.datetime.now().isoformat("T") + "Z"
            del_api = gc.influxClient.delete_api()
            del_api.delete("1900-01-01T00:00:00Z", endDateString, f'Depot="{myDepot}"', bucket=gc.influx_db, org=gc.influx_org)
        
        
        logger.debug(f"Saving Depot {myDepot}")
        gc.writeJobStatus("Running", statusMessage=f"Saving Depot {myDepot}")
        
        saveDepot(gc, df_full, myDepot)
        
        calcXIRR(gc, df_full, myDepot, 5*365)
        calcXIRR(gc, df_full, myDepot, 3*365)
        calcXIRR(gc, df_full, myDepot, 365)
        calcXIRR(gc, df_full, myDepot, 180)
        
        logger.debug(f'Building depot {myDepot} - DONE')
    
        return df_full
    
    except Exception as e:
        logger.exception(f"Crash building depot for depot {myDepot}", exc_info=e)
        gc.numErrors += 1
        gc.errMsg += f"Crash building depot for depot {myDepot}; "


def buildDepotStock(gc, df_trans, df_distri, myDepot, myISIN):
    
    logger = logging.getLogger(__name__)
    
    try:
        logger.debug(f'Building value list for depot {myDepot} and stock {myISIN}')
        gc.writeJobStatus("Running", statusMessage=f'Building value list for depot {myDepot} and stock {myISIN}')

        engines.scaffold.addStock(gc, myISIN)
        
        # Filter for depot and ISIN and cumulate
        df_trans = df_trans[(df_trans['Depot'] == myDepot) & (df_trans['ISIN'] == myISIN)]

        # if there is no transaction, we add an empty transaction. Reason: Thete might be an isin in the distribution, but not in the transactions
        if (len(df_trans.index) == 0):
            df_trans = df_trans.append({'Datum': '01.01.2020','Depot': myDepot, 'ISIN': myISIN, 'Anzahl': 0, 'Wert': 0}, ignore_index=True)

        # convert date to timestamp
        df_trans['TimeStamp'] = pd.to_datetime(df_trans['Datum'], format='%d.%m.%Y')
        df_trans = df_trans.set_index('TimeStamp')
        df_trans = df_trans.drop('Datum', axis=1)
        
        # Filter for depot and ISIN and cumulate
        
        df_trans[f'NumStock-{myISIN}'] = df_trans['Anzahl'].cumsum()
        df_trans[f'Invested-{myISIN}'] = df_trans['Wert'].cumsum()
        
        
        # add target percentage
        
        # convert date to timestamp
        df_distri['TimeStamp'] = pd.to_datetime(df_distri['Datum'], format='%d.%m.%Y')
        df_distri = df_distri.set_index('TimeStamp')
        df_distri = df_distri.drop('Datum', axis=1)
        
        # Filter for depot and ISIN and cumulate
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
        
        
        return df_full
    
    except Exception as e:
        logger.exception(f"Crash building depot for depot {myDepot}, stock {myISIN}", exc_info=e)
        gc.numErrors += 1
        gc.errMsg += f"Crash building depot for depot {myDepot}, stock {myISIN}; "


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
                            
            res = gc.influx_query_api.query_data_frame(qry)

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
                gc.influx_write_api.write(gc.influx_db, gc.influx_org, record=df_chunk, data_frame_measurement_name="Depots", data_frame_tag_columns=['Depot'])
                gc.influx_write_api.close()
    
    
        gc.writeJobStatus("Running", statusMessage=msg + " - DONE")
        logger.debug(msg + " - DONE")
    except Exception as e:
        logger.exception(f'Crash saveDepot with {loc}!', exc_info=e)
        gc.numErrors += 1
        gc.errMsg += f"Crash saveDepot with {loc}; "
    
    
def calcXIRR(gc, df_full, myDepot, days):
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
        df = df[['Value-total', 'Invested-total']]
        
        l_xirr = {}
        
        startVal=0
        if (len(df.index) > 0):
            startDate = df.index[0]
            startVal = df.iloc[0]['Value-total']
        
        l_xirr[startDate] = startVal
        
        # get transactions
        df_diff = df.diff().dropna()
        df_diff = df_diff[df_diff['Invested-total'] != 0]

        for i, row in df_diff.iterrows():
            l_xirr[i] = row['Invested-total']
        
        # get value at end
        l_xirr[df.index[-1]] = -df.iloc[-1]['Value-total']
        
        # calculate
        #print(l_xirr)
        x = xirr.xirr(l_xirr)
        
        logger.debug(f"XIRR of depot {myDepot}: {x}")
   
        # Save
        now=pd.Timestamp(datetime.datetime.now(UTC)).replace(hour=0, minute=0, second=0, microsecond=0)
        
        df_res = pd.DataFrame(index=[now], columns=["KPI-Value", "KPI", "Depot"], data=[[x, f"XIRR-{days}", myDepot]])
        #print(df_res)
        if ((x != float("inf")) and (x != float("-inf"))):
            gc.influx_write_api.write(gc.influx_db, gc.influx_org, record=df_res, data_frame_measurement_name="KPIs", data_frame_tag_columns=['Depot', 'KPI'])
            gc.influx_write_api.close()
    
        gc.writeJobStatus("Running", statusMessage=msg + " - DONE")
        logger.debug(msg + " - DONE")
    except Exception as e:
        logger.exception(f'Crash calcXIRR with {loc}!', exc_info=e)
        gc.numErrors += 1
        gc.errMsg += f"Crash calcXIRR with {loc}; "