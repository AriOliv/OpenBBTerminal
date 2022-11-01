#%%
import pickle
import pandas as pd
import numpy as np
import datetime as dt
import json

import MetaTrader5 as mt5
from indicators.didi import data_processing, implement_didi_strategy
import talib as ta
import yfinance as yf
import warnings

from utils import allTickers, ASSETS_LIST, WATCH_LIST
import asyncio

from timeit import timeit

warnings.simplefilter("ignore")
warnings.warn("deprecated", DeprecationWarning)



def get_historical_data_1(symbol, start_date='2012-8-10',end_date='2023-02-01',interval='1wk') -> pd.DataFrame:
    if symbol[0] == '^':
        df = yf.Ticker(symbol)
    else:
        df = yf.Ticker(symbol + '.SA')

    df = df.history(start=str(start_date),end=str(end_date),interval=interval)
    
    df[['high','low','close']] = df[['High','Low','Close']]

    try:
        df = df.drop(['Open','High','Low','Close','Dividends','Stock Splits'],axis=1)
    except KeyError:
        return df
        
    return df.dropna()

def get_historical_data(symbol, start_date='2010-1-10',end_date='2023-02-01',interval='1wk') -> pd.DataFrame:
    """
    Obtém as cotações a partir do MetaTrader 5 das empresas dado determinado período.
    """
    if interval == '1wk':
        timeframe=mt5.TIMEFRAME_W1
    elif interval == '1d':
        timeframe=mt5.TIMEFRAME_D1
    else:
        print("Timeframe não aceito.")
        timeframe=mt5.TIMEFRAME_W1

    utc_from = dt.datetime.strptime(start_date, '%Y-%m-%d')
    utc_to = dt.datetime.strptime(end_date, '%Y-%m-%d')
    
    close = mt5.copy_rates_range(symbol, timeframe, utc_from, utc_to)
    close = pd.DataFrame(close)
    try:
        close['time'] = pd.to_datetime(close['time'], unit='s')
    except KeyError:
        return close

    return close.set_index('time')

def TEMA(data: pd.DataFrame, periods: int=9) -> pd.Series:
    m1=data['close'].ewm(span=periods,adjust=True).mean()
    m2=m1.ewm(span=periods,adjust=True).mean()
    m3=m2.ewm(span=periods,adjust=True).mean()

    tema=(3*m1)-(3*m2)+m3

    return tema

def SMMA(data: pd.DataFrame, periods: int=4) -> np.array:
    result = data['close'].ewm(periods-1,adjust=False).mean()
    return np.array(result).reshape(-1,1)

def get_indicators(data: pd.DataFrame) -> pd.DataFrame:
    if data.empty:
        return data
    data = data_processing(data,True)
    data = implement_didi_strategy(data)

    data['SMMA'] = SMMA(data,periods=4)
    data['TEMA'] = TEMA(data,9)
    data['plus_di'] = ta.PLUS_DI(data['high'],data['low'],data['close'],8)
    data['minus_di'] = ta.MINUS_DI(data['high'],data['low'],data['close'],8)
    data['adx'] = ta.ADX(data['high'],data['low'],data['close'],8)
    data['trix'] = ta.TRIX(data['close'],3)
    data['tema'] = data['TEMA']/data['SMMA'] - np.ones(len(data))     
    data['smma'] = data['SMMA']/data['SMMA'] - np.ones(len(data))
    data['upperband'], data['middleband'], data['lowerband'] = ta.BBANDS(data['close'],timeperiod=8, nbdevup=2, nbdevdn=2)
    data['return'] = 100 * (data['close'].pct_change())

    return data

def get_volatility(data: pd.DataFrame) -> float:
    return data['return'].std()


async def buy_func(data: pd.DataFrame,ticker: str,thr: int,delta: int,trend_method :str ='tema',tm: str = '1wk') -> dict:
    """
    
    --- Critérios:
        -- ENTRADA
            1 ----> Confirmação de compra DIDI_IDX
            2 ----> Não estar comprado
            3 ----> DI+ > DI-
            4 ----> ADX > threshold
            5 ----> Alertas de compra no DIDI_IDX <= #alertas
            6 ----> TEMA > 0

        -- SAÍDA
            1 ----> Estar comprado
            2 ----> 
            - TRIX:
                1 ----> 

    Args:
        data (pd.DataFrame): _description_
        ticker (str): _description_
        thr (int): _description_
        delta (int): _description_
        trend_method (str, optional): _description_. Defaults to 'tema'.

    Returns:
        dict: _description_
    """

    out = {"ATIVO": [],"PREÇO ENTRADA": [], "DATA ENTRADA": [], "PREÇO SAIDA": [], "DATA SAIDA": [], "FATOR": [], "BB": 0}
    saida = json.dumps({'symbol': ticker,'status': None, 'date':None,'price': None, "BB": None})
    
    if data.empty:
        return {}
    
    alerta_compra = 0

    in_buy = 0
    cum_fator = 1

    for idx in range(len(data)):
        if not np.isnan(data['alerta_compra'][idx]):
            alerta_compra += 1
        elif not np.isnan(data['alerta_venda'][idx]):
            alerta_compra = 0
        else:
            alerta_compra = 0
        
        # ENTRADA
        if not np.isnan(data['confirma_compra'][idx]) and in_buy != 1: # CONFIRMAÇÃO DIDI
            
            if data['plus_di'][idx] <= data['minus_di'][idx]:
                continue
            if data['adx'][idx] <= thr:
                continue
            if data['tema'][idx] <= 0:
                continue
            if alerta_compra <= delta:
                in_buy = 1
                out['ATIVO'].append(ticker)
                out['DATA ENTRADA'].append(data.index[idx])
                out['PREÇO ENTRADA'].append(round(data['high'][idx],2))

                entrada = data['high'][idx]

        # SAIDA
        if in_buy == 0:
            continue

        if not np.isnan(data['confirma_venda'][idx]):
            in_buy = 0

            out['DATA SAIDA'].append(data.index[idx])
            out['PREÇO SAIDA'].append(round(data['low'][idx],2))

            saida = data['low'][idx]

            fator = saida/entrada

            out['FATOR'].append(fator)

            try:
                cum_fator *= fator
            except:
                pass
            
        if trend_method == 'trix':
            if data['trix'][idx] >= 0:
                continue
            if data['adx'][idx] >= data['adx'][idx-1]:
                continue
            if data['m3'][idx] < data['m8'][idx] and data['m20'][idx] > data['m20'][idx-1]:
                in_buy = 0

                out['DATA SAIDA'].append(data.index[idx])
                out['PREÇO SAIDA'].append(round(data['low'][idx],2))

                saida = data['low'][idx]

                fator = saida/entrada

                out['FATOR'].append(fator)

                try:
                    cum_fator *= fator
                except:
                    pass

        if trend_method == 'tema':
            if data['tema'][idx] > 0:
                continue
            if data['adx'][idx] <= data['adx'][idx-2]:
                continue
            if data['m3'][idx] < data['m8'][idx] and data['m20'][idx] > data['m20'][idx-1]:
                in_buy = 0

                out['DATA SAIDA'].append(data.index[idx])
                out['PREÇO SAIDA'].append(round(data['low'][idx],2))

                saida = data['low'][idx]

                fator = saida/entrada

                out['FATOR'].append(fator)

                try:
                    cum_fator *= fator
                except:
                    pass
    
    if len(out['DATA ENTRADA']) > len(out['DATA SAIDA']):
        stock_return = (data.iloc[-1]/data.iloc[0]).close

        return {'stock': ticker, 'status': 'inBuy','date':data.index[idx],'price': out['PREÇO ENTRADA'][-1], 'timeframe': tm, 'bb': 0}

    return {}

async def buy_func_bbands(data: pd.DataFrame,ticker: str,thr: int,delta: int,trend_method :str ='tema',tm: str = '1wk') -> dict:
    out = {"ATIVO": [],"PREÇO ENTRADA": [], "DATA ENTRADA": [], "PREÇO SAIDA": [], "DATA SAIDA": [], "FATOR": [], "BB": 1}
    saida = json.dumps({'symbol': ticker,'status': None, 'date':None,'price': None, "BB": None, "fator": None})

    if data.empty:
        return {}

    alerta_compra = 0

    in_buy = 0
    cum_fator = 1

    for idx in range(len(data)):
        if not np.isnan(data['alerta_compra'][idx]):
            alerta_compra += 1
        elif not np.isnan(data['alerta_venda'][idx]):
            alerta_compra = 0
        else:
            alerta_compra = 0
        
        # ENTRADA
        if not np.isnan(data['confirma_compra'][idx]) and in_buy != 1:
            
            if data['plus_di'][idx] <= data['minus_di'][idx]:
                continue
            if data['adx'][idx] <= thr:
                continue
            if data['adx'][idx] <= data['adx'][idx-1]:
                continue
            if data['tema'][idx] <= 0:
                continue
            if data['upperband'][idx] <= data['upperband'][idx-1] and data['lowerband'][idx] >= data['lowerband'][idx-1]: # volatilidade
                continue
            if alerta_compra <= delta:
                in_buy = 1
                out['ATIVO'].append(ticker)
                out['DATA ENTRADA'].append(data.index[idx])
                out['PREÇO ENTRADA'].append(round(data['high'][idx],2))

                entrada = data['high'][idx]

        # SAIDA
        if in_buy == 0:
            continue

        if not np.isnan(data['confirma_venda'][idx]):
            in_buy = 0

            out['DATA SAIDA'].append(data.index[idx])
            out['PREÇO SAIDA'].append(round(data['low'][idx],2))

            saida = data['low'][idx]

            fator = saida/entrada

            out['FATOR'].append(fator)

            try:
                cum_fator *= fator
            except:
                pass

        if trend_method == 'trix':
            if data['trix'][idx] >= 0:
                continue
            if data['adx'][idx] >= data['adx'][idx-1]:
                continue
            if abs(data['m3'][idx] - data['m20'][idx]) >= abs(data['m3'][idx-1] - data['m20'][idx-1]): #aproximação das médias
                continue
            if data['upperband'][idx] <= data['upperband'][idx-1] and data['lowerband'][idx] >= data['lowerband'][idx-1]: 
                in_buy = 0

                out['DATA SAIDA'].append(data.index[idx])
                out['PREÇO SAIDA'].append(round(data['low'][idx],2))

                saida = data['low'][idx]

                fator = saida/entrada

                out['FATOR'].append(fator)

                try:
                    cum_fator *= fator
                except:
                    pass

        if trend_method == 'tema':
            if data['tema'][idx] >= 0:
                continue
            if data['plus_di'][idx] >= data['minus_di'][idx]:
                continue
            if data['adx'][idx] >= data['adx'][idx-2]:
                continue
            if abs(data['m3'][idx] - data['m20'][idx]) >= abs(data['m3'][idx-1] - data['m20'][idx-1]):
                continue
            if data['upperband'][idx] <= data['upperband'][idx-1] and data['lowerband'][idx] >= data['lowerband'][idx-1]: 
                in_buy = 0

                out['DATA SAIDA'].append(data.index[idx])
                out['PREÇO SAIDA'].append(round(data['low'][idx],2))

                saida = data['low'][idx]

                fator = saida/entrada

                out['FATOR'].append(fator)

                try:
                    cum_fator *= fator
                except:
                    pass
        
    if len(out['DATA ENTRADA']) > len(out['DATA SAIDA']):
        stock_return = (data.iloc[-1]/data.iloc[0]).close

        return {'stock': ticker, 'status': 'inBuy','date':data.index[idx],'price': out['PREÇO ENTRADA'][-1], 'timeframe': tm, 'bb': 1}

    return {}


async def sell_func(data: pd.DataFrame,ticker: str,thr: int,delta: int,trend_method :str ='tema',tm: str = '1wk')-> dict:
    out = {"ATIVO": [],"PREÇO ENTRADA": [], "DATA ENTRADA": [], "PREÇO SAIDA": [], "DATA SAIDA": [], "FATOR": [], "BB": 0}
    saida = json.dumps({'symbol': ticker,'status': None, 'date':None,'price': None, "BB": None})

    if data.empty:
        return {}

    alerta_venda = 0

    in_sell = 0
    cum_fator = 1

    for idx in range(len(data)):
        if not np.isnan(data['alerta_compra'][idx]):
            alerta_venda = 0
        elif not np.isnan(data['alerta_venda'][idx]):
            alerta_venda += 1
        else:
            alerta_venda = 0
        
        # ENTRADA
        if not np.isnan(data['confirma_venda'][idx]) and in_sell != 1:
            
            if data['plus_di'][idx] >= data['minus_di'][idx]:
                continue
            if data['adx'][idx] <= thr:
                continue
            if data['tema'][idx] >= 0:
                continue
            if data['adx'][idx] <= data['adx'][idx-1]:
                continue
            if alerta_venda <= delta:
                in_sell = 1
                out['ATIVO'].append(ticker)
                out['DATA ENTRADA'].append(data.index[idx])
                out['PREÇO ENTRADA'].append(round(data['low'][idx],2))

                entrada = data['low'][idx]
        # SAIDA
        if in_sell == 0:
            continue
        
        if not np.isnan(data['confirma_compra'][idx]):
            in_sell = 0
                
            out['DATA SAIDA'].append(data.index[idx])
            out['PREÇO SAIDA'].append(round(data['high'][idx],2))
            
            saida = data['high'][idx]

            if (entrada-saida) < 0:    
                fator = abs((abs(entrada-saida)/entrada)-1)
            elif (entrada-saida) > 0:
                fator = abs((abs(entrada-saida)/entrada)+1)
            else:
                fator = 0      

            out['FATOR'].append(fator)

            try:
                cum_fator *= fator
            except:
                pass

        if trend_method == 'trix':
            if data['trix'][idx] <= 0:
                continue
            if data['adx'][idx] >= data['adx'][idx-1]:
                continue
            if abs(data['m3'][idx] - data['m20'][idx]) < abs(data['m3'][idx-1] - data['m20'][idx-1]):
                in_sell = 0

                out['DATA SAIDA'].append(data.index[idx])
                out['PREÇO SAIDA'].append(round(data['high'][idx],2))
                
                saida = data['high'][idx]

                if (entrada-saida) < 0:    
                    fator = abs((abs(entrada-saida)/entrada)-1)
                elif (entrada-saida) > 0:
                    fator = abs((abs(entrada-saida)/entrada)+1)
                else:
                    fator = 0   
                out['FATOR'].append(fator)

                try:
                    cum_fator *= fator
                except:
                    pass

        if trend_method == 'tema':
            if data['tema'][idx] <= 0:
                continue
            if data['plus_di'][idx] <= data['minus_di'][idx]:
                continue
            if data['adx'][idx] >= data['adx'][idx-2]:
                continue
            if abs(data['m3'][idx] - data['m20'][idx]) < abs(data['m3'][idx-1] - data['m20'][idx-1]):
                in_sell = 0
                
                out['DATA SAIDA'].append(data.index[idx])
                out['PREÇO SAIDA'].append(round(data['high'][idx],2))
                
                saida = data['high'][idx]

                if (entrada-saida) < 0:    
                    fator = abs((abs(entrada-saida)/entrada)-1)
                elif (entrada-saida) > 0:
                    fator = abs((abs(entrada-saida)/entrada)+1)
                else:
                    fator = 0

                out['FATOR'].append(fator)

                try:
                    cum_fator *= fator
                except:
                    pass

    if len(out['DATA ENTRADA']) > len(out['DATA SAIDA']):
        stock_return = (data.iloc[-1]/data.iloc[0]).close
        
        return {'stock': ticker, 'status': 'inSell','date': data.index[idx],'price': out['PREÇO ENTRADA'][-1], 'timeframe': tm, 'bb': 0}

    return {}

async def sell_func_bbands(data: pd.DataFrame,ticker: str,thr: int,delta: int,trend_method :str ='tema',tm: str = '1wk')-> dict:
    out = {"ATIVO": [],"PREÇO ENTRADA": [], "DATA ENTRADA": [], "PREÇO SAIDA": [], "DATA SAIDA": [], "FATOR": [], "BB": 1}
    saida = json.dumps({'symbol': ticker,'status': None, 'date':None,'price': None, "BB": None})

    if data.empty:
        return {}

    alerta_venda = 0

    in_sell = 0
    cum_fator = 1

    for idx in range(len(data)):
        if not np.isnan(data['alerta_compra'][idx]):
            alerta_venda = 0
        elif not np.isnan(data['alerta_venda'][idx]):
            alerta_venda += 1
        else:
            alerta_venda = 0
        
        # ENTRADA
        if not np.isnan(data['confirma_venda'][idx]) and in_sell != 1:
            
            if data['plus_di'][idx] >= data['minus_di'][idx]:
                continue
            if data['adx'][idx] <= thr and data['tema'][idx] >= 0:
                continue
            if data['adx'][idx] <= data['adx'][idx-1]:
                continue
            if data['upperband'][idx] <= data['upperband'][idx-1] and data['lowerband'][idx] >= data['lowerband'][idx-1]: # volatilidade
                continue
            if alerta_venda <= delta:
                in_sell = 1
                out['ATIVO'].append(ticker)
                out['DATA ENTRADA'].append(data.index[idx])
                out['PREÇO ENTRADA'].append(round(data['low'][idx],2))

                entrada = data['low'][idx]
        # SAIDA
        if in_sell == 0:
            continue
        
        if not np.isnan(data['confirma_compra'][idx]):
            in_sell = 0
                
            out['DATA SAIDA'].append(data.index[idx])
            out['PREÇO SAIDA'].append(round(data['high'][idx],2))
            
            saida = data['high'][idx]

            if (entrada-saida) < 0:    
                fator = abs((abs(entrada-saida)/entrada)-1)
            elif (entrada-saida) > 0:
                fator = abs((abs(entrada-saida)/entrada)+1)
            else:
                fator = 0

            out['FATOR'].append(fator)

            try:
                cum_fator *= fator
            except:
                pass

        if trend_method == 'trix':
            if data['trix'][idx] <= 0:
                continue
            if data['adx'][idx] > data['adx'][idx-1]:
                continue
            if abs(data['m3'][idx] - data['m20'][idx]) > abs(data['m3'][idx-1] - data['m20'][idx-1]):
                continue
            if data['upperband'][idx] <= data['upperband'][idx-1] and data['lowerband'][idx] >= data['lowerband'][idx-1]: # volatilidade
                in_sell = 0

                out['DATA SAIDA'].append(data.index[idx])
                out['PREÇO SAIDA'].append(round(data['high'][idx],2))
                
                saida = data['high'][idx]

                if (entrada-saida) < 0:    
                    fator = abs((abs(entrada-saida)/entrada)-1)
                elif (entrada-saida) > 0:
                    fator = abs((abs(entrada-saida)/entrada)+1)
                else:
                    fator = 0

                out['FATOR'].append(fator)

                try:
                    cum_fator *= fator
                except:
                    pass
                
        if trend_method == 'tema':
            if data['tema'][idx] <= 0:
                continue
            if data['adx'][idx] >= data['adx'][idx-2]:
                continue
            if data['plus_di'][idx] <= data['minus_di'][idx]:
                continue
            if abs(data['m3'][idx] - data['m20'][idx]) >= abs(data['m3'][idx-1] - data['m20'][idx-1]):
                continue
            if data['upperband'][idx] <= data['upperband'][idx-1] and data['lowerband'][idx] >= data['lowerband'][idx-1]: # volatilidade
                in_sell = 0
                
                out['DATA SAIDA'].append(data.index[idx])
                out['PREÇO SAIDA'].append(round(data['high'][idx],2))
                
                saida = data['high'][idx]

                if (entrada-saida) < 0:    
                    fator = abs((abs(entrada-saida)/entrada)-1)
                elif (entrada-saida) > 0:
                    fator = abs((abs(entrada-saida)/entrada)+1)
                else:
                    fator = 0       

                out['FATOR'].append(fator)

                try:
                    cum_fator *= fator
                except:
                    pass
                

    if len(out['DATA ENTRADA']) > len(out['DATA SAIDA']):
        stock_return = (data.iloc[-1]/data.iloc[0]).close

        return {'stock': ticker, 'status': 'inSell','date':data.index[idx],'price': out['PREÇO ENTRADA'][-1], 'timeframe': tm, 'bb': 1}

    return {}

async def main(date=dt.date.today()):

    if not mt5.initialize():
        print("eita")
        quit()

    dff = pd.read_excel(r"C:\Users\M3\Desktop\m3\opa_opa_d_w_mt5_2012_2.xlsx")

    dff = dff.loc[dff.groupby(['ATIVO','EVENTO','TIMEFRAME'])["RESULTADO"].idxmax(), ['ATIVO', 'EVENTO','TIMEFRAME','THRESHOLD', 'DELTA', 'MÉTODO']]

    start_date = '2022-01-01'

    dic = []
    i=0

    for idx, row in dff.iterrows():
        stock = get_historical_data(row['ATIVO'],interval=row['TIMEFRAME'],start_date=start_date,end_date=str(date))
        data = get_indicators(stock)

        if row['EVENTO'] == 'COMPRA':          
            tf1 = asyncio.create_task(buy_func(data,row['ATIVO'],row['THRESHOLD'],row['DELTA'],row['MÉTODO'],row['TIMEFRAME']))
            tf2 = asyncio.create_task(buy_func_bbands(data,row['ATIVO'],row['THRESHOLD'],row['DELTA'],row['MÉTODO'],row['TIMEFRAME']))
            
            r1 = await tf1
            r2 = await tf2
            dic.extend([r1,r2])

        else:
            tf3 = asyncio.create_task(sell_func(data,row['ATIVO'],row['THRESHOLD'],row['DELTA'],row['MÉTODO'],row['TIMEFRAME']))
            tf4 = asyncio.create_task(sell_func_bbands(data,row['ATIVO'],row['THRESHOLD'],row['DELTA'],row['MÉTODO'],row['TIMEFRAME']))

            r3 = await tf3
            r4 = await tf4

            dic.extend([r3,r4])

        i+=1
        print(i)

    res = list(filter(lambda item: item is not None, dic))

    mt5.shutdown()

    df = pd.DataFrame.from_records(res)

    return df.dropna(subset=['stock'],axis=0)

#%%
# asyncio.run(main(date=dt.date.today()))
df = asyncio.run(main())
df.to_excel("alertas_.xlsx",index=False)
print(df)
#%%

