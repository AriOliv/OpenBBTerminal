import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from math import floor

import yfinance as yf

def get_historical_data(symbol, start_date='2012-01-01',period='10y',interval='1wk'):
    if symbol[0] == '^':
        df = yf.Ticker(symbol)
    else:
        df = yf.Ticker(symbol + '.SA')
        
    df = df.history(start=start_date,period=period,interval=interval)
    df[['high','low','close']] = df[['High','Low','Close']]

    df = df.drop(['Open','High','Low','Close','Dividends','Stock Splits'],axis=1)

    return df.dropna()

def get_adx(high, low, close, lookback):
    plus_dm = high.diff()
    minus_dm = low.diff()
    plus_dm[plus_dm < 0] = 0
    minus_dm[minus_dm > 0] = 0
    
    tr1 = pd.DataFrame(high - low)
    tr2 = pd.DataFrame(abs(high - close.shift(1)))
    tr3 = pd.DataFrame(abs(low - close.shift(1)))
    frames = [tr1, tr2, tr3]
    tr = pd.concat(frames, axis = 1, join = 'inner').max(axis = 1)
    atr = tr.rolling(lookback).mean()
    
    plus_di = 100 * (plus_dm.ewm(alpha = 1/lookback).mean() / atr)
    minus_di = abs(100 * (minus_dm.ewm(alpha = 1/lookback).mean() / atr))
    dx = (abs(plus_di - minus_di) / abs(plus_di + minus_di)) * 100
    adx = ((dx.shift(1) * (lookback - 1)) + dx) / lookback
    adx_smooth = adx.ewm(alpha = 1/lookback).mean()
    
    return plus_di, minus_di, adx_smooth

def implement_adx_strategy(prices, pdi, ndi, adx, threshold):
    length = len(prices)
    buy_price = np.array([np.nan]*length)
    sell_price = np.array([np.nan]*length)
    adx_signal = np.array([np.nan]*length)
    signal = 0
    
    for i in range(length):
        if adx[i-1] < threshold and adx[i] > threshold and pdi[i] > ndi[i]:
            if signal != 1:
                buy_price[i] = prices[i]
                sell_price[i] = np.nan
                signal = 1
                adx_signal[i] = signal
            else:
                buy_price[i] = np.nan
                sell_price[i] = np.nan
                adx_signal[i] =  0
        elif adx[i-1] < threshold and adx[i] > threshold and ndi[i] > pdi[i]:
            if signal != -1:
                buy_price[i] = np.nan
                sell_price[i] = prices[i]
                signal = -1
                adx_signal[i] = signal
            else:
                buy_price[i] = np.nan
                sell_price[i] = np.nan
                adx_signal[i] = 0
        else:
            buy_price[i] = np.nan
            sell_price[i] = np.nan
            adx_signal[i] =  0
            
    return buy_price, sell_price, adx_signal


def get_benchmark(bench_name, start_date, investment_value):
    bench = get_historical_data(bench_name.upper(), start_date)['close']
    benchmark = pd.DataFrame(np.diff(bench)).rename(columns = {0:'benchmark_returns'})
    
    investment_value = investment_value
    number_of_stocks = floor(investment_value/bench[-1])
    benchmark_investment_ret = []
    
    for i in range(len(benchmark['benchmark_returns'])):
        returns = number_of_stocks*benchmark['benchmark_returns'][i]
        benchmark_investment_ret.append(returns)

    benchmark_investment_ret_df = pd.DataFrame(benchmark_investment_ret).rename(columns = {0:'investment_returns'})
    return benchmark_investment_ret_df