import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import yfinance as yf

def implement_didi_strategy(data: pd.DataFrame, out: str ='both') -> pd.DataFrame:
    """
    --- Para o indicador DIDI INDEX, tanto na onfirmação de compra quanto na de venda
    está sendo considerado para a média de 20 períodos o seu aumento de inclinação(venda) 
    ou queda de inclinação(compra), e não o seu de fato cruzamento com a média de 8 períodos.

    --- O objetivo desse ajuste é antecipar entradas e saídas nos trades, sabendo que
    isso pode incorrer em entradas falsas.


    Args:
        data (_type_): _description_
        out (str, optional): _description_. Defaults to 'both'.

    Returns:
        _type_: _description_
    """

    # Dicionário para iterar sobre
    dict_ = data.to_dict('records')

    # Variáveis auxiliares
    lenght = len(data)
    counter = 0

    # Estruturas para armazenar os resultados
    buy_alert = np.array([np.nan]*lenght)
    buy_confirm = np.array([np.nan]*lenght)
    
    sell_alert = np.array([np.nan]*lenght)
    sell_confirm = np.array([np.nan]*lenght)

    # Iteração sobre cada média
    for idx in range(lenght):
        
        # Alerta de compra
        if dict_[idx]['m3'] > dict_[idx]['m8'] and dict_[idx]['m8'] < dict_[idx]['m20']:

            buy_alert[idx] = dict_[idx]['close']
            buy_confirm[idx] = np.nan

            sell_alert[idx] = np.nan
            sell_confirm[idx] = np.nan

        # Confirma compra
        elif dict_[idx]['m3'] > dict_[idx]['m8'] and dict_[idx]['m20'] < dict_[idx-1]['m20']:
            
            buy_alert[idx] = dict_[idx]['close']
            
            if buy_alert[idx - 1] == np.nan:
                buy_confirm[idx] = np.nan
            else:
                buy_confirm[idx] = dict_[idx]['close']

            sell_alert[idx] = np.nan
            sell_confirm[idx] = np.nan

        # Alerta de venda
        if dict_[idx]['m3'] < dict_[idx]['m8'] and dict_[idx]['m8'] > dict_[idx]['m20']:

            buy_alert[idx] = np.nan
            buy_confirm[idx] = np.nan

            sell_alert[idx] = dict_[idx]['close']
            sell_confirm[idx] = np.nan

        # Confirma venda 
        elif dict_[idx]['m3'] < dict_[idx]['m8'] and dict_[idx]['m20'] > dict_[idx-1]['m20']:

            buy_alert[idx] = np.nan
            buy_confirm[idx] = np.nan

            sell_alert[idx] = dict_[idx]['close']
            
            if sell_alert[idx - 1] == np.nan:
                sell_confirm[idx] = np.nan
            else:
                sell_confirm[idx] = dict_[idx]['close']
        
        counter += 1

    data.loc[:,'alerta_compra'] = buy_alert
    data.loc[:,'confirma_compra'] = buy_confirm
    data.loc[:,'alerta_venda'] = sell_alert
    data.loc[:,'confirma_venda'] = sell_confirm

    return data

def concat_events(data):
    f = data.to_dict('records')
    events = np.array([np.nan]*len(data))

    for idx in range(len(data)):
        if not np.isnan(f[idx]['alerta_compra']):
            events[idx] = -1
        if not np.isnan(f[idx]['alerta_venda']):
            events[idx] = -2
        if not np.isnan(f[idx]['confirma_compra']):
            events[idx] = 1
        if not np.isnan(f[idx]['confirma_venda']):
            events[idx] = 2

    data['events'] = events

    return data

def data_processing(data,normalize=False):
    df = data

    df = df.dropna(subset='close',axis=0)
    
    # ADIÇÃO DAS MÉDIAS MÓVEIS AOS DADOS
    df.loc[:,"m3"] = df['close'].rolling(window=3).mean()
    df.loc[:,"m8"] = df['close'].rolling(window=8).mean()
    df.loc[:,"m20"] = df['close'].rolling(window=20).mean()

    didi_df = df

    if normalize:
        # DATAFRAME COM AS MÉDIAS "NORMALIZADAS"
        didi_df.loc[:,"m3"] = didi_df["m3"]/didi_df["m8"]
        didi_df.loc[:,"m20"] = didi_df["m20"]/didi_df["m8"]
        didi_df.loc[:,"m8"] = didi_df["m8"]/didi_df["m8"]

    return didi_df

def maximum_value_in_column(column):    

    highlight = 'background-color: RED;'
    default = ''

    maximum_in_column = column.max()

    # must return one string per cell in this column
    return [highlight if v == maximum_in_column else default for v in column]

def plot_index(data):
    fig = px.line(data, x=data.index, y=['close','m3','m8','m20'],
            labels={
                "close": "fechamento",
                "m3": "média de 3",
                "m8": "média de 8",
                "m20": "média de 20"
            }, title="Médias Móveis normalizadas",
            template='plotly_dark')

    fig.update_layout(
        xaxis_title="Data",
        yaxis_title=""
    )

    fig.add_trace(go.Scatter(
                    x=data.index,
                    y=data['alerta_compra'],
                    mode='markers',
                    marker_symbol="circle-dot",
                    marker=dict(color=('yellow'),size=(15)),
                    name='alert_buy'
                ))

    fig.add_trace(go.Scatter(
                    x=data.index,
                    y=data['confirma_compra'],
                    mode='markers',
                    marker_symbol="circle-dot",
                    marker=dict(color=('red'),size=(7)),
                    name='buy'
                ))

    fig.add_trace(go.Scatter(
                    x=data.index,
                    y=data['alerta_venda'],
                    mode='markers',
                    marker_symbol="circle-dot",
                    marker=dict(color=('orange'),size=(15)),
                    name='alert_sell'
                ))

    fig.add_trace(go.Scatter(
                    x=data.index,
                    y=data['confirma_venda'],
                    mode='markers',
                    marker_symbol="circle-dot",
                    marker=dict(color=('green'),size=(7)),
                    name='sell'
                ))

    fig.update_xaxes(rangeslider_visible=True)
    fig.update_yaxes(fixedrange=False)

    fig.show()