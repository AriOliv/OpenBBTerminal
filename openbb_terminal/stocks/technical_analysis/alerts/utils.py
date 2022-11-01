import requests
import pandas as pd

url = "https://orealvalor.com.br/blog/cnpj-de-todas-as-acoes-da-b3/"

header = {
  "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.75 Safari/537.36",
  "X-Requested-With": "XMLHttpRequest"
}

r = requests.get(url, headers=header)

dfs = pd.read_html(r.text)[0]
allTickers = set(dfs.Ticker.tolist())

ASSETS_LIST = [
    'AGRO3','BLAU3','VBBR3','ENEV3',
    'EQTL3','FESA4','GGBR4','JALL3',
    'KEPL3','PRIO3','SMTO3','TGMA3',
    'TTEN3','TUPY3','VAMO3','ETER3',
    'PETR4','FHER3','PCAR3','UNIP3','UNIP6','HBSA3'
]

WATCH_LIST = [
  'SIMH3','RAPT4','LOGG3','VALE3',
  'POSI3','PGMN3','AURA33','RANI3',
  'CBAV3','MULT3','POMO3','ASAI3',
  'CSAN3','RAIL3','CSUD3','TOTS3',
  'PORT3','VIVA3','SOMA3','KLBN11',
  'WEGE3','ENBR3','EZTC3','MTRE3',
]

import MetaTrader5 as mt5
# if not mt5.initialize():
#         print("eita")
#         quit()
# syn=mt5.symbols_get()
# mt5.shutdown()
# [coisa.name for coisa in syn if coisa.path[:16] == 'BOVESPA\\A VISTA\\']