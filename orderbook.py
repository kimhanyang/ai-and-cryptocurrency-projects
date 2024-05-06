 #! /usr/bin/env python
 # rdate -s time.nist.gov
 import sys
 import datetime
 import json
 import csv
 import os
 import requests
 import time
 import pandas as pd
 import argparse
 import pytz
 import timeit
 import urllib3
 import numpy as np
 import collections
 from requests.adapters import HTTPAdapter
 from requests.packages.urllib3.util.retry import Retry
 def agg_order_book(bids, asks):

group_bid = (bids.groupby('price').sum()).reset_index()
 group_bid = group_bid.sort_values('price', ascending=False)
 group_ask = (asks.groupby('price').sum()).reset_index()
 group_ask = group_ask.sort_values('price', ascending=True)
 group_ask['type'] = 1
 return group_bid, group_ask
 def bithumb_live_book(data, req_timestamp):
 #timestamp, price, type, quantity
 data = data['data']
 bids = (pd.DataFrame(data['bids'])).apply(pd.to_numeric,errors='coerce')
 bids.sort_values('price', ascending=False, inplace=True)
 bids.reset_index(drop=True, inplace=True)
    bids['type'] = 0
    asks = (pd.DataFrame(data['asks'])).apply(pd.to_numeric,errors='coerce')
    asks.sort_values('price', ascending=True, inplace=True)
    asks['type'] = 1
    df = pd.concat([bids, asks])
    df['quantity'] = df['quantity'].round(decimals=4)
    df['timestamp'] = req_timestamp
    return df
 def agg_diff_trade(diff):
    df = diff.copy()
    df['count'] = ''
    if df.empty:
        df = pd.concat([df, pd.DataFrame([{'price':0, 'total':0, 'transaction_date':0, 
'type':0, 'units_traded':0, 'count':0}])], ignore_index=True)
        return df
    group_bid = df[(df.type == 0)].copy().reset_index()
    group_ask = df[(df.type == 1)].copy().reset_index()
    if not group_bid.empty:
        quant = group_bid['units_traded'].sum()
        w_price = int(group_bid['total'].sum() / quant)
        group_bid.loc[0, 'units_traded'] = quant
        group_bid.loc[0, 'price'] = w_price
        group_bid.loc[0, 'type'] = 0
        group_bid.loc[0, 'count'] = len(group_bid.index)
    if not group_ask.empty:
        quant = group_ask['units_traded'].sum()
        w_price = int(group_ask['total'].sum() / quant)
        group_ask.loc[0, 'units_traded'] = quant
        group_ask.loc[0, 'price'] = w_price
        group_ask.loc[0, 'type'] = 1
        group_ask.loc[0, 'count'] = len(group_ask.index)
    df = pd.concat([group_bid.head(1), group_ask.head(1)])
    df = df.astype({'total': int, 'price': int, 'type': int, 'count': int})
    df.reset_index(drop=True, inplace=True)
    #del df['index']
    return df
 #This part of the code is too messy; might rework on this
 first_seq = True
 df1 = ''
 bithumb_empty_df = pd.DataFrame(columns=['price', 'total', 'transaction_date', 'type', 
'units_traded'])
 def bithumb_live_trade(data, req_timestamp):
    global df1
    global first_seq
    df = (pd.DataFrame(data['data'])).apply(pd.to_numeric,errors='coerce')
    df.loc[df['type'] == 'bid', 'type'] = 0
    df.loc[df['type'] == 'ask', 'type'] = 1
    df = (df.sort_values(by=['transaction_date'], ascending=False)).reset_index()
    if first_seq:
        df1 = df
        first_seq = False
        return None, None
    df2 = df
    ###
    #print df1
    #print df2
    #print req_timestamp
    #print df2.isin(df1.head(1))
    _index = 50
    if not df1.empty:
        _h = df1.head(1)
        _l_index = df2[(df2['price']==_h['price'].values[0])
                & (df2['units_traded']==_h['units_traded'].values[0])
                & (df2['transaction_date']==_h['transaction_date'].values[0])
                & (df2['type']==_h['type'].values[0])].index.tolist()
        if _l_index:
            _index = _l_index[0]
    df1 = df
    diff = bithumb_empty_df
    if _index > 0:
        diff = df2[0:_index]
    diff = agg_diff_trade(diff)
    diff['timestamp'] = req_timestamp
    df['timestamp'] = req_timestamp
    return diff[['price', 'total', 'transaction_date', 'type', 'units_traded', 'timestamp', 
'count']], df
 def write_csv(fn, df):
    need_header = not os.path.exists(fn)
    df.to_csv(fn, index=False, header=need_header, mode='a')
 def http_get(url):
    return (session.get(url, headers={ 'User-Agent': 'Mozilla/5.0' }, verify=False, 
timeout=1)).json()
 def get_book_trade(ex, url, req_timestamp):
    book = trade = {}
    try:
        book = (session.get(url[0], headers={ 'User-Agent': 'Mozilla/5.0' }, 
verify=False, timeout=1)).json()
        trade = (session.get(url[1], headers={ 'User-Agent': 'Mozilla/5.0' }, 
verify=False, timeout=1)).json()
    except:
        return None, None
    return book, trade
 def pull_csv_book_trade():
    timestamp = last_update_time = datetime.datetime.now()
    kst_tz = pytz.timezone('Asia/Seoul')
    
    show_info = True
    while True:
        timestamp = datetime.datetime.now()
        if ((timestamp-last_update_time).total_seconds() < 1.0):
            continue
        last_update_time = timestamp
        timestamp_kst = timestamp.astimezone(tz=kst_tz)
        req_timestamp = timestamp_kst.strftime('%Y-%m-%d %H:%M:%S.%f')
        req_time = req_timestamp.split(' ')[0]
        _dict_book_trade = {}
        _err = False
        if show_info == True: #for checking purposes
          start_time = timeit.default_timer()
        for ex in ex_data:
            id, ex_market, currency = ex
            book, trade = get_book_trade(ex_market, _dict_url[id], req_timestamp)
            #if book is None or trade is None: #seems redundant
            if not book or not trade:
                _err = True
                break
            _dict_book_trade.update({id: [book, trade]})
        if show_info: #for checking purposes
          delay = timeit.default_timer() - start_time
          print("fetch delay: %.2fs" %delay)
          show_info = False
        if _err == True:
            continue
        for ex in ex_data:
            id, ex_market, currency = ex
            book, trade = _dict_book_trade[id]
            #"book-yyyy-mm-dd-exchange-market.csv"
            #"book-2021-04-22-bithumb-btc.csv"
            book_fn = '%s/book-%s-%s-%s.csv'% (csv_dir, req_time, ex_market, 
currency)
            book_df = bithumb_live_book(book, req_timestamp)
            write_csv(book_fn, book_df)
            #trade_fn = '%s/%s-only-%s-%s-trade.csv'% (csv_dir, req_time, 
ex_market, currency)
            #trade_df, raw_trade_df = bithumb_live_trade(trade, req_timestamp)
            #if trade_df is None:
            #    continue
            #write_csv(trade_fn, trade_df)
        time.sleep(interval) #Sleep few seconds for each interval.
 def init_session():
    session = requests.Session()
    retry = Retry (connect=1, backoff_factor=0.1)
    adapter = HTTPAdapter (max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session
 def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--currency', help="choose crypto-currency", choices = 
('BTC','ETH'), dest='currency', action="store")
    return parser.parse_args()
 session = init_session()
 csv_dir = '.' #In colab, this starts in the /content directory.
 ex_data = [['1', 'bithumb', 'BTC'], ['2', 'bithumb', 'ETH']] #WIP; this could be 
changed.
 starting_time = {'hour': 0, 'minute': 0, 'second': 0, 'microsecond': 0} #Set this in 
00:00.
 level = 15
 interval = 5
 _dict_url = { #hard-coded all the urls
    '1':
    ['https://api.bithumb.com/public/orderbook/%s_KRW/?count=%d' %('BTC', level),
    'https://api.bithumb.com/public/transaction_history/%s_KRW/?count=50' %'BTC'],
    '2':
    ['https://api.bithumb.com/public/orderbook/%s_KRW/?count=%d' %('ETH', level),
    'https://api.bithumb.com/public/transaction_history/%s_KRW/?count=50' %'ETH']}
 def main():
    urllib3.disable_warnings()
    #args = parse_args()
    #currency = args.currency
    current_timezone = datetime.datetime.now().astimezone().tzinfo
    kst_tz = pytz.timezone('Asia/Seoul')
    print("Current system timezone: %s" %current_timezone)
    if current_timezone == kst_tz:
      print("NOTE: current timezone is already in KST")
    init_time = datetime.datetime.now(tz=kst_tz).replace(**starting_time)
    if datetime.datetime.now(tz=kst_tz) > init_time:
      init_time += datetime.timedelta(days=1)
    delay = init_time - datetime.datetime.now(tz=kst_tz)
    print("Data collection will start in:", delay)
    time.sleep(delay.total_seconds())
    print("Starting...")
    print("Please come back at:", datetime.datetime.now(tz=kst_tz) + 
datetime.timedelta(days=1))
    pull_csv_book_trade()
 if __name__ == '__main__':
    main()
