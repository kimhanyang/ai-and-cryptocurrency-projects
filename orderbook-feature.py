
import math
import pandas as pd
import numpy as np

def calc_mid_price():
  top = orderbook_df.iloc[::level]
  bid_top, ask_top = top.iloc[0::2].reset_index(), top.iloc[1::2].reset_index()

  mean = orderbook_df.groupby(['timestamp', 'type']).mean()
  bid_mean, ask_mean = mean.iloc[0::2].reset_index(), mean.iloc[1::2].reset_index()

  mid_price = (bid_top['price'] + ask_top['price']) / 2
  mid_price_wt = (bid_mean['price'] + ask_mean['price']) / 2
  mid_price_mkt = (bid_top['price'] * ask_top['quantity'] + ask_top['price'] * bid_top['quantity']) / (bid_top['quantity'] + ask_top['quantity'])
  mid_price_vwap = (trade_df['units_traded'] * trade_df['price']).sum() / trade_df['units_traded'].sum()

  mid_price_data = {
    'mid_price': mid_price,
    'mid_price_wt': mid_price_wt,
    'mid_price_mkt': mid_price_mkt,
    'mid_price_vwap': mid_price_vwap
  }
  features.update(mid_price_data)

def calc_book_imbalance(ratio = 0.2, interval = 1):
  quants = orderbook_df['quantity'] ** ratio
  prices = orderbook_df['price'] * quants

  new_df = pd.concat([orderbook_df[['timestamp', 'type']], quants, prices], axis=1)
  new_df.columns = ['timestamp', 'type', 'quantity', 'price']

  sum_df = new_df.groupby(['timestamp', 'type']).sum().reset_index()
  bid_sum, ask_sum = sum_df.iloc[0::2].reset_index(), sum_df.iloc[1::2].reset_index()

  bid_qty, ask_qty = bid_sum['quantity'], ask_sum['quantity']
  bid_px, ask_px = bid_sum['price'], ask_sum['price']

  book_price = (ask_qty * bid_px / bid_qty + bid_qty * ask_px / ask_qty) / (bid_qty + ask_qty)
  book_price.fillna(0) #Consider cases where 'book_price' == NaN due to division by zero.

  mid_price = features['mid_price']
  book_imbalance = (book_price - mid_price) / interval
  feature_name = f'book_imbalance-{ratio}-{level}-{interval}'
  features.update({feature_name: book_imbalance})

  # Drop rows where book_imbalance is NaN due to division by zero
  #orderbook_df.dropna(subset=['book_imbalance'], inplace=True)

def calc_book_delta(ratio = 0.2, interval = 1):
  decay = math.exp(-1.0 / interval)
  book_delta = []

  def extend_first_row(df):
    df = df.reset_index()
    new_df = pd.concat([df.iloc[[0]], df], axis=0, ignore_index=True)
    return new_df
    
  top = orderbook_df.iloc[::level]
  bid_top, ask_top = extend_first_row(top.iloc[0::2]), extend_first_row(top.iloc[1::2])
  
  sum_df = orderbook_df.groupby(['timestamp', 'type']).sum()
  bid_sum, ask_sum = extend_first_row(sum_df.iloc[0::2]), extend_first_row(sum_df.iloc[1::2])

  bidSideAdd, bidSideDelete, bidSideTrade, bidSideFlip, bidSideCount = [0] * 5
  askSideAdd, askSideDelete, askSideTrade, askSideFlip, askSideCount = [0] * 5

  trade_counts = pd.merge(bid_top['timestamp'], trade_df[['timestamp', 'type', 'count']], on='timestamp', how='left')
  trade_counts = trade_counts.groupby(['timestamp', 'type'])['count'].sum().reset_index()
  trade_counts = trade_counts.pivot(index='timestamp', columns='type', values='count').fillna(0).reset_index()

  for i in range(1, len(bid_top)):
    curBidQty, prevBidQty = bid_sum.at[i, 'quantity'], bid_sum.at[i - 1, 'quantity']
    curAskQty, prevAskQty = ask_sum.at[i, 'quantity'], ask_sum.at[i - 1, 'quantity']
    curBidTop, prevBidTop = bid_top.at[i, 'price'], bid_top.at[i - 1, 'price']
    curAskTop, prevAskTop = ask_top.at[i, 'price'], ask_top.at[i - 1, 'price']

    if curBidQty > prevBidQty:
        bidSideAdd += 1
        bidSideCount += 1
    if curBidQty < prevBidQty:
        bidSideDelete += 1
        bidSideCount += 1
    if curAskQty > prevAskQty:
        askSideAdd += 1
        askSideCount += 1
    if curAskQty < prevAskQty:
        askSideDelete += 1
        askSideCount += 1
    if curBidTop < prevBidTop:
        bidSideFlip += 1
        bidSideCount += 1
    if curAskTop > prevAskTop:
        askSideFlip += 1
        askSideCount += 1

    #current_trade_df = trade_df[trade_df['timestamp'] == bid_top.at[i, 'timestamp']]
    #current_trade_df = current_trade_df.drop('timestamp', axis=1).groupby(['type']).sum().reset_index()
    #print(current_trade_df)

    #count_0, count_1 = 0, 0
    #if (current_trade_df['type'] == 0).any():
    #  count_0 = current_trade_df[current_trade_df['type'] == 0]['count'].item()
    #if (current_trade_df['type'] == 1).any():
    #  count_1 = current_trade_df[current_trade_df['type'] == 1]['count'].item()
    
    count_0, count_1 = trade_counts.at[i - 1, 0].item(), trade_counts.at[i - 1, 1].item()

    bidSideTrade += count_1
    bidSideCount += count_1
    askSideTrade += count_0
    askSideCount += count_0

    if bidSideCount == 0:
        bidSideCount = 1
    if askSideCount == 0:
        askSideCount = 1

    bidBookV = (bidSideDelete - bidSideAdd + bidSideFlip) / (bidSideCount ** ratio)
    askBookV = (askSideDelete - askSideAdd + askSideFlip) / (askSideCount ** ratio)
    tradeV = (askSideTrade / askSideCount ** ratio) - (bidSideTrade / bidSideCount ** ratio)
    book_delta += [askBookV - bidBookV + tradeV]

    bidSideCount *= decay
    askSideCount *= decay
    bidSideAdd *= decay
    bidSideDelete *= decay
    askSideAdd *= decay
    askSideDelete *= decay
    bidSideTrade *= decay
    askSideTrade *= decay
    bidSideFlip *= decay
    askSideFlip *= decay

  book_delta = pd.Series(book_delta)
  feature_name = f'book_delta-{ratio}-{level}-{interval}'
  features.update({feature_name: book_delta})

############################################################################
orderbook_filename = '2024-05-01-upbit-BTC-book.csv'
trade_filename = '2024-05-01-upbit-BTC-trade.csv'
features_filename = '2024-05-01-upbit-BTC-feature.csv'

level = 15
############################################################################

orderbook_df = pd.read_csv(orderbook_filename)
trade_df = pd.read_csv(trade_filename)
features = dict()

orderbook_df['timestamp'] = pd.to_datetime(orderbook_df['timestamp'])
trade_df['timestamp'] = pd.to_datetime(trade_df['timestamp'])

calc_mid_price()
calc_book_imbalance(0.2, 1)
calc_book_delta(0.2, 1)

orderbook_top = orderbook_df[::(2*level)].reset_index()
result_df = pd.concat([orderbook_top['timestamp'], pd.DataFrame.from_dict(features)], axis=1)
result_df.to_csv(features_filename, index=False)
