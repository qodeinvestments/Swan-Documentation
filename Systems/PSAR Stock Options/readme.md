# PSAR
The parabolic indicator generates buy or sell signals when the position of the dots moves from one side of the asset's price to the other. For example, a buy signal occurs when the dots move from above the price to below the price, while a sell signal occurs when the dots move from below the price to above the price.

The PSAR indicator forms a parabola composed of small dots that are either above or below the trading price. When the parabola is below the stock price, it acts as a support and trail-stop area, while indicating bullish up trending price action. When the stock price falls below a single dot, then a stop-loss/sell /sell-short trigger forms.

- If PSAR is ABOVE underlying price, we go short on the ATM CALL option and if PSAR is BELOW underlying price, we go short on the ATM PUT option.

## Objective:
To capture the trend indicated by PSAR indicator.

## Backtest:
- Trade Delay = 0
- Entry Signals: PSAR
- Exit Signals: Change in direction of PSAR
### Data used:
- 1 min options data for BankNifty Constituents since inception.
- Expiry dates
- Stock weights


## Optimisations:
## 1. Long and Short optimisation - 
- Tested going long and short the ATM option.
- Long had negative to flat results, Short had positive results.

## 2. PSAR Acceleration - 
- Optimized PSAR acc from 0.01 - 0.03, with an interval of 0.01.
- We observed that for short, more volatile stock performed better at lower PSAR.
- We chose to go ahead with 0.01 - 0.03 acc.

## 2. TimeFrame - 
- Optimised for 5min, 15min, 30min, 1Hour.
- 15min had optimal results.

## 3. Leverage -
- Tested on 1x and 3x leverage.

## 4. Hedging with Banknifty -
- To protect from overnight risks, we tested going LONG on Banknifty ATM CE or PE (Depending on PSAR at EOD) at EOD and squaring it off at open.
-

## 4. Hedging with OTM option -
- To protect from gap risks,  we tested going LONG on 10 Delta CE if PSAR is above Underlying or PE if PSAR is Below Underlying.
- To maintain Short upside, short position sizing was increased by a multiple. This multiple was calculated by = (Short_Premium + Long_Premium)/ Short_Premium
- Long position sizing will be the same as original short position size or (Short_premium/Multiple).

## Position Sizing -
### 1. Minimum Premium
- Here, to calculate the minimum premium, Margin per Share is calculated by dividing the Underlying equity value by the leverage.
- Margin per share is multiplied with the percent of equity to get the minimum premium.
- - If total premium of the position on that day is greater than the minimum premium, number of shares logic is used to calculate the quantity (stated below).
- - If total premium of the position on that day is lesser than the minimum premium, to get the new units, a ratio is calculated -                  
     Minimum_Premium*percent_of_equity/Total_Premium.
- This ratio is then multiplied with the number of units to get the final quantitity.
- The main aim here is to not open exorbitantly high quantities for positions with very low premium.

 
### 2. Number of Shares
- Here, the available equity is multiplied with the leverage to get the Total available margin. We then divide this margin with the underlying equity value to get the Number of shares. 




