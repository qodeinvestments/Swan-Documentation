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

Trade Delay = 0.
## 1. Long and Short optimisation - 
- Tested going long and short the ATM option.
- Long had negative to flat results, Short had positive results.

## 2. PSAR Acceleration - 
- Optimized PSAR acc from 0.01 - 0.03, with an interval of 0.01.
- We observed that for short, more volatile stock performed better at lower PSAR.
- We chose to go ahead with 0.01 - 0.03 acc.

## 2. TimeFrame - 
- Optimised for 5min, 15min, 30min, 1Hour.
- 15min had optimal results, went ahead with it for further tests.

## 3. Leverage -
- Tested on 1x and 3x leverage.

## 4. Hedging with Banknifty -
- To protect from overnight risks, we tested going LONG on ATM Call option of BankNifty if PSAR is above Underlying or ATM Put option of the same stock if PSAR is Below Underlying. and squaring it off at open.
- This did not add any value.

## 4. Hedging with OTM option -
- To protect from gap risks,  we tested going LONG on 10 Delta Call option of the same stock if PSAR is above Underlying or Put option of the same stock if PSAR is Below Underlying.
- To maintain Short upside, short position sizing was increased by a multiple. This multiple was calculated by = (Short_Premium + Long_Premium)/ Short_Premium
- Long position sizing will be the same as original short position size or (Short_premium/Multiple).
- An average multiple of 1.2 was used.
- This did not add any value.

## Optimisations with TradeDelay=1 :
### - Premium Filter
- Premium should be greater than some percent of the equity.
- Percent range - 0.1% - 1% with a step of 0.1%
### - Position Sizing - Min Premium, No of Shares 

### - Leverage - 1x to 5x


## Position Sizing -
### 1. Minimum Premium
- Here, to calculate the minimum premium, Margin per Share is calculated by dividing the Underlying equity value by the leverage.
- Margin per share is multiplied with the percent of equity to get the minimum premium.
- - If total premium of the position on that day is greater than the minimum premium, number of shares logic is used to calculate the quantity (stated below).
- - If total premium of the position on that day is lesser than the minimum premium, to get the new units, a ratio is calculated -                  
     Minimum_Premium*percent_of_equity/Total_Premium.
- This ratio is then multiplied with the number of units to get the final quantitity.
- The main aim here is to not open exorbitantly high quantities for positions with very low premium.
- Reduced Drawdown as well as returns.

![image](https://user-images.githubusercontent.com/111041920/235865623-88df411d-1fcd-4c51-80ba-6b45d4f821a5.png)

Significant difference is visible above as position size was smaller

 
### 2. Number of Shares
- Here, the available equity is multiplied with the leverage to get the Total available margin. We then divide this margin with the underlying equity value to get the Number of shares. 
- No. of shares, 3x lev

![image](https://user-images.githubusercontent.com/111041920/235865287-0a83da60-cf32-4cb4-8e64-bfceaab5a242.png)








# PSAR on the Underlying equity - 
- This backtest was originally performed on the underlying equity, where the equity was bought and sold depending on the PSAR.
#### - Optimisations 
- This was optimised for 5min,15min,1hour and daily time frame and PSAR acc from 0.01 to 0.05 at an interval of 0.01.
- Results - [PSAR on Equity](https://github.com/qodeinvestments/Swan-Documentation/tree/main/Systems/PSAR%20Stock%20Options/results/PSAR%20on%20equity)
- Chose 15min and 0.02 ACC for further testing on Options.

