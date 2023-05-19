# BANKNIFTY LONG:
Objective:  Since Banknifty is trend following we want to buy and hold Banknifty – But it makes sense to do the same thing using the Underlying Option, The risk to rewards are better since we have weekly options. So the underlying trend needs to be in the Index and based on that move we buy the underlying Option. 

### Optimization 2: 07.02.2023

File Path: Maxpos 2: Systems/Finnifty_Long/Optimizations/All_long_options_Combined_Curves_pos_2.xlsx
           Maxpos 1: Systems/Finnifty_Long/Optimizations/All_long_options_Combined_Curves_pos_1.1.xlsx
           
           
           
07.02.2023: Equity Curves Built for Banknifty: We backtested 8 curves with different times:

![image](https://user-images.githubusercontent.com/67407393/217749191-991842cf-84d6-48d4-a113-77da398b43bf.png)


![image](https://user-images.githubusercontent.com/67407393/217749226-51c09f19-574d-43bf-9631-7e974a3f3ea9.png)

//////07.02.2023 Testing ends here///////

### Optimization 1: 

## Bankniftylong
Banknifty Long system is an options trading strategy that buys Calls/Put Options as based on two Indicators:
1.	Option Premium as an Indicator or
2.	Banknifty Index as an Indicator
Both system versions have been tested and Optimized and results saved.

## BNL V.1.1:
Entry Signals: Based on Move in Premium

Exit Signals: Next Day Morning Optimized Time

Position Sizing: % of Equity : Between 1-4% of Equity Based on Capacity

Expiry Symbol: Trading all Same Expiries

## BNL V.2.1:
Entry Signals:Based on Move in Banknifty

Expiry Symbol: Trading all Same Expiries

## BNL V.2.2:
Entry Signals:Based on Move in Banknifty

Expiry Symbol: Trading Next Expiry on Expiry Days(Thursdays)

The system involves buying the ATM Option based on movement in the index or the premium. For example: If the ATM option moves 40% in the day, we will buy the option and hold on till the next day. It could calls or puts.
this is a test.

### Optimisation - 19-05-2023

#### Vix Entry Filter:
- Entry only if vix at open has fallen from previous day vix.
- - Reduced Drawdown and increased CAR/MDD.
- - Entry at 093000, exit at 091500.
- - Optimised for vix difference of -1 to 1, at a step of 0.5 .


