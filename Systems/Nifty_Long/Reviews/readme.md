### Nifty Long Options Weekly Overnight Review: 
### Review #1: 01.04.2022-24.11.2022
### Next Review: 01.04.2023


#### Introduction:
This is the Review for the Nifty Long Options Weekly Overnight System.Unlike its peer's,NSE500 and ShortIntraday Futures,the complexity of 
this system is very low in terms of execution,variable optimization and ease of running a system,Its worthwhile to conduct a review keeping in mind that 
the position sizing of this strategy together with BankNifty Long Options Weekly Overnight is currently 1% of the Entire Portfolio. As the Portfolio value increases,
this strategy is pivotal as it can cause the P/L to swing drastically.

Process:
1) We have a list of all the live trades from the SOS PropBook on DropBox.The variables optimized in this system are the EntryTime,% Gap and Exit Time.
All the prior optimizations for this system can be found in Nifty Long Options Weekly Overnight Documnetation on GitHub.The review for this system starts from
01/04/2022 to 24/11/2022. From 01/04/2022 to 09/05/2022, only one variation of this system was being run. The EntryTime was greater than 100000am, Exit was 092000am the
next trading day with ExitTime being 151500 in case of Expiry Days, Gap was 40% from Day's Open and Max Open positions were 2.From 10/05/2022, another variation was added where on 
only on expiry days,trades for the next expiry were opened. The variables for both these systems can be found in 'Nifty Live Trades' Tab in the 'Nifty Long Options Overnight Review - 01042022 to 24112022' 
spreadsheet.

2)The purpose of any review is to check firstly if the trades taken live and the trades in the backtest are same or no and secondly to check how the system has performed during the
review period and compare it to its historical performance.To do the former, we have hit a backtest with the variables prescribed in 1).Two Backtests have been done. One on a 1 Min
Time frame and the other on a 5Min Time Timeframe. The reason for this is because,in our database we have current expiry symbols as well as next expiry symbols. The current week expiry
symbols have 1 Min data while the next expiry symbols have 5min as their base interval.In the 1 Min backtest, only the Current expiry symbols have been backtested on 1 Min while the next 
expiry symbols have been backtested on a 5min timeframe.In the 5 Min backtest, both have been backtested on 5 Mins. After doing a backtest, we compare firstly whether 
the number of trades are same in backtest and live and secondly whether the same trades are placed in live and in the backtest.While doing a trade count, the number of trades taken in live
are 332 and in the backtest they are around 330.So the number of trades taken are the same. From 01/04/2022 to 24/11/2022 there have been 129 trading dates out of which on 115 trading days, 
the number of trades taken match. We then compare if the trades taken everyday are the same.Out of 332 trades taken live, 95.18% of those trades appeared in the Backtest.This is when the
backtest is done on 1Min time frame. When backtest is done on a 5Min time frame, only 84.34% of the trades match.

3)We then compared the actual performance of the Live with the backtest trades. While trading live, the position size had changed many times from being run at 1% of The Options Portfolio Amount,
to 0.5% of the total Portfolio amount to currently being run at 1% of the Total Portfolio. Due to these changes in position sizing, its tough to know what return the strategy has done as a percentage.
For Nifty, we basically run two systems. The varaibles for both these systems can be found in the Execution documentataion for Nifty Weekly Long Overnighton GitHub.
Therefore, we take the live trades, which is a culmination of both these systems and take the actual Entry and Exit Prices and run 0.5% position size per trade from 01/04/2022 till 09/05/2022.From there
onwards the position size is reduced to 0.25%. Reason being is till 09/05/2022, we ran only one variation which had a Max Position of 2 and from 10/05/2022 onwards two variations were being run which
had a max positions of 4 (2 systems with a Max Position of 2 each).The XIRR for the review period is 12.124% with a MaxDD of 3.35% which gives a CAR/MDD of 3.61.
     
4)We finally run a historical backtest from 11/02/2019 to 24/11/2022. The variables used for the backtest are the same that were used in live.Similarly, two variations are backtested and are combined 
in a similar way as described in 3) because on Amibroker, both the strategies could not be backtested simultaneously.Therefor the position sizing part was done on Excel.As we can see, since incepition,
the strategy has generated 10.78% with a MaxDD of 4.91% giving a CAR/MDD of 2.19. The backtested results for the period 01/04/2022 to 24/11/2022 are 3.52% with a MaxDD of 3.43% which gives a CAR/MDD of 1.02.
The drastic difference in the results are due to the fact that in live, we execute on a 1 Min time frame and in the backtest we are using a 5Min time frame.This can cause a difference in the number of trades which 
we can see in 2). In Long Options, its very normal for trades to become 5x and above. If one of these trades are missed out in the backtest, it can make a lot of difference to the results.

Conclusion:
The next review will be conducted in April 2023.   
