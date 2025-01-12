
## NSE500 ATRRanking Review: 24.11.2022

### Introduction:-

This is the review for the NSE500 VolRanking System.The purpose for this review is that we want to compare the Live results with the Backtest
Results. A system like NSE500 has a lot of moving parts due to which there can be several discrepancies between the Live and Backtest results,therefore the
review of such a system is necessary.Following process has been adopted for the review.

Period: 05.04.2022 - 24.11.2022
Slippage: Matched with Live(take same for BT)
Reference: Dropbox---> KavanRanking ---> Rankingsheets with Date(Rankingdate is one day prior to trade_Date)
Excludeok Code: From 02-03-2022 onwards: [Excludeok](https://github.com/qodeinvestments/Swan-Documentation/blob/main/Systems/NSE500/nse500_excludeok_24.11.2022)
Entry Time: 09.20

### Process: - 

Step1: Generate Backtest ranking and then hit the backtest.  

Step2: Match Trades: Match trade count and unique symbols for each day

### Review:

1. Only 76% trades are matching because of the Excludeok List(Stocks banned from trading by the broker): 

3. Re-done Analysis by Excluding those stocks that are in the ban list: Matching Trades: 86% 

![image](https://user-images.githubusercontent.com/67407393/209300085-cfe2166d-1748-4b8b-ad1f-70cbfcbd9321.png)

### Conclusion:
More or less the trades are matching, this year the system has not performed very well compared to the previous years. 
Reasons for bad results:
1. Curve Fitting
2. Extremely Thin Trading Edge
3. Trend Analysis shows a smile: So stocks on an overall basis are rising up by end of day 
4. 2017 was one of the years when the results were not so good, but otherwise
5. This system could have done the worst because of the last 09.20 entry time


Entire Backtest since Inception: Without Excludeok: Volume Ranking


![image](https://user-images.githubusercontent.com/67407393/209300944-ac2fb0dc-68f7-48a7-964b-15fb9ace1011.png)

### Trading Edge Year Wise:

![image](https://user-images.githubusercontent.com/67407393/209301071-2d8c3757-2dfc-4bc8-bc16-3a65a9441bcf.png)
