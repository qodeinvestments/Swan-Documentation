## UNDERSTANDING REGIMES

This exercise was carried out to understand the how different regimes affect the P/L of our trading systems as we have observed during live execution and in a backtest.

## PROCESS 

1. The analysis was done on BankNifty underlying (5 minutes timeframe) from 01-01-2016 to 31-05-2023. For the same period, we also calculated the BankNifty IV.</br>
(The calculation for BankNifty IV was - **Average of Call and Put IV of At The Money option for that bar**.)</br>

2. In order to understand the trend of a market, we first classified the days as *FLAT*, *BEARISH* OR *BULLISH*. This was purely based on the open and close of the underlying data. To strongly support this using metrics, we calculated the **Slope for each day**, **Ratio of (Difference between High and Low) and (Difference between Close and Low)**, **Standard Deviation Change of Underlying with respect to BankNifty VIX**.

3. In addition to the above classification, we also identified the days as *CHOPPY* or *TRENDY* which basically helps us to know the market noise.</br>
Market noise is nothing but all of the price data that distorts the picture of the underlying trend (the intraday volatility). **R-squared value**, **Price Density**, **Efficiency Ratio** and **Swing Count**.   

4. Using the above two classifications, we came up with 6 different buckets that each day could be compartmentalized into. Namely -</br></br>
![image](https://github.com/qodeinvestments/Swan-Documentation/assets/63246619/34595cb6-1a7e-4262-a77e-82cc26feac78)

5. Equity curves of our BankNifty systems (SID, PSAR and Long ON) were taken for the analysis. Using these curves, we plotted different deciles for each of the metrics and then compared how each system behaved for that decile of the particular indicator. Using the table so formed, we came up with different threshold values as to what would be the ideal range to justify a metric, as defined in points 2 and 3.</br>

(Attached below is an example for the **R-Squared value** indicator along with the deciles to define *CHOPPY* or *TRENDY*)</br>
						
![image](https://github.com/qodeinvestments/Swan-Documentation/assets/63246619/03c85ab0-1190-4b28-b635-3be86077cade)

To understand **R-Squared** value, higher the value, it means the datapoints show very high level of correlation (which means that the markets are not choppy). Lower the value means that the datapoints show very low level of correlation or are basically deviating a lot from the mean (which means the markets are choppy).</br>
As we observe from the table above, the value of **0.3** was used as the *THRESHOLD VALUE* for defining a system as *TRENDY* or *CHOPPY* based on **R-Squared** because the average returns across all systems for value less than 0.3, was 0.05% and for those above 0.3, was 0.13%.</br></br>

(Attached below is an example for the **Standard Deviation Change of Underlying with respect to BankNifty VIX** indicator along with the deciles to define *BEARISH*, *FLAT* or *BULLISH*)</br>

![image](https://github.com/qodeinvestments/Swan-Documentation/assets/63246619/90e01e47-51b5-4db7-ab12-459e4d5bc81e)

Here we used the ranges of **Minimum value to -0.25 SD** to denote a **Bearish Market**, **-0.25 SD to 0.25 SD** to denote a **Flat Market** and **0.25 SD to Maximum value** to denote a **Bullish Market**, based on the average returns across all systems for the above mentioned ranges.

We found out that this particular indicator strongly suggested the nature of the market as it considers BankNifty VIX for each period, which would basically suggest that the data is normalized.

## CATEGORIZATION INTO BUCKETS USING DIFFERENT INPUTS

1. After creating deciles for all indicators as mentioned above, we used different indicators to classify a day as Choppy or Trendy as an input, with different threshold values.</br>

2. Using the above mentioned inputs, we calculated - </br>
a) The number of occurrences of each of those buckets for a given year/quarter/month </br>
b) Probability of type 1-6 days </br>
c) Returns </br>

3. The number of occurences of each bucket year wise, for a indicator of **R-Squared value** with a threshold of **0.3**, is given below - </br>

![image](https://github.com/qodeinvestments/Swan-Documentation/assets/63246619/3ec9ec26-12b0-4870-ac66-a61e6139667f)

   

