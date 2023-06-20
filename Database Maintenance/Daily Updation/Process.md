# Process followed to create the stock options database
## Process
1. From GDFL raw file, we created symbolwise csv files for each symbol.
2. These files had these columns  
   Ticker  
   Date  
   Time  
   Open  
   High  
   Low  
   Close  
   Volume  
   Open Interest  
   New_date  
   symbol
3. From this file we created labelled data, in which we split the Ticker column and created new columns like Strike, Option_Type, Expiry_Date, etc.
4. This labelled data was then converted into continuous contracts (Monthly I, Monthly II, Monthly III) by taking the difference between current date month number and expiry date month number.
5. We maintained one expiry sheet to get the expiry dates for the current month expiry, next month expiry and far month expiry.
6. We maintained 3 different sheets for split/bonus, rights issue and dividend ajdustments to be performed.
7. We performed all these adjustments on the stocks which had the corporation actions. In some stocks, strike prices were in decimal numbers. In that case, rounded those strikes to the nearest tick size i.e. 0.05. Also we cross checked if these strikes match with new NSE strikes.
8. For all the stock we generated 1-min EQ data from e-signal.
## Checks
1. We checked number of rows for each stock in the raw files and in the final symbol files. In some symbols they were not matching because...  
   a. Symbols had duplicates in the final files.  
   b. Symbols had '15:30:59' timestamp data point in the raw file but in the final file it was till '15:29:59'.  
2. We checked sum of close column for each stock in the raw files and in the final symbol files.
3. We performed all the corporate actions(except for dividend) on the stocks which had them. In some stocks GDFL had not adjusted the strike after the corporate action had happened. So we corrected that as well.
4. In some stocks EQ data from e-signal was not generated for all the dates. So force backfilled the data for all those stocks.
5. We generated 5-95 delta strikes based on the greeks of the options EOD data. We compared this 50 delta (At The Money) strike with EQ close and check how much difference is there between prices. If the difference is huge it means theres some issue with the data.
6. We also checked dates which were there in the options data of a symbol but not there in the EQ data.  
## Adjustments
### A. BONUS
1. Ex. Bonus = 3:7
2. **Adjustment Factor** = (A+B)/B  
                         = 10/7
3. *New Strike Price = Old Strike Price / Adjustment Factor
4. Option Price = Old Price / Adjustment Factor
5. Lot Size = Old Lot Size * Adjustment Factor
6. Volume = Old Volume * Adjustment Factor


*If the revised strike prices after adjustment appear in decimal places, the strike shall be rounded off to the nearest tick size.i.e 0.05
### B. SPLITS
1. Face value splits from Rs.10/- to Rs.5/-.
2. **Adjustment Factor** = A/B  
                                                    = 10/5
3. *New Strike Price = Old Strike Price / Adjustment Factor
4. Option Price = Old Price / Adjustment Factor
5. Lot Size = Old Lot Size * Adjustment Factor
6. Volume = Old Volume * Adjustment Factor


*If the revised strike prices after adjustment appear in decimal places, the strike shall be rounded off to the nearest tick size.i.e 0.05
### C. RIGHTS ISSUE
1. Rights 5:21 @Premium Rs 3/- and F.V. is 2
2. A = 5  
   B = 21  
   C = 3  
   D = 2  
   X = Old Strike Price  
3. **Adjustment Factor** = ((B * X) + A * (C + D))/(A + B)
4. *New Strike Price = Old Strike Price / Adjustment Factor
5. Option Price = Old Price / Adjustment Factor
6. Lot Size = Old Lot Size * Adjustment Factor
7. Volume = Old Volume * Adjustment Factor


*If the revised strike prices after adjustment appear in decimal places, the strike shall be rounded off to the nearest tick size.i.e 0.05
### D. DIVIDENDS
1. For dividends, we need to check the difference between the strike prices on the day of adjustment and one day prior in the NSE bhavcopy.
2. If the difference matches with the dividend amount, then we will have to adjust the dividend for that stock.
