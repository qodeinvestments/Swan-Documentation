### New approach to group companies based on thier Market Capitalization.
We used to group companies based on conventional grouping ranges of Market Cap like,  
  
**Large cap** companies => above 20k Cr  
**Mid cap** companies => 5000 to 20k Cr  
**Small cap** companies => 500 to 5000 Cr  
**Micro cap** companies => below 500 Cr  

  
But the marketcap range is not inflation adjusted.  
The above ranges could have been true for recent years. But as years pass on, the range of the groups also changes.  

_i.e. a company of marketcap 5000 Cr could be a largecap company in the year 2005, but a company of the same marketcap may not fall under largecap category in the year 2023._  

So, we tried to find a way to group companies based on Marketcap without fixing to the above ranges.  

### Approach :  
The companies which contribute the most marketcap could be Largecap companies.  
The companies contributing the second most marketcap could be Midcap companies, and so on...  
  
But we need to fix on how much contribution of the total marketcap shall they be put in that category.  

So, we fixed some numbers via subjectiveness :  

**Largecap** => contributes to 80% of total marketcap.  
**Smallcap** => contributes to 15% of total marketcap.  
**Midcap** => contributes to 03% of total marketcap.  
**Microcap** => contributes to remaining 02% of total marketcap.  

### Steps to get the Marketcap Ranges :  
  

1. Get the Fundamental data for all companies for a given year.  
2. Arrange the companies from **highest to lowest marketcap**.  
3. Remove companies with _Market cap = 0_ or _null_.  
4. Find the summation of marketcaps of all the companies in that year.  
5. Select the top companies, who's summation of Marketcap contribute to **80%** of the Total Marketcap. These will be considered as our **Largecap Stocks**.
6. Select next top companies, who's summation of Marketcap contribute to **15%** of the Total Marketcap. These will be considered as our **Midcap Stocks**.
7. Select next top companies, who's summation of Marketcap contribute to **3%** of the Total Marketcap. These will be considered as our **Smallcap Stocks**.
8. Select next top companies, who's summation of Marketcap contribute to remaining **2%** of the Total Marketcap. These will be considered as our **Microcap Stocks**.  


We can see that over the years, the Marketcap range shifts for every group.  
  
  


### Below is the comparison of marketcap range of different groups YOY :  
  

<img src="https://github.com/qodeinvestments/Swan-Documentation/blob/main/Systems/QGF/Backtest_Code/Python%20Codes/pngs/New%20Market%20cap%20change%20yearly.PNG" width="1000"> 
