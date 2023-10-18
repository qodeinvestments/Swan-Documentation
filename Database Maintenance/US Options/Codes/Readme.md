### UNDERSTANDING THE CODES

Here, all the codes used for creating the US Options Database is present. </br>

The code used for creating Daily Continuous Contracts is [here](https://github.com/qodeinvestments/Swan-Documentation/blob/main/Database%20Maintenance/US%20Options/Codes/DailyExpiryContracts.ipynb). 
1. The logic behind the code was to extract the *Expiry Date* from the *Ticker* of the contract, calculate the **day difference** between the *Date of the Contract* and *Expiry Date*, keeping in mind the holidays and weekends. 
2. The first block of the code is used for creating the holiday list according to US trading days. We need the holiday list as we need to check for the difference between trading days and not calendar days.
3. Once we have the holiday list, we run the second block of code on the raw file, obtained using the Polygon API. This creates the **DaysToExpiry** column and appends it to our main dataframe. We then group them by the same column and create contracts based on the *Difference in the Days*.
