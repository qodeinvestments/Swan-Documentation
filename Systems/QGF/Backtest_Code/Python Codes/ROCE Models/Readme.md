**This a Model to do a Backtest of ROCE Based strategy from the year 2004-23.**  
1) **Top 30** companies are bought for first year with the highest **ROCE (Return on Capital Employed)**.  
2) We hold the companies which repeat in the next year's list, and sell rest all.
3) Top 30 companies are found in the next year, and new comapnies are bought from the capital after selling the non-repetative companies.
4) Similarly, such **Trade Sheet** is found for all the given  years and then combined.
5) **CAGR, Trading Edge, MDD** are found.
6) **Daily Portfolio change** is found and compared with indexes _(Nifty50, BSE500, NiftyMidcap100)_ with graphs.
7) **Monthly Table** of % Change is formed of our Portfolio.
8) All the above Tradesheets and Trade Reports are exported as an Excel file at the given location.


## Explaing the code :

### 1. Importing Libraries
We import the following libraries :  
pandas, numpy, warnings, math, matplotlib.pyplot, datetime & tqdm.  
  
<img src="https://github.com/qodeinvestments/Swan-Documentation/blob/main/Systems/QGF/Backtest_Code/Python%20Codes/pngs/import.PNG" width = "800">


### 2. Reading Files :  
We will be reading 2 main files :  
1) **Fundamental File (fdf)** : _This csv file contains all the fundamental data for al the companies from the year 2001 to 2022. We use this file to extarct companies based on their fundamental data._
2) **Close Prices (cdf)** : _This csv files contains daily closing prices for all the companies from the year 2001 to 2023. We use this file to get close prices for companies and the daily portfolio value._

**f_path** contains the location of the fundamental file.  
**c_path** contains the location of the Close Prices.  
  
<img src="https://github.com/qodeinvestments/Swan-Documentation/blob/main/Systems/QGF/Backtest_Code/Python%20Codes/pngs/read%20files.PNG" width = "800">  
  
### 3. Input data before starting :  
1) **start_year** _(YYYY)_ : This will be considered as the start year and will start selecting companies based of the fundaemntals from this year.  
2) **buy_date** _(YYYY-MM-DD)_ : On what date do you need to buy the stocks. Based on this date all the backtets will run, i.e. this will be buying date for all years.
3) **sell_date** _(YYYY-MM-DD)_ : On what date do you need to sell the stocks. Based on this date all the backtets will run, i.e. this will be selling date for all years.
4) **indicator** _("ROCE" or "ROE")_ : What indicator shall to look at. Now only **ROCE** & **ROE**.
5) **m**, **n** _(integer)_ : To select Top m to n companies i.e. for ge. Top 0 to 30 companies or 0 to 10 companies, etc.
6) **transaction_cost** _(float)_: Percentage of Transaction cost eg.input 1 if you want 1% ; here 0.1 => 0.1%  
7) **capital** _(integer)_ : Initial capital to start the Backtest with.
8) **weightage** _("equal" / "marketcap" / "rank")_ : Do you want to divide the capital equally / based on marketcap or based on the Rank of the companies.
9) **a**, **b** _(integer)_ : Marketcap range between a & b. i.e. a=500, b=20000 implies that **500 <= Marketcap <= 20000**.
10) **position_sizing** _("sell_all_stocks" / "hold_stocks")_ :  
**_"sell_all_stocks"_** : Sell all stocks every year and buy again if repeated in next year.  
**_"hold_stocks"_** : Hold the stocks that repeat in next year
  
<img src="https://github.com/qodeinvestments/Swan-Documentation/blob/main/Systems/QGF/Backtest_Code/Python%20Codes/pngs/Input%20variables.PNG" width = "800">  
  
 
