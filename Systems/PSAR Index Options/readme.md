## PSAR Index Options
Updation Date: 30.11.2022 ( This was the last day of the Analysis)
Done By: Rishabh Nahar

The PSAR indicator forms a parabola composed of small dots that are either above or below the trading price. When the parabola is below the stock price, it acts as a support and trail-stop area, while indicating bullish up trending price action. When the stock price falls below a single dot, then a stop-loss/sell /sell-short trigger forms.
The parabolic indicator generates buy or sell signals when the position of the dots moves from one side of the asset's price to the other.

- If PSAR is ABOVE underlying price, we go short on the ATM CALL option and if PSAR is BELOW underlying price, we go short on the ATM PUT option.
- This Backtest was initially performed on equity.

### Objective:
To capture the trend indicated by PSAR indicator on the Underlying Indices.Study was done on 
i)   Banknifty Weekly and Monthly
ii)  Nifty Weekly
iii) Nifty Monthly




### i) Banknifty Weekly:



###### Backtest

A simple Backtest taking 0.015 acc is shown below. Major returns are made in 2020 where VIX was very high.

CAR: 22.40%
MDD: -9.69%
TE:  8.54%

![image](https://user-images.githubusercontent.com/67407393/236745165-ec368aa9-4672-444d-a501-9b2e92968d1d.png)


#### Optimizations:

Database: Taken from SQL: Banknifty Weekly Data 
Time Period: 01-06-2016 to 30-11-2022

As shown in image, we have taken In sample and out sample periods for the analysis. Acceleration factor of 0.005 to 0.04.

![image](https://user-images.githubusercontent.com/67407393/236748167-885b656d-f8d8-4020-b579-5ce429ac0b6b.png)


On Expiry day we tried running two variations: 

i) Trading next expiry on expiry day and 

ii) Trading next expiry on expiry evening only post 3.20(Change the position and buy a new position)

![image](https://user-images.githubusercontent.com/67407393/236747401-9caf4733-08b4-4c80-893a-cd4e27e8793b.png)


##### Combined 5 Equity Curves:

![image](https://user-images.githubusercontent.com/67407393/236748370-894a0332-c336-4345-ac47-7c6964cc3125.png)

###### Conclusion from Optimizations:

As the Acceleration increases the number of trades go up. Due to which the Trading edge is reducing. It might be better to trade faster but due to Transaction costs
the system can fail. Hence we decided to run 5 systems ranging from 0.005 to 0.025 with a step of 0.005. We have combined the equity curves and see the result as shown in the table above. 

Finally we concluded to run next expiry only after 3.20 in the evening. Since we want the option to expire worthless. We combined all the Equity curves 5 different curves.The result is in the table above. 

### ii) Nifty Weekly:


Database: Taken from SQL: Banknifty Weekly Data 
Time Period: 11-02-2019 to 30-11-2022

Short Weekly Options						

![image](https://user-images.githubusercontent.com/67407393/236762562-4b52ba86-66b7-471f-abe3-c2a2bb75499f.png)


###### Conclusion:

We ran similar optimizations for Nifty like for BN. THe only difference here was that the data is for a very tiny period. So we should not be taking the results with much seriousness. Overall the system seems to be working.

After this we combined both Nifty Weekly and BankNifty Weekly 5 curves each to check the results:

![image](https://user-images.githubusercontent.com/67407393/236764069-90cba61f-14e0-414f-84ac-530001bfd062.png)

### iii) Nifty Monthly:

##### Backtest:

Simple Backtest:

Time Period: 04-01-2011 to 30-11-2022
ACC: 0.015
 
![image](https://user-images.githubusercontent.com/67407393/236765345-88728bd6-ad82-4b4f-909a-9537e45c12e5.png)
![image](https://user-images.githubusercontent.com/67407393/236765152-dc7c650a-098a-454a-ab6c-298251c664b6.png)


##### Optimizations:

We ran optimizations for the whole period as well as the period comparing it to the Weekly Data from 2019-22. We also combined all the Equity Curves
to check the results. All the results are in the table below. 

![image](https://user-images.githubusercontent.com/67407393/236765834-cb5b878c-58d8-4532-af92-a8e519f9e7fa.png)

