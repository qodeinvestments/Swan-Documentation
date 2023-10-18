## WHAT IS THIS ABOUT?
A write-up about why we used Polygon as the data source, what was the process we followed while trying to use their service, what were the challenges and issues we faced, creating the continuous contracts etc.

### WHY POLYGON?
The reason why we used *Polygon* as the data source was mainly because it was easy to get the historical strike price intervals for SPY/SPX options.
Initially, when we were using *E-Signal* to fetch option data on Amibroker, the challenge that we faced was to figure out the tickers/strikes/strike intervals going back in time as we had to manually construct the ticker to load the data on to Amibroker.
As getting the historical strikes were difficult and Polygon provides an API through which you can fetch historical strikes, we figured using Polygon would be much easier.

**SUBSCRIPTION** - In order to get access to historical option strikes/data, we subscribed to the *Options Starter* pack which provides data up to 2 years.

### PROCESS

1. Using the Polygon API, we fetch the historical tickers - **expiry wise**, dating back to 2 years. Fetching tickers expiry wise means that say a SPY contract of 450 Call option expires on the 11th Oct, 2023, the ticker for the same would look like - *O:SPY231011C00450000*.
2. We do this for fetching all the tickers till Oct, 2021.
3. Once we have the ticker list, we use the **get_aggs()** function mentioned in the API documentation to fetch Price data for the tickers so obtained from the above step.
   (We ran it a loop for different expiry months as time taken to fetch data using the API was about 1 hour 30 minutes for 1 expiry month's *1 minute* option price data)
4. Thus, we had 1 raw data file for each expiry month - totalling to 25 files since Oct'21.
5. To verify the data, we cross checked the prices for a particular strike with the data available on the Option chain. There was a slight mismatch in the data (Especially the volume) but it was so marginal that it could be neglected.

### CONTINUOUS CONTRACTS 

1. The first SPY weekly options expired only on **Friday**, with **Monday** and **Wednesday** expirations added over time. However on April, '22 and 11 May 2022 respectively, Tuesday and Thursday expirations were also added. This basically meant SPY options have **5** expirations in a week.
2. We thus created contracts with DTE = 0,1,2,3,4 and 5. This meant that for contract expiring today, it would fall in the **D0** contract, **D1** for a contract expiring tomorrow and so on.
3. The numbers were restricted to 5 as there could be a contract expiring next year too which would then fall in a **D365** contract. As that would increase the number of contracts and data redundancy (that could be termed as a Yearly contract instead), we restricted the number to **D5**.
4. Once the contracts were created and labelled in the required format, we used the data required for our backtests.
