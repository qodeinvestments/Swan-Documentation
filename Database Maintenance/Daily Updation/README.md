# Daily Data Updation Documentation
## Starting the process
1. Log in to AWS Console. Select the region as SINGAPORE and start the EC2 instance (NSE500) 
2. Change the region to MUMBAI and start the RDS database (swandatabase).
3. In your inbox check if GDFL has sent the mail with dropbox link containing F&O segment data for the current date.
4. Open the EC2 instance and go to the following path : "C:\Users\Administrator\Desktop\"
5. You will find the Final.py file. Double click on it.
6. A command prompt will open. You need to copy the dropbox link sent by GDFL on your mail and paste it here. Press enter.



![](https://github.com/qodeinvestments/Swan-Documentation/blob/35173040855f93ef8272cd6b2a283be6e7555950/Database%20Maintenance/Daily%20Updation/Start.PNG)

7. This will start the process.

## Process
The daily updation is divided into four parts namely uploading raw GDFL file on database, stock options updation , index options updation and equity updation.

### Uploading raw GDFL file on database
1. [This code](https://github.com/qodeinvestments/Swan-Documentation/blob/main/Database%20Maintenance/Daily%20Updation/Codes/Final.py) will download the zip file from the dropbox folder and unzip it.
2. We rename the file as NSEFO_date and upload it to the database.

### Stock Options Updation
1. [This code](https://github.com/qodeinvestments/Swan-Documentation/blob/main/Database%20Maintenance/Daily%20Updation/Codes/Final.py) will download the raw file from the database and will create symbolwise files and segregate the ticker to create a labelled data.
2. This labelled data will be split into current month, next month and far month contracts.
3. Continuous contracts will be then appended to the historical stock options data which is stored on database.
8. Go through the corporate actions for that day and if there are any adjustments perfrom them individually on that specific stock.
9. By default this code will generate the stock options data for today, but if need to generate it for a specific date you will have to change the 'date' variable.

### Index Options Updation
1. [This code](https://github.com/qodeinvestments/Swan-Documentation/blob/main/Database%20Maintenance/Daily%20Updation/Codes/Index_Daily_Contracts.ipynb) is used to create daily continuous contracts for BankNifty, Nifty and FinNifty.
2. The input will be a "Date".
3. Continuous Contracts are created and stored in the standard format, example of which is - "BANKNIFTYMONTHLY-I40000CE".
4. The continuous contracts files are stored on the desktop under "Pyspark" folder.
5. Once these contracts are created, we run the [Append_Code](https://github.com/qodeinvestments/Swan-Documentation/blob/main/Database%20Maintenance/Daily%20Updation/Codes/Daily_Updation_Code.ipynb) to append the contracts to the database.

### Equity Updation
1. [This code](https://github.com/qodeinvestments/Swan-Documentation/blob/main/Database%20Maintenance/Daily%20Updation/Codes/EquityUpdation.py) is used to download and unzip stock and index equity files from GDFL mail. These unzipped files are then read and segregated into symbolwise csv files. There are two different folders for stocks and indices files namely StockEQ and IndexEQ.
2. Then files in these folders will be uploaded to the EQ database.
3. If there are any adjustments in any stock those will be performed in this code only. Make sure to updated the data in adjustment sheets.



