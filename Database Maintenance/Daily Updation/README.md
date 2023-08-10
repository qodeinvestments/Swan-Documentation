# Daily Data Updation Documentation
The daily updation is divided into four parts namely uploading raw GDFL file on database, stock options updation , index options updation and equity updation. Before starting this process make sure that you have updated the corporate action sheets till the current date. We have "AllAdjustments.csv" file for split and bonus and "RightIssue.xlsx" file for right issue adjustments. These files are stored at this location "C:\Data\CsvFiles\" on the EC2 instance. In the "RightIssue.xlsx" sheet you have fill the data in "Symbol", "Ex. Date", "Purpose", "Face Value", "Rights Ratio" columns. In the "AllAdjustments.csv" sheet you have to fill the data in "Symbol", "Ex. Date", "Corporate Action" and "NSE Ratio" columns.

We also maintain one sheet "DividendAdjustments.csv" for dividends. Its stored at this location "C:\Data\CsvFiles\". In this sheet you have to fill "Symbol", "Ex. Date", "Corporate Action", "Dividend" columns.

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

### Uploading raw GDFL file on database
1. [Final.py](https://github.com/qodeinvestments/Swan-Documentation/blob/main/Database%20Maintenance/Daily%20Updation/Codes/Final.py) code will download the zip file from the dropbox folder and unzip it.
2. We rename the file as NSEFO_date and upload it to the database.

### Stock Options Updation
1. [Final.py](https://github.com/qodeinvestments/Swan-Documentation/blob/main/Database%20Maintenance/Daily%20Updation/Codes/Final.py) code will download the raw file from the database and will create symbolwise files and segregate the ticker to create a labelled data.
2. This labelled data will be split into current month, next month and far month contracts.
3. Continuous contracts will be then appended to the historical stock options data which is stored on database.
4. This same code will perform the corporate actions if the corporate actions sheets are updated.
9. By default this code will generate the stock options data for today, but if you need to generate it for a specific date you will have to search IDLE in windows and open the 'Final.py' file and change the 'date' variable.

### Index Options Updation
1. Open cmd and type this command : "cd C:\Data\Codes" and press enter.
2. Type "jupyter notebook" and press enter.
3. Open "Index_Daily_Contracts.ipynb" file and run the code.
4. Prompt will ask for an input. Enter current date.
5. [This code](https://github.com/qodeinvestments/Swan-Documentation/blob/main/Database%20Maintenance/Daily%20Updation/Codes/Index_Daily_Contracts.ipynb) is used to create daily continuous contracts for BankNifty, Nifty and FinNifty.
6. Continuous Contracts are created and stored in the standard format, example of which is - "BANKNIFTYMONTHLY-I40000CE".
7. The continuous contracts files are stored on the desktop under "Pyspark" folder.
8. Once these contracts are created, run the "Daily_Updation_Code.ipynb" code which is stored at this path "C:\Data\Codes".
9. Prompt will ask for an input. Enter 4 for updating for all the symbols. 
10. [This Code](https://github.com/qodeinvestments/Swan-Documentation/blob/main/Database%20Maintenance/Daily%20Updation/Codes/Daily_Updation_Code.ipynb) appends the contracts to the database.

### Equity Updation
1. Go to the following path : "C:\Users\Administrator\Desktop\" and run the "EquityUpdation.py" code.
1. [This code](https://github.com/qodeinvestments/Swan-Documentation/blob/main/Database%20Maintenance/Daily%20Updation/Codes/EquityUpdation.py) is used to download and unzip stock and index equity files from GDFL mail. These unzipped files are then read and segregated into symbolwise csv files. There are two different folders for stocks and indices files namely StockEQ and IndexEQ.
2. Then files in these folders will be uploaded to the EQ database.
3. If there are any adjustments in any stock those will be performed in this code only. Make sure that you have updated the data in adjustment sheets.
   


