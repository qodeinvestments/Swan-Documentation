################################################################################
################################################################################
                        ## STOCK OPTIONS UPDAATION ##
import os
from os import walk
import pandas as pd
import psycopg2
import time
from io import StringIO
from datetime import datetime
import pyspark
#from datetime import date
from tqdm.notebook import tqdm
import calendar
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql import Row
from datetime import timedelta
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import array_contains
from pyspark.sql.functions import date_format
import re
import warnings
warnings.filterwarnings('ignore')


# In[3]:


def Sort_Tuple(tup): 
    tup.sort(key = lambda x: x[1]) 
    return tup
#path_list = Sort_Tuple(path_list)

def my_split(s):
    return list(filter(None, re.split(r'(\d+)', s)))
#print(path_list)
#print(len(path_list))
def get_symbol(tic):
    li = list(filter(None, re.split(r'(\d+)', tic)))
    return li[0]


## Read data from database and convert it into pandas dataframe
## This snippet will create symbolewise csv files

############################################## INPUTS #####################################################
####startDate = date(2023,4,20)
####endDate = date(2023,4,20)
#startDate = endDate = date.today()
startDate = endDate = date

total_data_combined = 0
ignore_symbols = ['NIFTY', 'BANKNIFTY', 'FINNIFTY']
numberOfRowsRaw = 0
start_time = datetime.strptime('09:15:00', '%H:%M:%S').time()
end_time = datetime.strptime('15:30:00', '%H:%M:%S').time()
###########################################################################################################
print(startDate)


# In[7]:


print(datetime.now())

# Start spark session
spark = SparkSession.builder.config("spark.jars", "C:\\Users\\Administrator\\Downloads\\postgresql-42.5.1.jar") \
        .master("local").appName("PySpark_Postgres_test").getOrCreate()
print(spark)

# Remove previous day files
new_data_path = r"C:\Data\OptionsDatabaseDailyUpdate\Updation_Temp_Data\\"
log_data_path = r"C:\Data\OptionsDatabaseDailyUpdate\Updation_Log_Data\\"

for file in next(os.walk(new_data_path))[2]:
    os.remove(new_data_path + file)    

for file in next(os.walk(log_data_path))[2]:
    os.remove(log_data_path + file)
    
misc_data_path = r"C:\Data\OptionsDatabaseDailyUpdate\Updation_Temp_Data\misc_data_afterstockwise\\"
log_misc_data_path = r"C:\Data\OptionsDatabaseDailyUpdate\Updation_Log_Data\misc_data_afterstockwise\\"
for file in next(os.walk(misc_data_path))[2]:
    os.remove(log_data_path + file)
    
for file in next(os.walk(log_misc_data_path))[2]:
    os.remove(log_data_path + file)

# Loop through all the dates to create symbolwise files
for n in tqdm(range(int((endDate - startDate).days)+1)):
    currentDate = startDate + timedelta(n)
        
    day = currentDate.strftime('%d')
    nummonth=currentDate.strftime("%m")
    year=currentDate.strftime('%Y')
    tablename="r"+str(day)+str(nummonth)+str(year)
    #print(tablename)

    st=time.time()

    # Check if file is available in the database
    conn = psycopg2.connect(database="RawDataBase",
                        user='postgres', password='swancap123',
                        host='swandatabase.cfehmk2wtejq.ap-south-1.rds.amazonaws.com', port='5432')
                        
    cursor = conn.cursor()
    stmt = '''Select 1 from rawinfo where name=\'''' + tablename + '''\';'''
    cursor.execute(stmt)
    result = cursor.fetchone()
    
    if result:
        print(tablename)
    else:
        continue
    
    
    df = spark.read.format("jdbc").option("url", "jdbc:postgresql://swandatabase.cfehmk2wtejq.ap-south-1.rds.amazonaws.com/RawDataBase").option("user","postgres").option("password","swancap123")\
        .option("driver", "org.postgresql.Driver").option("dbtable", tablename)\
        .option("user", "postgres").option("password", "swancap123").load()

    # GETTING ONLY TIME IN TIME COLUMN
    q = df.withColumn('Time',date_format('Time', 'HH:mm:ss'))

    # FILTERING OUT INDEX DATA
    bndata = q.filter((~q.ticker.contains('NIFTY')))
    bndata = bndata.filter(bndata.ticker.endswith('E.NFO') | bndata.ticker.endswith('E'))

    # CONVERTING PYSPARK DATAFRAME TO PANDAS DATAFRAME
    bndata=bndata.toPandas()

    data = bndata.copy()
    data.drop(data.filter(regex="Unname"),axis=1, inplace=True)
    
    data = data.rename(columns={'Ticker ' : 'Ticker',
                                'ticker' : 'Ticker',
                                'date' : 'Date',
                                'time' : 'Time',
                                'open' : 'Open',
                                'high' : 'High',
                                'low' : 'Low',
                                'close' : 'Close',
                                'volume' : 'Volume',
                                'open interest' : 'Open Interest'})
    
    data["Ticker"] = data["Ticker"].str.replace("HINDALC0","HINDALCO")
    data["Ticker"] = data["Ticker"].str.replace("IBN18","IBN_ET")
    data["Ticker"] = data["Ticker"].str.replace("TV-18","TV_ET")
    data["Ticker"] = data["Ticker"].str.replace("NETWORK18","NETWORK_ET")
    data["Ticker"] = data["Ticker"].str.replace("TV18BRDCST","TV_ETBRDCST")
    data["Ticker"] = data["Ticker"].str.replace("3IINFOTECH","TH_IINFOTECH")
    data["Ticker"] = data["Ticker"].str.replace("BAJAJ-AUTO","BAJAJ_AUTO")
    data["New_date"] = currentDate
    data['Date'] = currentDate
    print(currentDate)
    print("Total Data Size "+ str(data.shape[0]))
    
    data["symbol"] = data["Ticker"].str.split("(\d+)").str[0]
    
    fut_data = data[data["Ticker"].str.endswith(("I","II","III", 'I.NFO', 'II.NFO', 'III.NFO'))] 
    data = data[~data["Ticker"].str.endswith(("I","II","III"))]
    misc_data = data[~data["Ticker"].str.endswith(('.NFO', 'CE', 'PE'))]
    data = data[data["Ticker"].str.endswith(("PE","CE","PE.NFO","CE.NFO"))]
    
    
    data = data[~data['symbol'].isin(ignore_symbols)]
    data['Time'] = pd.to_datetime(data['Time']).dt.time
    data = data[(data['Time']>=start_time) & (data['Time']<=end_time)]
    
    numberOfRowsRaw += data.shape[0]
    total_data_combined = total_data_combined + data.shape[0]
       
    gb = data.groupby(["symbol"])
    df_list = [gb.get_group(x) for x in gb.groups]
       
    for tup in tqdm(gb.groups):
        temp = gb.get_group(tup)
        
        temp.to_csv(new_data_path + str(tup)  + '.csv', mode='a', header=not os.path.exists(new_data_path + str(tup)  + '.csv'), index=False)
        temp.to_csv(log_data_path + str(tup)  + '.csv', mode='a', header=not os.path.exists(log_data_path + str(tup)  + '.csv'), index=False)   

    print("Processing Finished")  
        
    gb = misc_data.groupby(["symbol"])
    df_list = [gb.get_group(x) for x in gb.groups]
       
    for tup in tqdm(gb.groups):
        temp = gb.get_group(tup)       
        
        temp.to_csv(misc_data_path + str(tup) + '.csv', mode='a', header=not os.path.exists(misc_data_path + str(tup) + '.csv'), index=False)
        temp.to_csv(log_misc_data_path + str(tup)  + '.csv', mode='a', header=not os.path.exists(log_misc_data_path + str(tup)  + '.csv'), index=False)
     
    print("Data appending finished")
    t = time.localtime()
    current_time = time.strftime("%H:%M:%S", t)
    print(current_time)
    
    print("Total Rows read from files till now " + str(total_data_combined))
    
    mypath = new_data_path
    filename = next(walk(mypath), (None, None, []))[2]  # [] if no file
    final = []
    for i in filename:
        temp = mypath +"/"+ i
        final.append(temp)
    sum1 = 0
    for i in final:
        df = pd.read_csv(i)
        sum1 += df.shape[0]
    print("Total Rows in csv files" + str(sum1))
print(datetime.now())


## Labelling
## This snippet will split Ticker into different columns like 'Option_Type', 'Strike', etc
date1 = datetime.strptime('30-12-2019', '%d-%m-%Y')
new_data_path = r"C:\Data\OptionsDatabaseDailyUpdate\Updation_Temp_Data\\"


print(datetime.now())
mypath = new_data_path
filename = next(walk(mypath), (None, None, []))[2]  # [] if no file
file_path = {}
for i in filename:
    temp = mypath +"/"+ i
    file_path[i.replace(".csv","")] = temp

labeled_data_path = r"C:\Data\OptionsDatabaseDailyUpdate\Updation_Temp_Data\labeled_data\\"
log_labeled_data_path = r"C:\Data\OptionsDatabaseDailyUpdate\Updation_Log_Data\labeled_data\\"

for i in next(os.walk(labeled_data_path))[2]:
    os.remove(labeled_data_path + i)

for i in next(os.walk(log_labeled_data_path))[2]:
    os.remove(log_labeled_data_path + i)

for sym in tqdm(sorted(file_path)):
    print(sym)
    df = pd.read_csv(file_path[sym])

    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    df = df.drop_duplicates()
    df = df.drop(columns = ["symbol"])

    #print(df['New_date'])
    df['Date'] = pd.to_datetime(df['Date'], format='mixed', dayfirst=True)
##    print(df['Date'])
##    print(df['New_date'])
    df['Ticker'] = df['Ticker'].str.replace('.NFO', '')
    df["Symbol"] = sym
    df["Option_type"] = df["Ticker"].str[-2:]
    df["Temp"] = df["Ticker"].str.replace(sym,"")        
    df["Temp"] = df["Temp"].str[:-2]
    
    df['Exp_year'] = np.where(df['Date']>=date1, df["Temp"].str[5:7], df['Temp'].str[0:2])
    #df["Exp_year"] = df["Temp"].str[5:7] # change Exp_year
    df["Exp_month"] = df["Temp"].str[2:5]
    df['Strike'] = np.where(df['Date']>=date1, df["Temp"].str[7:], df['Temp'].str[5:])
    
    df = df.rename(columns={'Time' : 'Timestamp'})
    df['New_date_Time'] = pd.to_datetime(df['New_date'] + ' ' + df['Timestamp'])
    
    #df["Strike"] = df["Temp"].str[7:] # change Strike
    df.to_csv(labeled_data_path + sym + '.csv', mode='a', header=not os.path.exists(labeled_data_path + sym + '.csv'), index=False)    
    df.to_csv(log_labeled_data_path + sym + '.csv', mode='a', header=not os.path.exists(log_labeled_data_path + sym + '.csv'), index=False)
    
    del df
    
##    for df in data:
##        df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
##        df = df.drop_duplicates()
##        df = df.drop(columns = ["symbol"])
##        
##        df['Date'] = pd.to_datetime(df['Date'], dayfirst=True)
##        #print(df)
##        df['Ticker'] = df['Ticker'].str.replace('.NFO', '')
##        df["Symbol"] = sym
##        df["Option_type"] = df["Ticker"].str[-2:]
##        df["Temp"] = df["Ticker"].str.replace(sym,"")        
##        df["Temp"] = df["Temp"].str[:-2]
##        
##        df['Exp_year'] = np.where(df['Date']>=date1, df["Temp"].str[5:7], df['Temp'].str[0:2])
##        #df["Exp_year"] = df["Temp"].str[5:7] # change Exp_year
##        df["Exp_month"] = df["Temp"].str[2:5]
##        df['Strike'] = np.where(df['Date']>=date1, df["Temp"].str[7:], df['Temp'].str[5:])
##        
##        df = df.rename(columns={'Time' : 'Timestamp'})
##        df['New_date_Time'] = pd.to_datetime(df['New_date'] + ' ' + df['Timestamp'])
##        
##        #df["Strike"] = df["Temp"].str[7:] # change Strike
##        df.to_csv(labeled_data_path + sym + '.csv', mode='a', header=not os.path.exists(labeled_data_path + sym + '.csv'), index=False)    
##        df.to_csv(log_labeled_data_path + sym + '.csv', mode='a', header=not os.path.exists(log_labeled_data_path + sym + '.csv'), index=False)
##        
##        del df
        
print(datetime.now())


## Continuous Contracts
## This snippet will create Monthly-I, Monthly-II, Monthly-III files


def add(stri):
    obj = datetime.strptime(stri, "%b")
    month_number = obj.month
    return month_number

mypath = labeled_data_path
filename = next(walk(mypath), (None, None, []))[2]  # [] if no file
file_path = {}

for i in filename:
    temp = mypath +"/"+ i
    file_path[i.replace(".csv","")] = temp

exp_file_path = r"C:\Users\Administrator\Downloads\MonthlyExpiry.csv"
exp_df = pd.read_csv(exp_file_path, usecols = ["curr_exp_date","curr_date"]).dropna()
exp_df['curr_date'] = pd.to_datetime(exp_df['curr_date'], format='mixed', dayfirst=True)
exp_df['curr_exp_date'] = pd.to_datetime(exp_df['curr_exp_date'], format='mixed', dayfirst=True)
exp_df.rename({'curr_date': 'New_date'}, axis=1, inplace=True)
print(exp_df)
print(exp_df.isnull().values.any())

folpath = r"C:\Data\OptionsDatabaseDailyUpdate\Updation_Temp_Data\cont_data\\"
folpath_log = r"C:\Data\OptionsDatabaseDailyUpdate\Updation_Log_Data\cont_data\\"
diff_list_dict = {}

print(datetime.now())

for file in next(os.walk(folpath))[2]:
    os.remove(folpath + file)

for file in next(os.walk(folpath_log))[2]:
    os.remove(folpath_log + file)
j = 0
numberOfRowsDF = pd.DataFrame()
for sym in tqdm(sorted(file_path)):
    
    print(sym)
    df = pd.read_csv(file_path[sym])
    numberOfRowsDF.loc[j, 'Symbol'] = sym
    numberOfRowsDF.loc[j, 'RawRows'] = df.shape[0]
    j += 1
    if df.empty == True:
        continue
        
    if sym == 'S&P':
        continue
        
    df['exp_month_number'] = df.apply(lambda row : add(row["Exp_month"]), axis = 1)
    df["New_date"] = pd.to_datetime(df["New_date"], format='mixed', dayfirst=True)
    df["current_month_number"] = df['New_date'].dt.month
    df["difference"] = df['exp_month_number'].astype(int) - df["current_month_number"].astype(int)
    
    df1 = pd.merge(df, exp_df, on ='New_date', how ='left')
    

    df1.drop(df1.filter(regex="Unname"),axis=1, inplace=True)
    print(df1.shape[0])
    df1["current_exp_month_number"] = df1['curr_exp_date'].dt.month
    df1["Diff_months"] = df1["current_exp_month_number"] - df1["current_month_number"]
    
    #print(df1['Diff_months'])
    if sym=='AARTIIND':
        df1.to_csv(r"C:\users\administrator\desktop\data.csv",index=False)
    df1["Diff_months"] = df1["Diff_months"].astype(int) 

    bdf = df1[df1["Diff_months"] == 0]
    adf = df1[(df1["Diff_months"] == 1) | (df1["Diff_months"] == -11)]
    
    if bdf.shape[0] + adf.shape[0] == df1.shape[0]:
        print("Sanity Check Success")
    else:
        print("Error1")
        break
    
    agb = adf.groupby(["difference"])
    unique_val_list_a = list(adf["difference"].unique())
    bgb = bdf.groupby(["difference"])
    unique_val_list_b = list(bdf["difference"].unique())

    for i in unique_val_list_b:
        temp_df = bgb.get_group(i)
        
        if i == 0:
            temp_df.to_csv(folpath + sym + '-I.csv', mode='a', header=not os.path.exists(folpath + sym + '-I.csv'), index=False)
            temp_df.to_csv(folpath_log + sym + '-I.csv', mode='a', header=not os.path.exists(folpath_log + sym + '-I.csv'), index=False) 
        
        elif i == 1 or i == -11:
            temp_df.to_csv(folpath + sym + '-II.csv', mode='a', header=not os.path.exists(folpath + sym + '-II.csv'), index=False)
            temp_df.to_csv(folpath_log + sym + '-II.csv', mode='a', header=not os.path.exists(folpath_log + sym + '-II.csv'), index=False)
        
        elif i == 2 or i == -10:
            temp_df.to_csv(folpath + sym + '-III.csv', mode='a', header=not os.path.exists(folpath + sym + '-III.csv'), index=False)
            temp_df.to_csv(folpath_log + sym + '-III.csv', mode='a', header=not os.path.exists(folpath_log + sym + '-III.csv'), index=False)

        else:
            temp_df.to_csv(folpath + sym + 'misc.csv', mode='a', header=not os.path.exists(folpath + sym + 'misc.csv'), index=False)
            temp_df.to_csv(folpath_log + sym + 'misc.csv', mode='a', header=not os.path.exists(folpath_log + sym + 'misc.csv'), index=False)
         
    for i in unique_val_list_a:
        temp_df = agb.get_group(i)
        
        if i == 1 or i == -11:
            temp_df.to_csv(folpath + sym + '-I.csv', mode='a', header=not os.path.exists(folpath + sym + '-I.csv'), index=False)
            temp_df.to_csv(folpath_log + sym + '-I.csv', mode='a', header=not os.path.exists(folpath_log + sym + '-I.csv'), index=False)
    
        elif i == 2 or i == -10:
            temp_df.to_csv(folpath + sym + '-II.csv', mode='a', header=not os.path.exists(folpath + sym + '-II.csv'), index=False)
            temp_df.to_csv(folpath_log + sym + '-II.csv', mode='a', header=not os.path.exists(folpath_log + sym + '-II.csv'), index=False)
                           
        elif i == 3 or i == -9:
            temp_df.to_csv(folpath + sym + '-III.csv', mode='a', header=not os.path.exists(folpath + sym + '-III.csv'), index=False)               
            temp_df.to_csv(folpath_log + sym + '-III.csv', mode='a', header=not os.path.exists(folpath_log + sym + '-III.csv'), index=False)               
                        
        else:
            temp_df.to_csv(folpath + sym +'misc.csv', mode='a', header=not os.path.exists(folpath + sym +'misc.csv'), index=False)
            temp_df.to_csv(folpath_log + sym + 'misc.csv', mode='a', header=not os.path.exists(folpath_log + sym + 'misc.csv'), index=False)               
            
        
    list_files = os.listdir(folpath)
    sum1 = 0
    for i in list_files:
        if i.startswith(sym):
            dff = pd.read_csv(folpath + i)
            sum1 += dff.shape[0]
    if sum1 == df1.shape[0]:
        print("Sanity check2 Success")
    else:
        print("Error2")
        break
    print(sum1)
    
print(datetime.now())


## Create Adj Columns and Append to Main Database
final_data_path = r"C:\Data\OptionsDatabaseDailyUpdate\Updation_Temp_Data\cont_data\\"
#output_path = r"E:\sourav\Database\OptionsDatabaseDailyUpdate\Updation_Temp_Data\split_adjusted_final\\"
#output_path = r"D:\Sourav\Data\UpdatedTill23Dec2022\\"
output_path = r"C:\Data\OptionsDatabaseDailyUpdate\Updation_Temp_Data\split_adjusted_final\\"
symbols = next(os.walk(final_data_path))[2]
len(symbols)


# connect to the database
conn = psycopg2.connect(database="StockOptions",
                            user='postgres', password='swancap123',
                            host='swandatabase.cfehmk2wtejq.ap-south-1.rds.amazonaws.com', port='5432')

conn.autocommit = True
cursor = conn.cursor()




#symbols = ['AARTIIND']
for i in range(len(symbols)):
    symbols[i] = symbols[i].replace('-III.csv', '').replace('-II.csv', '').replace('-I.csv', '')    
symbols = sorted(list(set(symbols)))
print(len(symbols))
##ignore_symbols = ['AARTIIND']
##print(len(symbols))
##symbols = np.setdiff1d(symbols, ignore_symbols)
print(len(symbols))


hyphens = ['I', 'II', 'III']
numberOfRowsFinal = 0
j = 0
numberOfRowsDF['FinalRows'] = 0
for symbol in tqdm(symbols):
    print(symbol)  
    sym1 = re.sub('[^a-zA-Z0-9 \n\.]', '', symbol)
    df_final = pd.DataFrame()
    
    for i in hyphens:    
        #print(i)
        file_path = final_data_path + '\\' + symbol + '-' + i + '.csv'
        
        if os.path.isfile(file_path):
            print(i)
            df = pd.read_csv(file_path, dayfirst=True)

            df['Date'] = pd.to_datetime(df['Date'].astype(str), dayfirst=True).dt.date
            df = df.drop(['Date'], axis=1)
            df = df.rename(columns={'New_date' : 'Date'})
            
            df['Adj_Open'] = df['Open']
            df['Adj_High'] = df['High']
            df['Adj_Low'] = df['Low']
            df['Adj_Close'] = df['Close']
            df['Adj_strike'] = df['Strike']
            df['Adj_volume'] = df['Volume']
            df['Adj_OI'] = df['Open Interest']

            df['Final_strike'] = df['Strike']
            df['month'] = pd.to_datetime(df['Date'], dayfirst=True).dt.month    
            
            df = df.sort_values(by=['Date', 'Timestamp', 'Option_type', 'Strike'])
            df = df[['New_date_Time', 'Ticker', 'Date', 'Open', 'High', 'Low', 'Close',
                     'Volume', 'Open Interest', 'Symbol', 'Option_type', 'Temp', 'Exp_year',
                     'Exp_month', 'Strike', 'exp_month_number', 'current_month_number',
                     'difference', 'curr_exp_date', 'current_exp_month_number',
                     'Diff_months', 'Timestamp', 'Adj_Open', 'Adj_High', 'Adj_Low',
                     'Adj_Close', 'Adj_volume', 'Adj_OI', 'Adj_strike', 'Final_strike',
                     'month']]
            numberOfRowsFinal += df.shape[0]
            #df.to_csv(output_path + symbol + '-' + i + '.csv', mode='a', header=not os.path.exists(output_path + symbol + i + '.csv'), index=False)
            numberOfRowsDF.loc[j, 'FinalRows'] += df.shape[0]

            schema = 'Monthly' + i
            newSymbol = symbol.replace('&', '_')
            tablename = newSymbol + '-' + i
            #df.reset_index(drop=True,inplace=True)
            #df = pd.read_csv(file_path)
            df = df.rename(columns={'Timestamp' : 'Time'})
            df['rem'] = df['Final_strike'] % df['Final_strike'].astype(int)
            df.loc[df['rem'] == 0, 'Ticker'] = newSymbol + '-' + i + df['Final_strike'].astype(int).astype(str) + df['Option_type']
            df.loc[df['rem'] != 0, 'Ticker'] = newSymbol + '-' + i + df['Final_strike'].round(2).astype(str) + df['Option_type'] 
            
            df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
            adjusted_df = df[['Ticker', 'Date', 'Time', 'Adj_Open','Adj_High','Adj_Low','Adj_Close','Adj_volume', 'Adj_OI']]
            
            
            sql2=f'''CREATE TABLE IF NOT EXISTS "MonthlyRaw{i}"."{tablename}"("Ticker" varchar(50),"Date" Date NOT NULL, "Time" time NOT NULL, "Open" float NOT NULL, "High" float NOT NULL, "Low" float NOT NULL, "Close" float NOT NULL, "Volume" float NOT NULL, "Open_Interest" float NOT NULL);'''
            sql3=f'''CREATE TABLE IF NOT EXISTS "Monthly{i}"."{tablename}"("Ticker" varchar(50),"Date" Date NOT NULL, "Time" time NOT NULL, "Open" float NOT NULL, "High" float NOT NULL, "Low" float NOT NULL, "Close" float NOT NULL, "Volume" float NOT NULL, "Open_Interest" float NOT NULL);'''
            #print(sql2)
            cursor.execute(sql2)
            #print(sql3)
            cursor.execute(sql3)
            conn.commit()
##
##            sql2 = f'''DELETE FROM "MonthlyRaw{i}"."{tablename}" WHERE "Date" = '2023-05-10' '''
##            cursor.execute(sql2)
##            sql3 = f'''DELETE FROM "Monthly{i}"."{tablename}" WHERE "Date" = '2023-05-10' '''
##            cursor.execute(sql3)

            buffer = StringIO()
            adjusted_df.to_csv(buffer, index = False)
            buffer.seek(0)
            sql = "COPY %s FROM STDIN WITH CSV HEADER DELIMITER AS ','"
            table = f'"{schema}"."{tablename}"'

            with conn.cursor() as cur:
                cur.copy_expert(sql = sql % table, file=buffer)
                conn.commit()

            schema = 'MonthlyRaw' + i
            
            raw_df = df[['Ticker', 'Date', 'Time', 'Open','High','Low','Close','Volume', 'Open Interest']]
            
            buffer = StringIO()
            raw_df.to_csv(buffer, index = False)
            buffer.seek(0)
            sql = "COPY %s FROM STDIN WITH CSV HEADER DELIMITER AS ','"
            table = f'"{schema}"."{tablename}"'

            with conn.cursor() as cur:
                cur.copy_expert(sql = sql % table, file=buffer)
                conn.commit()
            
    j+=1

# In[15]:


print(numberOfRowsRaw)
print(numberOfRowsFinal)
numberOfRowsDF.to_csv(r"C:\Users\Administrator\Desktop\data3.csv", index=False)

### ## Right Issue Adjustment 
##
### In[16]:
##
##
##rightIssueFile = r"E:\sourav\Database\Codes\CsvFiles\RightsIssue.xlsx"
##input_path = r"D:\Sourav\Data\UpdatedTill23Dec2022\\"
###final_data_path = r"E:\sourav\Database\RoundOffDone\\"
##final_data_path = input_path
##output_path = r"E:\sourav\Database\RightIssue\\"
##bhavcopy_path = r"\\iMAC2\F\All Databases\Options (Updated as of 29122022)\Index Options\Excel\EQBhavcopyNSE\\"
##
##for file in next(os.walk(output_path))[2]:
##    try:
##        os.remove(output_path + file)
##    except:
##        pass
##x = pd.read_excel(rightIssueFile)
##x = x.loc[:, ~x.columns.str.contains('^Unnamed')] 
##x['Ex. Date'] = pd.to_datetime(x['Ex. Date'], dayfirst=True)
##
##startDate = date(2022,10,3)
##endDate = date(2022,10,3)
##startDate = datetime.strptime(str(startDate), '%Y-%m-%d')
##endDate = datetime.strptime(str(endDate), '%Y-%m-%d')
##x = x[(x['Ex. Date'] >= startDate) & (x['Ex. Date']<=endDate)]
##x = x.sort_values(by=['Ex. Date'])
##print(x)
##
##for index, row in tqdm(x.iterrows()):
##    
##    date1 = row['Ex. Date']
##    year1 = str(date1.year)
##    month1 = date1.strftime('%b').upper()
##    day1 = date1.strftime('%d')
##    file_path = bhavcopy_path + '_' + year1 + '//' + '_' + month1 + '//' + '_' +  day1 + month1 + year1 + '.csv'
##    
##    df1 = pd.read_csv(file_path)
##    df1 = df1[df1['SYMBOL']==row['Symbol']]
##    
##    x.loc[index, 'EQ'] = df1.iloc[0]['PREVCLOSE']
##
##print(x)
###x['Symbol_New'] = x['Symbol'].str.replace('\_|\&|\-','')
##x['Symbol_New'] = x['Symbol'].copy()
##x['A'] = x['Rights Ratio'].str.replace("\'", '').str.split(':').str[0].astype(float)
##x['B'] = x['Rights Ratio'].str.replace("\'", '').str.split(':').str[1].astype(float)
##x = x[['Symbol_New', 'A', 'B', 'Rights Ratio', 'Premium', 'FACE VALUE', 'Ex. Date', 'EQ']]
##x['Expected_EQ'] = ((x['B'] * x['EQ']) + x['A'] * (x['Premium'] + x['FACE VALUE'])) / (x['A'] + x['B'])
##x['Split_Ratio'] = x['EQ'] / x['Expected_EQ']
##
##
##split_dict = {}
##for name, group in x.groupby(['Symbol_New']):
##    split_dict[name] = group
##
##symbols = next(os.walk(input_path))[2]
##for i in range(len(symbols)):
##    symbols[i] = symbols[i].replace('-III.csv', '').replace('-II.csv', '').replace('-I.csv', '')
##
##symbols = sorted(list(set(symbols)))
##print(len(symbols))
##
##symbols = sorted(list(set(symbols).intersection(x['Symbol_New'].unique())))
##print(symbols)
##
##
### In[18]:
##
##
##print('Start time : ', datetime.now())
##hyphens = ['-I', '-II', '-III']
##if symbols:
##    for symbol in tqdm(symbols):
##        print(symbol)  
##        sym1 = symbol
##
##        for i in hyphens:    
##            print(i)
##            file_path = final_data_path + '//' + symbol + i + '.csv'
##            if os.path.isfile(file_path):
##
##                df = pd.read_csv(file_path)
##
##                df['Date'] = pd.to_datetime(df['Date'].astype(str), dayfirst=True).dt.date
##                df['Adj_Open'] = df['Open']
##                df['Adj_High'] = df['High']
##                df['Adj_Low'] = df['Low']
##                df['Adj_Close'] = df['Close']
##                df['Adj_strike'] = df['Strike']
##                df['Adj_volume'] = df['Volume']
##                df['Adj_OI'] = df['Open Interest']
##
##                if symbol in split_dict.keys():
##                    print('Symbol has some adjustment!!!')
##
##                    for j in range(split_dict[sym1].shape[0]):
##                        if j==0:
##                            df['Adj_Open'] = np.where(df['Date'] < split_dict[sym1].iloc[j]['Ex. Date'], 
##                                                        df['Adj_Open'] / split_dict[sym1].iloc[j]['Split_Ratio'], df['Adj_Open'])
##
##                            df['Adj_High'] = np.where(df['Date'] < split_dict[sym1].iloc[j]['Ex. Date'], 
##                                                        df['Adj_High'] / split_dict[sym1].iloc[j]['Split_Ratio'], df['Adj_High'])
##
##                            df['Adj_Low'] = np.where(df['Date'] < split_dict[sym1].iloc[j]['Ex. Date'], 
##                                                        df['Adj_Low'] / split_dict[sym1].iloc[j]['Split_Ratio'], df['Adj_Low'])
##
##                            df['Adj_Close'] = np.where(df['Date'] < split_dict[sym1].iloc[j]['Ex. Date'], 
##                                                        df['Adj_Close'] / split_dict[sym1].iloc[j]['Split_Ratio'], df['Adj_Close'])
##
##                            df['Adj_strike'] = np.where(df['Date'] < split_dict[sym1].iloc[j]['Ex. Date'], 
##                                                        df['Adj_strike'] / split_dict[sym1].iloc[j]['Split_Ratio'], df['Adj_strike'])
##
##                            df['Adj_volume'] = np.where(df['Date'] < split_dict[sym1].iloc[j]['Ex. Date'], 
##                                                        df['Adj_volume'] * split_dict[sym1].iloc[j]['Split_Ratio'], df['Adj_volume'])
##
##                            df['Adj_OI'] = df['Adj_OI'].astype('float')
##                            df['Adj_OI'] = np.where(df['Date'] < split_dict[sym1].iloc[j]['Ex. Date'], 
##                                                        df['Adj_OI'] * split_dict[sym1].iloc[j]['Split_Ratio'], df['Adj_OI'])
##
##                else:
##                    print('Symbol does not have any adjustment!!!')
##
##
##                df['Final_strike'] = df['Adj_strike']
##                df['month'] = pd.to_datetime(df['Date'], dayfirst=True).dt.month    
##                df = df.sort_values(by=['Date', 'Timestamp', 'Option_type', 'Strike'])
##                df.to_csv(output_path + symbol + i + '.csv', index=False)
##print('End time : ', datetime.now())
##
##
### ## Split and Bonus Adjustment
##
### In[21]:
##
##
##splitAndBonusFile = r"E:\sourav\Database\Codes\CsvFiles\AllAdjustments.csv"
##input_path = r"D:\Sourav\Data\UpdatedTill23Dec2022\\"
##final_data_path = input_path
##output_path = r"E:\sourav\Database\SpliAndBonus\\"
##
##for file in next(os.walk(output_path))[2]:
##    os.remove(output_path + file)
##
##x = pd.read_csv(splitAndBonusFile)
##x = x.loc[:, ~x.columns.str.contains('^Unnamed')] 
##x['Ex. Date'] = pd.to_datetime(x['Ex. Date'], dayfirst=True)
##startDate = datetime.strptime(str(startDate), '%Y-%m-%d')
##endDate = datetime.strptime(str(endDate), '%Y-%m-%d')
##
##x = x[(x['Ex. Date'] >= startDate) & (x['Ex. Date']<=endDate)]
##x = x.sort_values(by=['Ex. Date'])
##print(x)
##
##x['Symbol_New'] = x['Symbol'].copy()
##x = x[x['Corporate Action'].isin(['Split', 'Bonus', 'Split ', 'split'])]
##x['Numerator'] = x['NSE Ratio'].str.replace("\'", '').str.split(':').str[0].astype(float)
##x['Denominator'] = x['NSE Ratio'].str.replace("\'", '').str.split(':').str[1].astype(float)
##x['Split_Ratio'] = np.where(x['Corporate Action']=='Bonus', 1 + (x['Numerator']/x['Denominator']), x['Numerator']/x['Denominator'])
##x = x[['Symbol_New', 'Ex. Date', 'Corporate Action', 'NSE Ratio', 'Split_Ratio']]
##
##split_dict = {}
##for name, group in x.groupby(['Symbol_New']):
##    split_dict[name] = group
##
##symbols = next(os.walk(input_path))[2]
##for i in range(len(symbols)):
##    symbols[i] = symbols[i].replace('-III.csv', '').replace('-II.csv', '').replace('-I.csv', '')
##
##symbols = sorted(list(set(symbols)))
##print(len(symbols))
##
##symbols = sorted(list(set(symbols).intersection(x['Symbol_New'].unique())))
##symbols
##
##
### In[ ]:
##
##
##print('Start time : ', datetime.now())
##hyphens = ['-I', '-II', '-III']
##
##if symbols:
##    for symbol in tqdm(symbols):
##        print(symbol)  
##        sym1 = symbol
##
##        for i in hyphens:    
##            print(i)
##            file_path = final_data_path + '//' + symbol + i + '.csv'
##            if os.path.isfile(file_path):
##
##                df = pd.read_csv(file_path)
##                df['Date'] = pd.to_datetime(df['Date'].astype(str), dayfirst=True).dt.date
##                df['Adj_strike'] = df['Final_strike']
##
##                if sym1 in split_dict.keys():
##
##                    for j in range(split_dict[sym1].shape[0]):
##
##                        df['Adj_Open'] = np.where(df['Date'] < split_dict[sym1].iloc[j]['Ex. Date'], 
##                                                    df['Adj_Open'] / split_dict[sym1].iloc[j]['Split_Ratio'], df['Adj_Open'])
##
##                        df['Adj_High'] = np.where(df['Date'] < split_dict[sym1].iloc[j]['Ex. Date'], 
##                                                    df['Adj_High'] / split_dict[sym1].iloc[j]['Split_Ratio'], df['Adj_High'])
##
##                        df['Adj_Low'] = np.where(df['Date'] < split_dict[sym1].iloc[j]['Ex. Date'], 
##                                                    df['Adj_Low'] / split_dict[sym1].iloc[j]['Split_Ratio'], df['Adj_Low'])
##
##                        df['Adj_Close'] = np.where(df['Date'] < split_dict[sym1].iloc[j]['Ex. Date'], 
##                                                    df['Adj_Close'] / split_dict[sym1].iloc[j]['Split_Ratio'], df['Adj_Close'])
##
##                        df['Adj_strike'] = np.where(df['Date'] < split_dict[sym1].iloc[j]['Ex. Date'], 
##                                                    df['Adj_strike'] / split_dict[sym1].iloc[j]['Split_Ratio'], df['Adj_strike'])
##
##                        df['Adj_volume'] = np.where(df['Date'] < split_dict[sym1].iloc[j]['Ex. Date'], 
##                                                    df['Adj_volume'] * split_dict[sym1].iloc[j]['Split_Ratio'], df['Adj_volume'])
##
##                        df['Adj_OI'] = np.where(df['Date'] < split_dict[sym1].iloc[j]['Ex. Date'], 
##                                                    df['Adj_OI'] * split_dict[sym1].iloc[j]['Split_Ratio'], df['Adj_OI'])
##
##                df['Final_strike'] = df['Adj_strike']
##                df['month'] = pd.to_datetime(df['Date'], dayfirst=True).dt.month    
##
##                df = df.sort_values(by=['Date', 'Option_type', 'Strike'])
##                df.to_csv(output_path + symbol + i + '.csv', index=False)
##
##print('End time : ', datetime.now())
##
##
### ## Dividend 
##
### In[ ]:
##
##
##dividendFile = r"E:\sourav\Database\Codes\CsvFiles\DividendAdjustments.xlsx"
###input_path = r"D:\Sourav\Data\UpdatedTill23Dec2022\\"
##input_path = r"D:\Sourav\Data\UpdatedTill23Dec2022 - 1\\"
##final_data_path = input_path
##output_path = r"E:\sourav\Database\Dividend\\"
##
##for file in next(os.walk(output_path))[2]:
##    os.remove(output_path + file)
##
##x = pd.read_excel(dividendFile)
##x = x.loc[:, ~x.columns.str.contains('^Unnamed')] 
##x['Ex. Date'] = pd.to_datetime(x['Ex. Date'], dayfirst=True)
##startDate = datetime.strptime(str(startDate), '%Y-%m-%d')
##endDate = datetime.strptime(str(endDate), '%Y-%m-%d')
##x = x[(x['Ex. Date'] >= startDate) & (x['Ex. Date']<=endDate)]
##x = x.sort_values(by=['Ex. Date'])
##print(x)
##
##x['Symbol_New'] = x['Symbol'].copy()
##x = x[['Symbol_New', 'Ex. Date', 'Corporate Action', 'Dividend']]
##split_dict = {}
##for name, group in x.groupby(['Symbol_New']):
##    split_dict[name] = group
##
##
##symbols = next(os.walk(input_path))[2]
##for i in range(len(symbols)):
##    symbols[i] = symbols[i].replace('-III.csv', '').replace('-II.csv', '').replace('-I.csv', '')
##
##symbols = sorted(list(set(symbols)))
##print(len(symbols))
##
##symbols = sorted(list(set(symbols).intersection(x['Symbol_New'].unique())))
##print(symbols)
##
##
##
### In[ ]:
##
##
##print('Start time : ', datetime.now())
##hyphens = ['-I', '-II', '-III']
##if symbols:
##    for symbol in tqdm(symbols):
##        print(symbol)  
##        sym1 = symbol
##        for i in hyphens:    
##            print(i)
##            file_path = final_data_path + '//' + symbol + i + '.csv'
##            if os.path.isfile(file_path):
##
##                df = pd.read_csv(file_path)
##                df['Date'] = pd.to_datetime(df['Date'].astype(str), dayfirst=True).dt.date
##                df['Adj_strike'] = df['Final_strike']
##
##                if sym1 in split_dict.keys():
##
##                    for j in range(split_dict[sym1].shape[0]):
##
##                        df['Adj_strike'] = np.where(df['Date'] < split_dict[sym1].iloc[j]['Ex. Date'], 
##                                                    df['Adj_strike'] - split_dict[sym1].iloc[j]['Dividend'], df['Adj_strike'])            
##
##                df['Final_strike'] = df['Adj_strike']
##                df['month'] = pd.to_datetime(df['Date'], dayfirst=True).dt.month    
##
##                df = df.sort_values(by=['Date', 'Option_type', 'Strike'])
##                df.to_csv(output_path + symbol + i + '.csv', index=False)
##
##
##print('End time : ', datetime.now())
##
##
### ## Strike Round Off
##
### In[22]:
##
##
##print(datetime.now())
##import math
##hyphens = ['-I', '-II', '-III']
##symbols = ['SUZLON']
###input_path1 = r"E:\sourav\Database\SpliAndBonus\\"
##input_path2 = r"E:\sourav\Database\RightIssue\\"
###input_path3 = r"E:\sourav\Database\Dividend\\"
##output_path = r"E:\sourav\Database\RoundOffDone\\"
##
##for file in next(os.walk(output_path))[2]:
##    os.remove(output_path + file)
##    
###symbols = next(os.walk(input_path1))[2] + next(os.walk(input_path1))[2] + next(os.walk(input_path1))[2]
##
##for symbol in symbols:
##    for i in tqdm(hyphens):
###         if os.path.isfile(input_path1 + symbol + i + '.csv'):
###             df = pd.read_csv(input_path1 + symbol + i + '.csv')
##        if os.path.isfile(input_path2 + symbol + i + '.csv'):
##            df = pd.read_csv(input_path2 + symbol + i + '.csv')
###         if os.path.isfile(input_path3 + symbol + i + '.csv'):
###             df = pd.read_csv(input_path3 + symbol + i + '.csv')
##        if df.empty:
##            continue
##        print(symbol + i + ' Number of Rows : ' + str(df.shape[0]))
##        print(df)
##
##        df['Final_strike'] = df['Adj_strike'].apply(lambda x : round(x / 0.05) * 0.05)
##        df['Temp1'] = df['Final_strike']%df['Final_strike'].astype(int)
##        df['Final_strike'] = np.where(df['Temp1']==0, df['Final_strike'].astype(int), df['Final_strike'])
##        df = df.drop(['Temp1'], axis=1)
##        print(symbol + i + ' Number of Rows : ' + str(df.shape[0]))
##        df.to_csv(output_path + symbol + i + '.csv', index=False)
##
##        print(df['Final_strike'].unique())
##print(datetime.now())

##x = input('Press enter to exit...')
##exit()
