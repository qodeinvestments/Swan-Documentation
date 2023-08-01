# Download stock eq GDFL file

import pandas as pd
import os
import numpy as np

symbols = next(os.walk(r"C:\Data\OptionsDatabaseDailyUpdate\Updation_Temp_Data\cont_data"))[2]
for i in range(len(symbols)):
    symbols[i] = symbols[i].replace('-III.csv', '').replace('-II.csv', '').replace('-I.csv', '')
symbols = sorted(list(set(symbols)))
print(len(symbols))

indexSymbols = ['INDIA VIX', 'NIFTY 50', 'NIFTY BANK', 'NIFTY FIN SERVICE']

import os
from imbox import Imbox # pip install imbox
import traceback
import datetime
from datetime import date
import googleapiclient
import struct
import pandas as pd
import zipfile

# enable less secure apps on your google account
# https://myaccount.google.com/lesssecureapps

########################## INPUTS ###########################
username = "vishwanath.raj@swancapital.in"
#password = 'qeqsurbstwqhxiqq' # sourav
password = 'qtphrosdvtnfsuee' # vishwanath
download_folder = r"C:\Data\StockEQRawFiles\\"
date = datetime.date(2023, 7, 31)
startDate = endDate = date
#date = date.today()
sendersEmail = 'noreply1@globaldatafeeds.in'
index_eq_path = r"C:\Data\IndexEQ\DailyUpdate\\"
stock_eq_path = r"C:\Data\StockEQ\DailyUpdate\\"
#############################################################

year = date.strftime('%Y')
month = date.strftime('%B')
day = date.strftime('%d')
print(date)
print(year)
print(month)
print(day)

if not os.path.isdir(download_folder):
    os.makedirs(download_folder, exist_ok=True)

host = "imap.gmail.com"
mail = Imbox(host, username=username, password=password, ssl=True, ssl_context=None, starttls=False)
# messages = mail.messages() # defaults to all the mails in inbox
         
# select specific sender and a date.
messages = mail.messages(sent_from=sendersEmail, 
                         date__on=date)

download_path = download_folder + '\\' + year + '\\' + month + ' ' + year + '\\' + day

#print(download_path)
if not os.path.exists(download_path):
    os.makedirs(download_path)

for file in next(os.walk(download_path))[2]:
    os.remove(download_path + '//' + file) 

for (uid, message) in messages:
    mail.mark_seen(uid) # optional, mark message as read
        

    for idx, attachment in enumerate(message.attachments):
        #try:
        print(attachment)
        
        att_fn = attachment.get('filename')
        #print(att_fn)
        if not att_fn.endswith('.zip'):
            continue
       
##        #print(download_path)
##        if not os.path.exists(download_path):
##            os.makedirs(download_path)
        
        print(att_fn)
        date1 = att_fn[13:21]
        print(date1)
        download_path1 = download_path + '//' + att_fn

        

        with open(download_path1, "wb") as fp:
            fp.write(attachment.get('content').read())
            

#         except Exception as e:
#             print(e)
print(download_path)
print(download_path1)



for file in os.listdir(download_path):
    print(file)


    with zipfile.ZipFile(download_path + '\\' + file, 'r') as zip_ref:
        zip_ref.extractall(download_path)

# stocks
print(next(os.walk(download_path))[2])

df_stocks = pd.read_csv(download_path + '//' + f'GFDLCM_STOCK_{date1}.csv')
df_stocks['Symbol'] = df_stocks['Ticker'].str.replace('.NSE', '')
df_stocks = df_stocks[df_stocks['Symbol'].isin(symbols)]

print(np.setdiff1d(symbols, df_stocks['Symbol'].unique()))
print(np.setdiff1d(df_stocks['Symbol'].unique(), symbols))

for file in next(os.walk(stock_eq_path))[2]:
        os.remove(stock_eq_path + file) 

for name, group in df_stocks.groupby(['Symbol']):
    print(name[0])
    #print(group)
    group.to_csv(stock_eq_path + name[0] + '.EQ-NSE.csv', index=False)

# indices
df_indices = pd.read_csv(download_path + '//' + f'GFDLCM_INDICES_{date1}.csv')
df_indices['Symbol'] = df_indices['Ticker'].str.replace('.NSE_IDX', '')
df_indices = df_indices[df_indices['Symbol'].isin(indexSymbols)]

for file in next(os.walk(index_eq_path))[2]:
        os.remove(index_eq_path + file) 

for name, group in df_indices.groupby(['Symbol']):
    print(name[0])
    #print(group)
    group.to_csv(index_eq_path + name[0] + '.EQ-NSE.csv', index=False)

# delete the zip files
try:
    os.remove(download_path + '//' + f'GFDLCM_STOCK_{date1}.zip')
except:
    pass
try:
    os.remove(download_path + '//' + f'GFDLCM_INDICES_{date1}.zip')
except:
    pass
mail.logout()


# Upload stock eq to the database

import os
import pandas as pd
import numpy as np
import psycopg2
import time
import pandas as pd
from io import StringIO
import re
from tqdm.notebook import tqdm
from datetime import datetime, date
import warnings
import requests
warnings.filterwarnings("ignore")

###########################################################################################################
###########################################################################################################
#startDate = date.today()
#endDate = date.today()
final_data_path = r"C:\Data\StockEQ\DailyUpdate\\"
raw_data_path = r"C:\Data\StockEQ\DailyUpdate\\"

symbols = next(os.walk(final_data_path))[2]
for i in range(len(symbols)):
    symbols[i] = symbols[i].replace('.EQ-NSE.csv', '').replace('-II.csv', '').replace('-I.csv', '')
    
symbols = sorted(list(set(symbols)))
#symbols = ['AUBANK', 'AXISBANK', 'BANDHANBNK', 'BANKBARODA', 'CANBK', 'FEDERALBNK']
#print(len(symbols))

indices = ['BANKNIFTY', 'CNXINFRA', 'CNXIT', 'CNXPSE',
       'FINNIFTY', 'FTSE', 'MIDCPNIFTY', 'MINIFTY',
       'NFTYMCAP', 'NIFTY', 'NIFTYCPSE', 'NIFTYMID', 'S&P']
symbols = np.setdiff1d(symbols, indices)

#print(len(symbols))

conn = psycopg2.connect(database="StockEQ",
						user='postgres', password='swancap123',
						host='swandatabase.cfehmk2wtejq.ap-south-1.rds.amazonaws.com', port='5432'
)

conn.autocommit = True
cursor = conn.cursor()


st=time.time()
hyphens= ['I']
#symbols = ['AARTIIND']
stock_dict = {
              '3IINFOTECH' : 'TH_IINFOTECH',
              'IBN18' : 'TV_ET',
              'TV18BRDCST' : 'TV_ETBRDCST',
              
              }
for symbol in tqdm(symbols):
    print(symbol)
    for i in hyphens:
        
        
        adjusted_path = final_data_path + symbol + '.EQ-NSE.csv' 
        if os.path.isfile(adjusted_path):
            #print(i)
            df = pd.read_csv(adjusted_path)
            df['Date'] = pd.to_datetime(df['Date'], format='mixed', dayfirst=True)
            df['Time'] = pd.to_datetime(df['Time']).dt.time
##             date1 = datetime.strptime('28-04-2023', '%d-%m-%Y')
##             df = df[df['Date']<date1]
            time1 = datetime.strptime('15:29:59', '%H:%M:%S').time()
            df = df[df['Time']<=time1]
            
            df = df.rename(columns={'Open' : 'EQ_Open',
                                    'High' : 'EQ_High',
                                    'Low' : 'EQ_Low',
                                    'Close' : 'EQ_Close',
                                    'Volume' : 'EQ_Volume',
                                    'Adj_Open' : 'EQ_Open',
                                    'Adj_High' : 'EQ_High',
                                    'Adj_Low' : 'EQ_Low',
                                    'Adj_Close' : 'EQ_Close'})
            adjusted_df = df[['Ticker','Date','Time','EQ_Open','EQ_High','EQ_Low','EQ_Close','EQ_Volume']]
            
            raw_path = raw_data_path + symbol + '.EQ-NSE.csv' 
            if os.path.isfile(raw_path):
                #print(i)
                df = pd.read_csv(raw_path)
                df['Date'] = pd.to_datetime(df['Date'], format='mixed', dayfirst=True)
                df['Time'] = pd.to_datetime(df['Time']).dt.time
#                 date1 = datetime.strptime('28-04-2023', '%d-%m-%Y')
#                 df = df[df['Date']<date1]
                time1 = datetime.strptime('15:29:59', '%H:%M:%S').time()
                df = df[df['Time']<=time1]

                df = df.rename(columns={'Open' : 'EQ_Open',
                                        'High' : 'EQ_High',
                                        'Low' : 'EQ_Low',
                                        'Close' : 'EQ_Close',
                                        'Volume' : 'EQ_Volume',
                                        'Adj_Open' : 'EQ_Open',
                                        'Adj_High' : 'EQ_High',
                                        'Adj_Low' : 'EQ_Low',
                                        'Adj_Close' : 'EQ_Close'})
            
            raw_df = df[['Ticker','Date','Time','EQ_Open','EQ_High','EQ_Low','EQ_Close','EQ_Volume']]
        
            
            #newSymbol = symbol.replace('&', '_')
            newSymbol = re.sub('\&|\-', '_', symbol)
            newSymbol = newSymbol
            
            delete_query = f'''DROP TABLE IF EXISTS "RawData"."{newSymbol}"'''
            cursor.execute(delete_query)
            delete_query = f'''DROP TABLE IF EXISTS "AdjustedData"."{newSymbol}"'''
            cursor.execute(delete_query)

##            sql2 = f'''DELETE FROM "AdjustedData"."{newSymbol}" WHERE "Date" = '2023-07-28' '''
##            #print(sql2)
##            cursor.execute(sql2)
##            sql3 = f'''DELETE FROM "RawData"."{newSymbol}" WHERE "Date" = '2023-07-28' '''
##            cursor.execute(sql3)

            if newSymbol in stock_dict:
                newSymbol = stock_dict[newSymbol]
            
            sql2=f'''CREATE TABLE IF NOT EXISTS "RawData"."{newSymbol}"("Ticker" varchar(50),"Date" Date NOT NULL, "Time" time NOT NULL, "EQ_Open" float NOT NULL, "EQ_High" float NOT NULL, "EQ_Low" float NOT NULL, "EQ_Close" float NOT NULL, "EQ_Volume" float NOT NULL);'''
            sql3=f'''CREATE TABLE IF NOT EXISTS "AdjustedData"."{newSymbol}"("Ticker" varchar(50),"Date" Date NOT NULL, "Time" time NOT NULL, "EQ_Open" float NOT NULL, "EQ_High" float NOT NULL, "EQ_Low" float NOT NULL, "EQ_Close" float NOT NULL, "EQ_Volume" float NOT NULL);'''
            #print(sql2)
            cursor.execute(sql2)
            #print(sql3)
            cursor.execute(sql3)
            conn.commit()

            buffer = StringIO()
            raw_df.to_csv(buffer, index = False)
            buffer.seek(0)
            sql = "COPY %s FROM STDIN WITH CSV HEADER DELIMITER AS ','"
            
            table=f'"RawData"."{newSymbol}"'
            with conn.cursor() as cur:
                cur.copy_expert(sql=sql % table, file=buffer)
                conn.commit()
                
            buffer = StringIO()
            adjusted_df.to_csv(buffer, index = False)
            buffer.seek(0)
            sql = "COPY %s FROM STDIN WITH CSV HEADER DELIMITER AS ','"
            table= f'"AdjustedData"."{newSymbol}"'
            with conn.cursor() as cur:
                cur.copy_expert(sql=sql % table, file=buffer)
                conn.commit()

conn.close()
#print("sql done")
et=time.time()

elapsed_time=et-st;
print('Stock EQ updation to database finished!')
print("elapsed_time:",elapsed_time)

##########################################################################################
##########################################################################################


import pyspark
import numpy as np
import pandas as pd
import os
import time
import datetime
from datetime import datetime
from os import walk
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import array_contains
# from pyspark.sql.functions import *
from pyspark.sql.functions import date_format
from datetime import date, timedelta
from tqdm.notebook import tqdm
import psycopg2
import warnings
import math
import psycopg2 as pg

from io import StringIO
warnings.filterwarnings('ignore')


splitAndBonusFile = r"C:\Data\CsvFiles\AllAdjustments.csv"
#input_path = r"D:\Sourav\Data\UpdatedTill23Dec2022\\"
#final_data_path = input_path
#output_path = r"E:\sourav\Database\SpliAndBonus\\"
##startDate = date(2023,6,21)
##endDate = date(2023,6,21)
##for file in next(os.walk(output_path))[2]:
##    os.remove(output_path + file)

x = pd.read_csv(splitAndBonusFile)
x = x.loc[:, ~x.columns.str.contains('^Unnamed')] 
x['Ex. Date'] = pd.to_datetime(x['Ex. Date'], dayfirst=True).dt.date
startDate = datetime.strptime(str(startDate), '%Y-%m-%d').date()
endDate = datetime.strptime(str(endDate), '%Y-%m-%d').date()

x = x[(x['Ex. Date'] >= startDate) & (x['Ex. Date']<=endDate)]
x = x.sort_values(by=['Ex. Date'])
print(x)
 
x['Symbol_New'] = x['Symbol'].copy()
x = x[x['Corporate Action'].isin(['Split', 'Bonus', 'Split ', 'split'])]
x['Numerator'] = x['NSE Ratio'].str.replace("\'", '').str.split(':').str[0].astype(float)
x['Denominator'] = x['NSE Ratio'].str.replace("\'", '').str.split(':').str[1].astype(float)
x['Split_Ratio'] = np.where(x['Corporate Action']=='Bonus', 1 + (x['Numerator']/x['Denominator']), x['Numerator']/x['Denominator'])
x = x[['Symbol_New', 'Ex. Date', 'Corporate Action', 'NSE Ratio', 'Split_Ratio']]

split_dict = {}
for name, group in x.groupby(['Symbol_New']):
    split_dict[group.iloc[0]['Symbol_New']] = group

    
print(split_dict)

hyphens = ['I']

print('Checking split and bonus adjustments...')
if split_dict:
    print('Performing split and bonus adjustments...')
    for key, value in split_dict.items():
        print(key)
        
        
        split_ratio = list(value['Split_Ratio'])[0]
        symbol = key
        for i in hyphens:
            print(i)
            try:
                df = pd.DataFrame()
                engine = pg.connect("dbname='StockEQ' user='postgres' host='swandatabase.cfehmk2wtejq.ap-south-1.rds.amazonaws.com' port='5432' password='swancap123'")
                df = pd.read_sql(f'select * from "AdjustedData"."{symbol}"', con=engine)
                
            except Exception as e:
                print(e)
                print("May be table does not exist...") 
            engine.close()
            if df.empty:
                print('Table does not exist')
            else:
                print(i)
                print('Table exists')
                print(df['Date'])
                df['Date'] = pd.to_datetime(df['Date'].astype(str), format='mixed', dayfirst=True).dt.date
                print(df.columns)
                print(df)
                
                sym1 = symbol
                df = df.rename(columns={'EQ_Open' : 'Adj_Open',
                                        'Open' : 'Adj_Open',
                                        'EQ_High' : 'Adj_High',
                                        'High' : 'Adj_High',
                                        'EQ_Low' : 'Adj_Low',
                                        'Low' : 'Adj_Low',
                                        'EQ_Close' : 'Adj_Close',
                                        'Close' : 'Adj_Close',
                                        'EQ_Volume' : 'Adj_volume',
                                        'Volume' : 'Adj_volume',
                                        'Open_Interest' : 'Adj_OI'})
                print(df.columns)
                if sym1 in split_dict.keys():

                    for j in range(split_dict[sym1].shape[0]):
                        print(type(df.iloc[0]['Date']))
                        print(type(split_dict[sym1].iloc[j]['Ex. Date']))

                        df['Adj_Open'] = np.where(df['Date'] < split_dict[sym1].iloc[j]['Ex. Date'], 
                                                    df['Adj_Open'] / split_dict[sym1].iloc[j]['Split_Ratio'], df['Adj_Open'])

                        df['Adj_High'] = np.where(df['Date'] < split_dict[sym1].iloc[j]['Ex. Date'], 
                                                    df['Adj_High'] / split_dict[sym1].iloc[j]['Split_Ratio'], df['Adj_High'])

                        df['Adj_Low'] = np.where(df['Date'] < split_dict[sym1].iloc[j]['Ex. Date'], 
                                                    df['Adj_Low'] / split_dict[sym1].iloc[j]['Split_Ratio'], df['Adj_Low'])

                        df['Adj_Close'] = np.where(df['Date'] < split_dict[sym1].iloc[j]['Ex. Date'], 
                                                    df['Adj_Close'] / split_dict[sym1].iloc[j]['Split_Ratio'], df['Adj_Close'])

#                         df['Adj_strike'] = np.where(df['Date'] < split_dict[sym1].iloc[j]['Ex. Date'], 
#                                                     df['Adj_strike'] / split_dict[sym1].iloc[j]['Split_Ratio'], df['Adj_strike'])

                        df['Adj_volume'] = np.where(df['Date'] < split_dict[sym1].iloc[j]['Ex. Date'], 
                                                    df['Adj_volume'] * split_dict[sym1].iloc[j]['Split_Ratio'], df['Adj_volume'])

#                         df['Adj_OI'] = np.where(df['Date'] < split_dict[sym1].iloc[j]['Ex. Date'], 
#                                                     df['Adj_OI'] * split_dict[sym1].iloc[j]['Split_Ratio'], df['Adj_OI'])

#                 df['Final_strike'] = df['Adj_strike']
#                 df['month'] = pd.to_datetime(df['Date'], dayfirst=True).dt.month
                
                # strike round off
# 
                df = df.rename(columns={'Open' : 'EQ_Open',
                                        'High' : 'EQ_High',
                                        'Low' : 'EQ_Low',
                                        'Close' : 'EQ_Close',
                                        'Volume' : 'EQ_Volume',
                                        'Adj_Open' : 'EQ_Open',
                                        'Adj_High' : 'EQ_High',
                                        'Adj_Low' : 'EQ_Low',
                                        'Adj_Close' : 'EQ_Close',
                                        'Adj_volume' : 'EQ_Volume'})


                conn = psycopg2.connect(database="StockEQ",
                                        user='postgres', password='swancap123',
                                        host='swandatabase.cfehmk2wtejq.ap-south-1.rds.amazonaws.com', port='5432')

                conn.autocommit = True
                cursor = conn.cursor()
                #cursor = conn.cursor()
                sql1 = f'''DROP TABLE IF EXISTS "AdjustedData"."{sym1}" CASCADE'''
                print(sql1)
                cursor.execute(sql1)
                conn.commit()
                #conn.close()
                
                
                sql2=f'''CREATE TABLE IF NOT EXISTS "AdjustedData"."{sym1}"("Ticker" varchar(50),"Date" Date NOT NULL, "Time" time NOT NULL, "EQ_Open" float NOT NULL, "EQ_High" float NOT NULL, "EQ_Low" float NOT NULL, "EQ_Close" float NOT NULL, "EQ_Volume" float NOT NULL);'''
                print(sql2)
                cursor.execute(sql2)
                
                schema = 'AdjustedData'
        
                buffer = StringIO()

                df.to_csv(buffer, index = False)
                buffer.seek(0)
                sql = "COPY %s FROM STDIN WITH CSV HEADER DELIMITER AS ','"
                table = f'"{schema}"."{sym1}"'

                with conn.cursor() as cur:
                    cur.copy_expert(sql = sql % table, file=buffer)
                    conn.commit()

                conn.close()
else:
  print('There are no split and bonus adjustments!')


######################################################################################################
######################################################################################################

########################################### RIGHT ISSUE ##############################################
import pyspark
import numpy as np
import pandas as pd
import os
import time
import datetime
from datetime import datetime
from os import walk
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import array_contains
# from pyspark.sql.functions import *
from pyspark.sql.functions import date_format
from datetime import date, timedelta
from tqdm.notebook import tqdm
import psycopg2
import warnings
import math
import psycopg2 as pg
from io import StringIO
warnings.filterwarnings('ignore')
rightIssueFile = "C:\Data\CsvFiles\RightsIssue.xlsx"
x = pd.read_excel(rightIssueFile)
x['Ex. Date'] = pd.to_datetime(x['Ex. Date'], dayfirst=True).dt.date

##startDate = date(2023,5,15)
##endDate = date(2023,5,15)
startDate = datetime.strptime(str(startDate), '%Y-%m-%d').date()
endDate = datetime.strptime(str(endDate), '%Y-%m-%d').date()
x = x[(x['Ex. Date'] >= startDate) & (x['Ex. Date']<=endDate)]
x = x.sort_values(by=['Ex. Date'])
#print(x)
print('Checking for right issue adjustments...')
if not x.empty:
    print('Performing right issue adjustments...')
    x['Symbol_New'] = x['Symbol'].copy()
    x['A'] = x['PURPOSE'].str.split(':').str[0].str.extract('(\d+)').astype(float)
    x['B'] = x['PURPOSE'].str.split(':').str[1].str.extract('(\d+)').astype(float)
    x['Premium'] = x['PURPOSE'].str.split('@').str[1].str.extract('(\d+.\d+)').astype(float)
    x = x[['Symbol_New', 'A', 'B', 'Rights Ratio', 'Premium', 'FACE VALUE', 'Ex. Date', 'EQ']]
    x['EQ'] = np.nan
    x['Expected_EQ'] = np.nan
    x['Split_Ratio'] = np.nan
    split_dict = {}
    for name, group in x.groupby(['Symbol_New']):
        #split_dict[name] = group
        split_dict[group.iloc[0]['Symbol_New']] = group
    #print(split_dict)
        
    
    if split_dict:
        for key, value in split_dict.items():
            #print(key)
        
            date1 = value.iloc[0]['Ex. Date']
            #print(date1)
            symbol = key
            try:
                df = pd.DataFrame()
                engine = pg.connect("dbname='StockEQ' user='postgres' host='swandatabase.cfehmk2wtejq.ap-south-1.rds.amazonaws.com' port='5432' password='swancap123'")
                df = pd.read_sql(f'select * from "AdjustedData"."{symbol}"', con=engine)
                df['Date'] = pd.to_datetime(df['Date'], dayfirst=True).dt.date
                df = df.sort_values(by=['Date'])
                #print(df)
            except Exception as e:
                print(e)
                #print("May be table does not exist...") 
            engine.close()
            
            if not df.empty:
                df = df[df['Date']<date1]
                #df['Time'] = pd.to_datetime(df['Time']).dt.time
                time1 = datetime.strptime('15:29:59', '%H:%M:%S').time()
                df = df[df['Time']==time1]
                            
                split_dict[symbol]['EQ'] = df.iloc[-1]['EQ_Close']
                #print(((x['B'] * x['EQ']) + x['A'] * (x['Premium'] + x['FACE VALUE'])) / (x['A'] + x['B']))
                #split_dict[symbol]['Expected_EQ'] = ((x['B'] * x['EQ']) + x['A'] * (x['Premium'] + x['FACE VALUE'])) / (x['A'] + x['B'])
                split_dict[symbol]['Expected_EQ'] = ((split_dict[symbol]['B'] * split_dict[symbol]['EQ']) + split_dict[symbol]['A'] * (split_dict[symbol]['Premium'] + split_dict[symbol]['FACE VALUE'])) / (split_dict[symbol]['A']+split_dict[symbol]['B'])
                split_dict[symbol]['Split_Ratio'] = split_dict[symbol]['EQ'] / split_dict[symbol]['Expected_EQ']
                #print(split_dict)
            
            
            
                hyphens = ['I'] 
                for i in hyphens:
                    #print(i)
                    try:
                        df = pd.DataFrame()
                        engine = pg.connect("dbname='StockEQ' user='postgres' host='swandatabase.cfehmk2wtejq.ap-south-1.rds.amazonaws.com' port='5432' password='swancap123'")
                        df = pd.read_sql(f'select * from "AdjustedData"."{symbol}"', con=engine)
                    except Exception as e:
                        print(e)
                        #print("May be table does not exist...") 
                    engine.close()
                    if df.empty:
                        print('Table does not exist')
                    else:
                        #print(i)
                        #print('Table exists')
                        df['Date'] = pd.to_datetime(df['Date'].astype(str), format='mixed', dayfirst=True).dt.date
#                         df['Adj_strike'] = df['Ticker'].str.replace(f'{symbol}-{i}', '').str.replace('CE', '').str.replace('PE', '')
#                         #df['Adj_strike'] = df['Ticker'].str.replace('ALSTOMT_D', '').str.replace('CE', '').str.replace('PE', '').str.replace('-I', '')
#                         df['Adj_strike'] = df['Adj_strike'].astype(float)
                        #print(df)

                        sym1 = symbol
                        df = df.rename(columns={'Open' : 'Adj_Open',
                                                'High' : 'Adj_High',
                                                'Low' : 'Adj_Low',
                                                'Close' : 'Adj_Close',
                                                'Volume' : 'Adj_volume',
                                                'Open_Interest' : 'Adj_OI',
                                                'EQ_Open' : 'Adj_Open',
                                                'EQ_High' : 'Adj_High',
                                                'EQ_Low' : 'Adj_Low',
                                                'EQ_Close' : 'Adj_Close',
                                                'EQ_Volume' : 'Adj_volume'})
            
                        if sym1 in split_dict.keys():

                            for j in range(split_dict[sym1].shape[0]):
                                
                                df['Adj_Open'] = np.where(df['Date'] < split_dict[sym1].iloc[j]['Ex. Date'], 
                                                            df['Adj_Open'] / split_dict[sym1].iloc[j]['Split_Ratio'], df['Adj_Open'])

                                df['Adj_High'] = np.where(df['Date'] < split_dict[sym1].iloc[j]['Ex. Date'], 
                                                            df['Adj_High'] / split_dict[sym1].iloc[j]['Split_Ratio'], df['Adj_High'])

                                df['Adj_Low'] = np.where(df['Date'] < split_dict[sym1].iloc[j]['Ex. Date'], 
                                                            df['Adj_Low'] / split_dict[sym1].iloc[j]['Split_Ratio'], df['Adj_Low'])

                                df['Adj_Close'] = np.where(df['Date'] < split_dict[sym1].iloc[j]['Ex. Date'], 
                                                            df['Adj_Close'] / split_dict[sym1].iloc[j]['Split_Ratio'], df['Adj_Close'])

#                                 df['Adj_strike'] = np.where(df['Date'] < split_dict[sym1].iloc[j]['Ex. Date'], 
#                                                             df['Adj_strike'] / split_dict[sym1].iloc[j]['Split_Ratio'], df['Adj_strike'])

                                df['Adj_volume'] = np.where(df['Date'] < split_dict[sym1].iloc[j]['Ex. Date'], 
                                                            df['Adj_volume'] * split_dict[sym1].iloc[j]['Split_Ratio'], df['Adj_volume'])

#                                 df['Adj_OI'] = np.where(df['Date'] < split_dict[sym1].iloc[j]['Ex. Date'], 
#                                                             df['Adj_OI'] * split_dict[sym1].iloc[j]['Split_Ratio'], df['Adj_OI'])

                        df = df.rename(columns={'Open' : 'EQ_Open',
                                                'High' : 'EQ_High',
                                                'Low' : 'EQ_Low',
                                                'Close' : 'EQ_Close',
                                                'Volume' : 'EQ_Volume',
                                                'Adj_Open' : 'EQ_Open',
                                                'Adj_High' : 'EQ_High',
                                                'Adj_Low' : 'EQ_Low',
                                                'Adj_Close' : 'EQ_Close',
                                                'Adj_volume' : 'EQ_Volume'})


#                         print(df)
#                         df['Final_strike'] = df['Adj_strike']
#                         df['month'] = pd.to_datetime(df['Date'], dayfirst=True).dt.month

#                         # strike round off
#                         df['Final_strike'] = df['Adj_strike'].apply(lambda x : round(x / 0.05) * 0.05)
#                         df['Temp1'] = df['Final_strike']%df['Final_strike'].astype(int)
#                         df['Final_strike'] = np.where(df['Temp1']==0, df['Final_strike'].astype(int), df['Final_strike'])
#                         df = df.drop(['Temp1'], axis=1)
#                         df['rem'] = df['Final_strike'] % df['Final_strike'].astype(int)
#                         df['Option_type'] = df['Ticker'].str[-2:]
#                         df.loc[df['rem'] == 0, 'Ticker'] = sym1 + '-' + i + df['Final_strike'].astype(int).astype(str) + df['Option_type']
#                         df.loc[df['rem'] != 0, 'Ticker'] = sym1 + '-' + i + df['Final_strike'].round(2).astype(str) + df['Option_type'] 
#                         df = df[['Ticker', 'Date', 'Time', 'Adj_Open','Adj_High','Adj_Low','Adj_Close','Adj_volume', 'Adj_OI']]


                        conn = psycopg2.connect(database="StockEQ",
                            user='postgres', password='swancap123',
                            host='swandatabase.cfehmk2wtejq.ap-south-1.rds.amazonaws.com', port='5432')

                        conn.autocommit = True
                        cursor = conn.cursor()
                        #cursor = conn.cursor()
                        sql1 = f'''DROP TABLE IF EXISTS "AdjustedData"."{sym1}" CASCADE'''
                        #print(sql1)
                        #sql1 = '''DROP table IF EXISTS "MonthlyI"."ALSTOMT_D-I"  CASCADE'''
                        #print(sql1)
                        cursor.execute(sql1)
                        conn.commit()
                        #conn.close()

                        sql2=f'''CREATE TABLE IF NOT EXISTS "AdjustedData"."{sym1}"("Ticker" varchar(50),"Date" Date NOT NULL, "Time" time NOT NULL, "EQ_Open" float NOT NULL, "EQ_High" float NOT NULL, "EQ_Low" float NOT NULL, "EQ_Close" float NOT NULL, "EQ_Volume" float NOT NULL);'''
                        #print(sql2)
                        cursor.execute(sql2)

                        schema = 'AdjustedData'

                        buffer = StringIO()
                        #print(df)
                        df.to_csv(buffer, index = False)
                        buffer.seek(0)
                        sql = "COPY %s FROM STDIN WITH CSV HEADER DELIMITER AS ','"
                        table = f'"{schema}"."{sym1}"'

                        with conn.cursor() as cur:
                            cur.copy_expert(sql = sql % table, file=buffer)
                            conn.commit()
                        conn.close()
else:
    print('There are no right issue adjustments!')

##########################################################################################
##########################################################################################

################################### NEW SYMBOL CHECK #####################################    
# Fetching a list of stock options symbols
conn = psycopg2.connect(host='swandatabase.cfehmk2wtejq.ap-south-1.rds.amazonaws.com', dbname='StockOptions',
                        user='postgres', password='swancap123')
cursor = conn.cursor()

cursor.execute("""SELECT table_name  FROM information_schema.tables WHERE TABLE_TYPE='BASE TABLE' AND TABLE_SCHEMA = 'MonthlyI'""") # "rel" is short for relation.

symbols = [i[0] for i in cursor.fetchall()] # A list() of tables.
for i in range(len(symbols)):
    symbols[i] = symbols[i].replace('-I', '')
#print(len(symbols))
symbols = sorted(list(set(symbols)))
print(len(symbols))
conn.close()

# Fetching a list of stock EQ symbols
conn = psycopg2.connect(host='swandatabase.cfehmk2wtejq.ap-south-1.rds.amazonaws.com', dbname='StockEQ',
                        user='postgres', password='swancap123')
cursor = conn.cursor()

cursor.execute("""SELECT table_name  FROM information_schema.tables WHERE TABLE_TYPE='BASE TABLE' AND TABLE_SCHEMA = 'AdjustedData'""") # "rel" is short for relation.

symbols1 = [i[0] for i in cursor.fetchall()] # A list() of tables.
for i in range(len(symbols1)):
    symbols1[i] = symbols1[i].replace('-I', '')
#print(len(symbols1))
symbols1 = sorted(list(set(symbols1)))
print(len(symbols1))
conn.close()

total_symbols = np.setdiff1d(symbols, symbols1)
total_symbols = np.setdiff1d(total_symbols, ['MUNDRAPORT', 'SESAGOA'])
 
if total_symbols:
    #print('New symbol is added...')
    link1 = "New symbol is added in the stock options database!"
    base_url =f"https://api.telegram.org/bot6237928541:AAHl267HrSFBRFE-iIajz_x8eNkPydiQEEs/sendMessage?chat_id=-939411532&text={link1}"
    requests.get(base_url)
    raise ValueError('New symbol is added...')
else:
    
    link1 = "No new symbol is added in the stock options database!"
    base_url =f"https://api.telegram.org/bot6237928541:AAHl267HrSFBRFE-iIajz_x8eNkPydiQEEs/sendMessage?chat_id=-939411532&text={link1}"
    requests.get(base_url)

# Upload Index EQ to the database

#import the modules
import os
import pandas as pd
import psycopg2
import time
from io import StringIO
from datetime import datetime
import pyspark
from datetime import date
from tqdm.notebook import tqdm
import calendar
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql import Row
from datetime import timedelta
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import array_contains
from pyspark.sql.functions import date_format
import warnings
import datetime
warnings.filterwarnings('ignore')

conn = psycopg2.connect(database="IndexEQ",
                            user='postgres', password='swancap123',
                            host='swandatabase.cfehmk2wtejq.ap-south-1.rds.amazonaws.com', port='5432'
    )
conn.autocommit = True
cursor = conn.cursor()

tablename = "AllData"

st = time.time()

for i in tqdm(range(4)):
    if i == 0:
        schema = 'BankNifty'
        df = pd.read_csv(r"C:\Data\IndexEQ\DailyUpdate\NIFTY BANK.EQ-NSE.csv",parse_dates=['Date'])
        df['Date'] = pd.to_datetime(df['Date'],dayfirst=True)
        df['Ticker'] = 'BANKNIFTY.EQ-NSE'
    elif i == 1:
        schema = 'Nifty'
        df = pd.read_csv(r"C:\Data\IndexEQ\DailyUpdate\NIFTY 50.EQ-NSE.csv",parse_dates=['Date'])
        df['Date'] = pd.to_datetime(df['Date'],dayfirst=True)
        df['Ticker'] = 'NIFTY.EQ-NSE'
    elif i == 2:
        schema = 'FinNifty'
        df = pd.read_csv(r"C:\Data\IndexEQ\DailyUpdate\NIFTY FIN SERVICE.EQ-NSE.csv",parse_dates=['Date'])
        df['Date'] = pd.to_datetime(df['Date'],dayfirst=True)
        df['Ticker'] = 'FINNIFTY.EQ-NSE'
    elif i == 3:
        schema = 'IndiaVix'
        df = pd.read_csv(r"C:\Data\IndexEQ\DailyUpdate\INDIA VIX.EQ-NSE.csv",parse_dates=['Date'])
        df['Date'] = pd.to_datetime(df['Date'],dayfirst=True)
        df['Ticker'] = 'INDIAVIX.EQ-NSE'

    df.reset_index(drop=True,inplace=True)
    df = df[['Date','Time','Ticker','Open','High','Low','Close','Volume']]
#     df = df[(df['Time']>='09:15:00') & (df['Time']<='15:30:00')]
    
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    buffer = StringIO()
    df.to_csv(buffer, index = False)
    buffer.seek(0)
    sql = "COPY %s FROM STDIN WITH CSV HEADER DELIMITER AS ','"
    table = f'"{schema}"."{tablename}"'

    with conn.cursor() as cur:
        cur.copy_expert(sql = sql % table, file=buffer)
        conn.commit()
    
    print(f"{schema} updation done")
conn.close()
print("Done")
et = time.time()
print("Elapsed time",et-st)








