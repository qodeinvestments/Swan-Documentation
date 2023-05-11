#################################################################################
#################################################################################
                    ## UPLOAD GDFL FILE TO DATABASE ##


import requests, zipfile, io
import pandas as pd
from io import BytesIO
from datetime import datetime, date
import os
import calendar
import time

################################# INPUTS ########################################################
zip_file_url = input()#"https://www.dropbox.com/sh/up7jncgjmrumj2p/AAB2ev3msAXeQr0a7QGvr557a?dl=0"
#date = currentDate = date(2023, 5, 10) # date.today() #date.today()
date = currentDate = date.today()
output_path = r"C:\Users\Administrator\Downloads\\"
common_drive_path = r"C:\Data\GDFLRawFiles\\"
#################################################################################################
print(currentDate)

link1 = "Data updation started..."
base_url =f"https://api.telegram.org/bot6240631533:AAEZgzjbx-PjlaaRKDfKpDTLlyPqTT5R1wY/sendMessage?chat_id=-876812744&text={link1}"
requests.get(base_url)



## Download GDFL bhavcopy from dropbox
st = time.time()
zip_file_url = zip_file_url.replace('?dl=0', '?dl=1')
r = requests.get(zip_file_url)
z = zipfile.ZipFile(io.BytesIO(r.content))
z.extractall(output_path)
#zipfile1 = zipfile.ZipFile(BytesIO(r.content))
file_list = sorted(z.namelist())
zip_file = file_list[1]#.replace('GFDLNFO_BACKADJUSTED', 'NSEFO')
print(zip_file)
print(file_list)

with zipfile.ZipFile(output_path + zip_file, 'r') as zip_ref:
    zip_ref.extractall(output_path)
    
date1 = datetime.strptime(zip_file[-12:-4], '%d%m%Y')

date = date1
print(date)

        
day = date.strftime('%d')
month=date.strftime('%B')
nummonth=date.strftime("%m")
year = str(date.year)
print(date)
print(year)
print(month)
print(day)

file_path = common_drive_path + year + '\\' + month + ' ' + year

print(common_drive_path + year + '\\' + month + ' ' + year)
if not os.path.exists(file_path):
    os.makedirs(file_path)    
df = pd.read_csv(output_path + zip_file.replace('.zip', '.csv'))
#print(df.head())
df['Date'] = pd.to_datetime(df['Date'], dayfirst=True).dt.date
uniqueDates = df['Date'].unique()

if len(uniqueDates)==1:
    print(len(uniqueDates))
    if uniqueDates[0]==currentDate:
        print('Dates match!')
    else:
        #raise Exception('Dates are not matching')
        print('Error! Dates are not matching')
        x = input("Press enter to exit")
        exit()
        
        
         
else:
    #raise Exception('Error!')
    print('Error!')
    x = input('Press enter to exit ')
    exit()

    
date1 = date1.strftime('%d%m%Y')
print(file_path + '\\' + 'NSEFO_' + date1 + '.csv')
df.to_csv(file_path + '\\' + 'NSEFO_' + date1 + '.csv', index=False)

# delete extracted GDFL files from downloads folder
print(date1)
try:
    os.remove(output_path + f'GFDLNFO_BACKADJUSTED_{date1}.csv')
except:
    pass
try:
    os.remove(output_path + f'GFDLNFO_BACKADJUSTED_{date1}.zip')
except:
    pass
try:
    os.remove(output_path + f'GFDLNFO_CONTRACT_{date1}.zip')
except:
    pass
    
et = time.time()
print(f'Time taken to run : {(et-st):.2f} secs')

################################################################################
################################################################################
## Daily Update Raw Database

#import the modules
import os
import pandas as pd
import psycopg2
import time
from io import StringIO
from datetime import datetime
#from datetime import date
#from datetime import datetime
import calendar

###################################### INPUTS #############################################################
#date = date(2023, 4, 5) # date.today() #date.today()
#date = date.today()
###########################################################################################################
print(date)
print(date)

print(type(date))
day = date.strftime('%d')
nummonth=date.strftime("%m")
year=str(date.year)
month = date.strftime('%B')

print(date)
print(year)
print(month)
print(day)

st=time.time()
conn = psycopg2.connect(database="RawDataBase",
                        user='postgres', password='swancap123',
                        host='swandatabase.cfehmk2wtejq.ap-south-1.rds.amazonaws.com', port='5432'
)

conn.autocommit = True
cursor = conn.cursor()

#read the path
file_path = r"C:\\Data\\GDFLRawFiles\\"
#file_path = r"C:\Users\ADMIN\Downloads\\"

csvfile = "NSEFO_" + str(day) + str(nummonth) + str(date.year) + ".csv"
print(csvfile)

file = file_path + '//' + year + '//' + month + ' ' + year 

df_append = pd.DataFrame()
df = pd.read_csv(file + '//' + csvfile)
vname = df.columns[-2]
name = df.columns[-1]
df[vname] = ['{:d}'.format(int(x)) for x in df[vname]]
df[name] = ['{:d}'.format(int(x)) for x in df[name]]
tablename = "r" + csvfile[-12:-4]

print(tablename)
datevalue = csvfile[-12:-4]
Date1 = csvfile[-12:-10] + "-" + csvfile[-10:-8] + "-" + csvfile[-8:-4]
print(Date1)

sql = '''DROP TABLE IF EXISTS ''' + tablename
cursor.execute(sql)

s = '''CREATE TABLE IF NOT EXISTS ''' + tablename + '''(Ticker varchar(50) NOT NULL,Date date,Time time,Open float,High float,Low float,Close float,Volume bigint,"Open Interest" bigint);'''
cursor.execute(s)
conn.commit()
   
buffer = StringIO()
  
    
df.to_csv(buffer, index = False)
buffer.seek(0)

sql = "COPY %s FROM STDIN WITH CSV HEADER DELIMITER AS ','"
with conn.cursor() as cur:
    #cur.execute("truncate " + table + ";")
    cur.copy_expert(sql=sql % tablename, file=buffer)
    conn.commit()
 
 
    
    
s = '''Select 1 from rawinfo where name=\'''' + tablename + '''\';'''
cursor.execute(s)
k = cursor.fetchall()
print(k)
#z=k[0]
#print(z[0])
if(k == []):
    sql3 ='''INSERT INTO rawinfo(NAME,Date) VALUES (%s,%s);'''
    record_to_insert = (tablename,Date1)
    cursor.execute(sql3,record_to_insert)

print('Number of rows before committing : ', df.shape[0])
conn.commit()

s = '''Select count(*) from '''+tablename+''';'''
cursor.execute(s)
k=cursor.fetchall()
print('Number of rows after committing : ', k[0][0])


print("sql2 done")
                        
conn.close()
print("sql done")
et=time.time()

elapsed_time=et-st;
print("elapsed_time:",elapsed_time)

