try:

    import requests, zipfile, io
    import pandas as pd
    from io import BytesIO
    from datetime import datetime, date
    import os
    import calendar
    import time

    #################################################################################
    #################################################################################



    with open (r"C:\Data\CsvFiles\ErrorLog.txt", 'w') as file:  
        file.write('Uploading to database...') 

    ################################# INPUTS ########################################################
    zip_file_url = input()#"https://www.dropbox.com/sh/up7jncgjmrumj2p/AAB2ev3msAXeQr0a7QGvr557a?dl=0"
    #date = currentDate = date(2023, 7, 28) # date.today() #date.today()
    date = currentDate = date.today()
    output_path = r"C:\Users\Administrator\Downloads\\"
    common_drive_path = r"C:\Data\GDFLRawFiles\\"
    #################################################################################################
    print(currentDate)

    # print message on telegram group
    link1 = "Data updation started..."
    base_url =f"https://api.telegram.org/bot6237928541:AAHl267HrSFBRFE-iIajz_x8eNkPydiQEEs/sendMessage?chat_id=-939411532&text={link1}"
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

    with open (r"C:\Data\CsvFiles\ErrorLog.txt", 'w') as file:  
        file.write('Uploading to database finished!')

    ################################################################################
    ################################################################################

    #!/usr/bin/env python
    # coding: utf-8

    # In[2]:


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
    #startDate = date(2023,6,12)
    #endDate = date(2023,6,15)
    #startDate = endDate = date.today()
    startDate = endDate = date
    print('Stock Option Updation starts - ', startDate)

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
        #data["Ticker"] = data["Ticker"].str.replace("BAJAJ-AUTO","BAJAJ_AUTO")
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
    with open (r"C:\Data\CsvFiles\ErrorLog.txt", 'w') as file:  
        file.write('Symbolwise files creation finished!')

    conn.close()
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
             
    print(datetime.now())

    with open (r"C:\Data\CsvFiles\ErrorLog.txt", 'w') as file:  
        file.write('Labelling finished!')
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

    with open (r"C:\Data\CsvFiles\ErrorLog.txt", 'w') as file:  
        file.write('Continous contracts creation finished!')
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




##    symbols = ['CANFINHOME', 'BHARATFORG', 'BHEL', 'LT', 'GODREJCP',
##               'BIOCON', 'CONCOR', 'JSWSTEEL']
    for i in range(len(symbols)):
        symbols[i] = symbols[i].replace('-III.csv', '').replace('-II.csv', '').replace('-I.csv', '')    
    symbols = sorted(list(set(symbols)))
    print(len(symbols))
##    ignore_symbols = ['CANFINHOME', 'BHARATFORG', 'BHEL', 'LT', 'GODREJCP',
##                      'BIOCON', 'CONCOR', 'JSWSTEEL']
##    print(len(symbols))
##    symbols = np.setdiff1d(symbols, ignore_symbols)
    print(len(symbols))


    hyphens = ['I', 'II', 'III']
    numberOfRowsFinal = 0
    j = 0
    numberOfRowsDF['FinalRows'] = 0
    date = date.date()
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
                #newSymbol = symbol.replace('&', '_')
                newSymbol = re.sub('\&|\-', '_', symbol)
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
##                print(startDate)
##                print(type(startDate))
##                print(date)
##                print(type(date))
                #exit()
                #date = date.date()
##                sql2 = f'''DELETE FROM "MonthlyRaw{i}"."{tablename}" WHERE "Date" = '{date}' '''
##                #print(sql2)
##                cursor.execute(sql2)
##                sql3 = f'''DELETE FROM "Monthly{i}"."{tablename}" WHERE "Date" = '{date}' '''
##                cursor.execute(sql3)
                

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
    conn.close()
    print(numberOfRowsRaw)
    print(numberOfRowsFinal)
    #numberOfRowsDF.to_csv(r"C:\Users\Administrator\Desktop\data3.csv", index=False)

    ############################################################################
    ###################### Right Issue Adjustment ##############################
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



    rightIssueFile = r"C:\Data\CsvFiles\RightsIssue.xlsx"
    x = pd.read_excel(rightIssueFile)
    x['Ex. Date'] = pd.to_datetime(x['Ex. Date'], dayfirst=True).dt.date

    ##    startDate = date(2023,5,17)
    ##    endDate = date(2023,5,17)
    print(type(startDate))
    print(startDate)
    try:
        startDate = datetime.strptime(str(startDate), '%Y-%m-%d').date()
        endDate = datetime.strptime(str(endDate), '%Y-%m-%d').date()
    except:
        startDate = startDate.date()
        endDate = endDate.date()
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
                print(key)
            
                date1 = value.iloc[0]['Ex. Date']
                print(date1)
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
                    print("May be table does not exist...") 
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
                    print(split_dict)
                
                
                
                    hyphens = ['I', 'II', 'III'] 
                    for i in hyphens:
                        #print(i)
                        try:
                            df = pd.DataFrame()
                            engine = pg.connect("dbname='StockOptions' user='postgres' host='swandatabase.cfehmk2wtejq.ap-south-1.rds.amazonaws.com' port='5432' password='swancap123'")
                            df = pd.read_sql(f'select * from "Monthly{i}"."{symbol}-{i}"', con=engine)
                        except Exception as e:
                            print(e)
                            print("May be table does not exist...") 
                        engine.close()
                        if df.empty:
                            print('Table does not exist')
                        else:
                            print(i)
                            print('Table exists')
                            df['Date'] = pd.to_datetime(df['Date'].astype(str), format='mixed', dayfirst=True).dt.date
                            df['Adj_strike'] = df['Ticker'].str.replace(f'{symbol}-{i}', '').str.replace('CE', '').str.replace('PE', '')
                            #df['Adj_strike'] = df['Ticker'].str.replace('ALSTOMT_D', '').str.replace('CE', '').str.replace('PE', '').str.replace('-I', '')
                            df['Adj_strike'] = df['Adj_strike'].astype(float)
                            #print(df)

                            sym1 = symbol
                            df = df.rename(columns={'Open' : 'Adj_Open',
                                                    'High' : 'Adj_High',
                                                    'Low' : 'Adj_Low',
                                                    'Close' : 'Adj_Close',
                                                    'Volume' : 'Adj_volume',
                                                    'Open_Interest' : 'Adj_OI'})
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

                                    df['Adj_strike'] = np.where(df['Date'] < split_dict[sym1].iloc[j]['Ex. Date'], 
                                                                df['Adj_strike'] / split_dict[sym1].iloc[j]['Split_Ratio'], df['Adj_strike'])

                                    df['Adj_volume'] = np.where(df['Date'] < split_dict[sym1].iloc[j]['Ex. Date'], 
                                                                df['Adj_volume'] * split_dict[sym1].iloc[j]['Split_Ratio'], df['Adj_volume'])

                                    df['Adj_OI'] = np.where(df['Date'] < split_dict[sym1].iloc[j]['Ex. Date'], 
                                                                df['Adj_OI'] * split_dict[sym1].iloc[j]['Split_Ratio'], df['Adj_OI'])

                            print(df)
                            df['Final_strike'] = df['Adj_strike']
                            df['month'] = pd.to_datetime(df['Date'], dayfirst=True).dt.month

                            # strike round off
                            df['Final_strike'] = df['Adj_strike'].apply(lambda x : round(x / 0.05) * 0.05)
                            df['Temp1'] = df['Final_strike']%df['Final_strike'].astype(int)
                            df['Final_strike'] = np.where(df['Temp1']==0, df['Final_strike'].astype(int), df['Final_strike'])
                            df = df.drop(['Temp1'], axis=1)
                            df['rem'] = df['Final_strike'] % df['Final_strike'].astype(int)
                            df['Option_type'] = df['Ticker'].str[-2:]
                            df.loc[df['rem'] == 0, 'Ticker'] = sym1 + '-' + i + df['Final_strike'].astype(int).astype(str) + df['Option_type']
                            df.loc[df['rem'] != 0, 'Ticker'] = sym1 + '-' + i + df['Final_strike'].round(2).astype(str) + df['Option_type'] 
                            df = df[['Ticker', 'Date', 'Time', 'Adj_Open','Adj_High','Adj_Low','Adj_Close','Adj_volume', 'Adj_OI']]


                            conn = psycopg2.connect(database="StockOptions",
                                user='postgres', password='swancap123',
                                host='swandatabase.cfehmk2wtejq.ap-south-1.rds.amazonaws.com', port='5432')

                            conn.autocommit = True
                            cursor = conn.cursor()
                            #cursor = conn.cursor()
                            sql1 = f'''DROP TABLE IF EXISTS "Monthly{i}"."{sym1}-{i}" CASCADE'''
                            print(sql1)
                            #sql1 = '''DROP table IF EXISTS "MonthlyI"."ALSTOMT_D-I"  CASCADE'''
                            print(sql1)
                            cursor.execute(sql1)
                            conn.commit()
                            #conn.close()

                            sql2=f'''CREATE TABLE IF NOT EXISTS "Monthly{i}"."{sym1}-{i}"("Ticker" varchar(50),"Date" Date NOT NULL, "Time" time NOT NULL, "Open" float NOT NULL, "High" float NOT NULL, "Low" float NOT NULL, "Close" float NOT NULL, "Volume" float NOT NULL, "Open_Interest" float NOT NULL);'''
                            print(sql2)
                            cursor.execute(sql2)

                            schema = 'Monthly' + i

                            buffer = StringIO()
                            print(df)
                            df.to_csv(buffer, index = False)
                            buffer.seek(0)
                            sql = "COPY %s FROM STDIN WITH CSV HEADER DELIMITER AS ','"
                            table = f'"{schema}"."{sym1}-{i}"'

                            with conn.cursor() as cur:
                                cur.copy_expert(sql = sql % table, file=buffer)
                                conn.commit()
                            conn.close()
        

    else:
        print('There are no right issue adjustments!')

    #####  Split and Bonus Adjustment
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


    ######################### SPLIT and BONUS #####################################
    splitAndBonusFile = r"C:\Data\CsvFiles\AllAdjustments.csv"
    #input_path = r"D:\Sourav\Data\UpdatedTill23Dec2022\\"
    #final_data_path = input_path
    #output_path = r"E:\sourav\Database\SpliAndBonus\\"
    ##startDate = date.today()
    ##endDate = date.today()
    ##for file in next(os.walk(output_path))[2]:
    ##    os.remove(output_path + file)

    x = pd.read_csv(splitAndBonusFile)
    x = x.loc[:, ~x.columns.str.contains('^Unnamed')] 
    x['Ex. Date'] = pd.to_datetime(x['Ex. Date'], dayfirst=True).dt.date
    print(startDate)
    print(type(startDate))
    try:
        startDate = datetime.strptime(str(startDate), '%Y-%m-%d').date()
        endDate = datetime.strptime(str(endDate), '%Y-%m-%d').date()
    except:
        startDate = startDate.date()
        endDate = endDate.date()
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

    hyphens = ['I', 'II', 'III']

    print('Checking for Split and Bonus adjustments...')
    if split_dict:
        print('Performing Split and Bonus adjustments...')
        for key, value in split_dict.items():
            print(key)
            
            
            split_ratio = list(value['Split_Ratio'])[0]
            symbol = key
            for i in hyphens:
                print(i)
                try:
                    df = pd.DataFrame()
                    engine = pg.connect("dbname='StockOptions' user='postgres' host='swandatabase.cfehmk2wtejq.ap-south-1.rds.amazonaws.com' port='5432' password='swancap123'")
                    df = pd.read_sql(f'select * from "Monthly{i}"."{symbol}-{i}"', con=engine)
                    
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
                    df['Adj_strike'] = df['Ticker'].str.replace(f'{symbol}-{i}', '').str.replace('CE', '').str.replace('PE', '')
                    #df['Adj_strike'] = df['Ticker'].str.replace('ALSTOMT_D', '').str.replace('CE', '').str.replace('PE', '').str.replace('-I', '')
                    df['Adj_strike'] = df['Adj_strike'].astype(float)
                    print(df)
                    
                    sym1 = symbol
                    df = df.rename(columns={'Open' : 'Adj_Open',
                                            'High' : 'Adj_High',
                                            'Low' : 'Adj_Low',
                                            'Close' : 'Adj_Close',
                                            'Volume' : 'Adj_volume',
                                            'Open_Interest' : 'Adj_OI'})
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

                            df['Adj_strike'] = np.where(df['Date'] < split_dict[sym1].iloc[j]['Ex. Date'], 
                                                        df['Adj_strike'] / split_dict[sym1].iloc[j]['Split_Ratio'], df['Adj_strike'])

                            df['Adj_volume'] = np.where(df['Date'] < split_dict[sym1].iloc[j]['Ex. Date'], 
                                                        df['Adj_volume'] * split_dict[sym1].iloc[j]['Split_Ratio'], df['Adj_volume'])

                            df['Adj_OI'] = np.where(df['Date'] < split_dict[sym1].iloc[j]['Ex. Date'], 
                                                        df['Adj_OI'] * split_dict[sym1].iloc[j]['Split_Ratio'], df['Adj_OI'])

                    df['Final_strike'] = df['Adj_strike']
                    df['month'] = pd.to_datetime(df['Date'], dayfirst=True).dt.month
                    
                    # strike round off
                    df['Final_strike'] = df['Adj_strike'].apply(lambda x : round(x / 0.05) * 0.05)
                    df['Temp1'] = df['Final_strike']%df['Final_strike'].astype(int)
                    df['Final_strike'] = np.where(df['Temp1']==0, df['Final_strike'].astype(int), df['Final_strike'])
                    df = df.drop(['Temp1'], axis=1)
                    df['rem'] = df['Final_strike'] % df['Final_strike'].astype(int)
                    df['Option_type'] = df['Ticker'].str[-2:]
                    df.loc[df['rem'] == 0, 'Ticker'] = sym1 + '-' + i + df['Final_strike'].astype(int).astype(str) + df['Option_type']
                    df.loc[df['rem'] != 0, 'Ticker'] = sym1 + '-' + i + df['Final_strike'].round(2).astype(str) + df['Option_type'] 
                    df = df[['Ticker', 'Date', 'Time', 'Adj_Open','Adj_High','Adj_Low','Adj_Close','Adj_volume', 'Adj_OI']]

                    conn = psycopg2.connect(database="StockOptions",
                                user='postgres', password='swancap123',
                                host='swandatabase.cfehmk2wtejq.ap-south-1.rds.amazonaws.com', port='5432')

                    conn.autocommit = True
                    cursor = conn.cursor()
                    
                    #cursor = conn.cursor()
                    sql1 = f'''DROP TABLE IF EXISTS "Monthly{i}"."{sym1}-{i}" CASCADE'''
                    print(sql1)
                    #sql1 = '''DROP table IF EXISTS "MonthlyI"."ALSTOMT_D-I"  CASCADE'''
                    print(sql1)
                    cursor.execute(sql1)
                    conn.commit()
                    #conn.close()
                    
                    
                    sql2=f'''CREATE TABLE IF NOT EXISTS "Monthly{i}"."{sym1}-{i}"("Ticker" varchar(50),"Date" Date NOT NULL, "Time" time NOT NULL, "Open" float NOT NULL, "High" float NOT NULL, "Low" float NOT NULL, "Close" float NOT NULL, "Volume" float NOT NULL, "Open_Interest" float NOT NULL);'''
                    print(sql2)
                    cursor.execute(sql2)
                    
                    schema = 'Monthly' + i
            
                    buffer = StringIO()
                    df.to_csv(buffer, index = False)
                    buffer.seek(0)
                    sql = "COPY %s FROM STDIN WITH CSV HEADER DELIMITER AS ','"
                    table = f'"{schema}"."{sym1}-{i}"'

                    with conn.cursor() as cur:
                        cur.copy_expert(sql = sql % table, file=buffer)
                        conn.commit()
              
                    conn.close()
    else:
        print('There are no Split and Bonus adjustments!')
    with open (r"C:\Data\CsvFiles\ErrorLog.txt", 'w') as file:  
        file.write('Stock Option Updation finished!')

    link1 = "Stock options updation finished!"
    base_url =f"https://api.telegram.org/bot6237928541:AAHl267HrSFBRFE-iIajz_x8eNkPydiQEEs/sendMessage?chat_id=-939411532&text={link1}"
    requests.get(base_url)

    input('Press enter to exit...')
    exit()

    ############################################################################
    ############################################################################
                            #INDEX OPTIONS
    ############################################################################
    ############################################################################

    #!/usr/bin/env python
    # coding: utf-8

    # In[3]:


    import pyspark
    import numpy as np
    import pandas as pd
    import os
    from os import walk
    from pyspark.sql import SparkSession
    from pyspark.sql import Row
    from pyspark.sql.functions import regexp_replace
    from pyspark.sql.functions import array_contains
    from pyspark.sql.functions import *
    import time
    from pyspark.sql.functions import date_format
    from datetime import date
    import datetime
    from datetime import datetime
    import re
    import warnings
    warnings.filterwarnings("ignore")

    path = 'Administrator'
    #date1 = input('Enter a date in YYYY-MM-DD format ')                                     ## TO BE CHANGED DAILY AS PER UPDATION DATE
    #date1 = currentDate
    #year , month , day = map(int,date1.split('-'))
    #day , month = f"{day:02d}",f"{month:02d}"
    #date1 = date.today()
    date1 = date(2023,5,19)
    day = date1.strftime('%d')
    nummonth=date1.strftime("%m")
    year=date1.strftime('%Y')
    tablename="r"+str(day)+str(nummonth)+str(year)
    print(tablename)

    st=time.time()

    def banknifty_data():

        spark = SparkSession.builder.config("spark.jars", "C:\\Users\\Administrator\\Downloads\\postgresql-42.5.1.jar") \
        .master("local").appName("PySpark_Postgres_test").getOrCreate()
        
        df = spark.read.format("jdbc").option("url", "jdbc:postgresql://swandatabase.cfehmk2wtejq.ap-south-1.rds.amazonaws.com/RawDataBase").option("user","postgres").option("password","swancap123")\
            .option("driver", "org.postgresql.Driver").option("dbtable", tablename)\
            .option("user", "postgres").option("password", "swancap123").load()

        ## GETTING ONLY TIME IN TIME COLUMN
        q = df.withColumn('time',date_format('time', 'HH:mm:ss'))
        bndata = q.filter(q.ticker.contains('BANKNIFTY') & ((q.ticker.endswith('E.NFO'))| (q.ticker.endswith('E'))))
        ## REPLACING .NFO IN ticker
        bndata = bndata.withColumn('ticker',regexp_replace('ticker','.NFO',''))

        ## CONVERTING PYSPARK DATAFRAME TO PANDAS DATAFRAME
        bndata = bndata.toPandas()
        return bndata

    def banknifty_monthly():
        
        ## CHECKING IF PATH DOES NOT EXIST, THEN CREATE PATH
        if not os.path.exists(r"C:\Users\Administrator\Desktop\Pyspark_Contracts\BankNifty\Monthly_data\\"):
            os.makedirs(r"C:\Users\Administrator\Desktop\Pyspark_Contracts\BankNifty\Monthly_data\\")
            
        if not os.path.exists(r"C:\users\Administrator\desktop\Pyspark\BankNifty\Monthly\\"):
            os.makedirs(r"C:\users\Administrator\desktop\Pyspark\BankNifty\Monthly\\")
        
        folpath = r"C:\Users\Administrator\Desktop\Pyspark_Contracts\BankNifty\Monthly_data\\"
        sym = 'BANKNIFTY'
        start_time = datetime.strptime('09:15:00', '%H:%M:%S').time()
        end_time = datetime.strptime('15:30:00', '%H:%M:%S').time()
        
        def add(stri):
            obj = datetime.strptime(stri, "%b")
            month_number = obj.month
            return month_number

        ## READING THE EXPIRY SHEET
        exp_file_path = r"C:\Users\Administrator\Downloads\MonthlyExpiry.csv"
        exp_df = pd.read_csv(exp_file_path,parse_dates=["curr_exp_date","curr_date"],dayfirst=True).dropna()
        ## CONVERTING TO PANDAS DATAFRAME
        exp_df.rename({'curr_date': 'New_date'}, axis=1, inplace=True)
        exp_df['New_date'] = pd.to_datetime(exp_df['New_date'],dayfirst=True)
        exp_date = pd.read_excel(r'C:\users\Administrator\desktop\Expiry_DT.xlsx')
        
        ## CALLING FUNCTION BANKNIFTY_DATA TO GENERATE ONLY BANKNIFTY TICKERS
        ddf = banknifty_data()
        ddf = ddf.loc[:, ~ddf.columns.str.contains('^Unnamed')]
        ddf['date'] = pd.to_datetime(ddf['date'],dayfirst=True)
        ddf['Optiontype'] = ddf['ticker'].str[-2:]
        ddf['Temp'] = ddf['ticker'].str.replace('BANKNIFTY','')
        ddf['Temp'] = ddf['Temp'].str[:-2]
        ddf['Length_of_temp'] = np.where(ddf['Temp'].str.len()==12,12,ddf['Temp'].str.len())
        ddf['Strike'] = np.where((ddf['Temp'].str.len()==12)|(ddf['Temp'].str.len()==10),ddf['Temp'].str[-5:],
                                 ddf['Temp'].str[-4:])
        ddf['Exp_year'] = np.where(ddf['Temp'].str.len()==12,ddf['Temp'].str[5:7],ddf['Temp'].str[:2])
        ddf['Exp_month'] = ddf['Temp'].str[2:5]
        ddf['Exp_year'] = ddf['Exp_year'].astype('str')
        ddf['MonthYear'] = ddf['Exp_month'] + ddf['Exp_year']
        merged_df = pd.merge(ddf,exp_date,on='MonthYear')
        merged_df = merged_df.drop(['MonthYear','Month','Year'],axis=1)
        merged_df['Length_of_temp'] = merged_df['Length_of_temp'].astype('int64')
        df_10 = merged_df[(merged_df['Length_of_temp']==10) | (merged_df['Length_of_temp']==9)]
        df_12 = merged_df[merged_df['Length_of_temp']==12]
        df_12['DateDate'] = df_12['Temp'].str[:2]
        df_12['DateDate'] = df_12['DateDate'].astype('int64')
        df_12['Exp_DT'] = pd.to_datetime(merged_df['Exp_DT'],dayfirst=True)
        df_12['Exp_Day'] = df_12['Exp_DT'].dt.day
        df_12 = df_12[df_12['Exp_Day']==df_12['DateDate']]
        df_12 = df_12.drop(['DateDate','Exp_Day'],axis=1)
        ddf = pd.concat([df_10,df_12],axis=0,ignore_index=True)
    #     ddf = df_10.append(df_12,ignore_index=True)
        ddf['time'] = ddf['time'].str.replace(' 15:00:59','15:00:59')
        ddf['time'] = ddf['time'].str.replace(' 9:','09:',regex=True)
        ddf['time'] = pd.to_datetime(ddf['time'], format='%H:%M:%S').dt.time
        ddf = ddf[(ddf['time']>=start_time) & (ddf['time']<=end_time)]
        ddf['exp_month_number'] = ddf.apply(lambda row : add(row["Exp_month"]), axis = 1)
        ddf['New_date'] = ddf['date']
        ddf["New_date"] = pd.to_datetime(ddf["New_date"],dayfirst=True)
        ddf["current_month_number"] = ddf['New_date'].dt.month
        ddf["difference"] = ddf['exp_month_number'].astype(int) - ddf["current_month_number"].astype(int)
        df1 = pd.merge(ddf, 
                         exp_df, 
                         on ='New_date', 
                         how ='left')
        df1.drop(df1.filter(regex="Unname"),axis=1, inplace=True)
        df1["current_exp_month_number"] = df1['curr_exp_date'].dt.month
        df1["Diff_months"] = df1["current_exp_month_number"] - df1["current_month_number"]
        df1["Diff_months"] = df1["Diff_months"].astype(int) 
        
        bdf = df1[df1["Diff_months"] == 0]
        adf = df1[(df1["Diff_months"] == 1) | (df1["Diff_months"] == -11)]
        if bdf.shape[0] + adf.shape[0] == df1.shape[0]:
            print("Sanity Check Success")
        else:
            print("Error1")
        agb = adf.groupby(["difference"])
        unique_val_list_a = list(adf["difference"].unique())
        bgb = bdf.groupby(["difference"])
        unique_val_list_b = list(bdf["difference"].unique())
        
        ## REMOVING YESTERDAY'S CREATED FILE
        if os.path.exists(folpath+sym+'-I.csv'):
            os.remove(folpath+sym+'-I.csv')
        if os.path.exists(folpath+sym+'-II.csv'):
            os.remove(folpath+sym+'-II.csv')
        if os.path.exists(folpath+sym+'-III.csv'):
            os.remove(folpath+sym+'-III.csv')
        if os.path.exists(folpath+sym+'misc.csv'):
            os.remove(folpath+sym+'misc.csv')
            
        for i in unique_val_list_b:
            temp_df_new = bgb.get_group(i)
            temp_df_new = temp_df_new.drop(temp_df_new.columns[9:],axis=1)
            if i == 0:
                temp_df_new.to_csv(folpath + sym + '-I.csv', mode = 'a', header = not os.path.exists(folpath + sym + '-I.csv'), index=False)
            if i == 1 or i == -11:
                temp_df_new.to_csv(folpath + sym + '-II.csv', mode = 'a', header = not os.path.exists(folpath + sym + '-II.csv'), index=False)
            if i == 2 or i == -10:
                temp_df_new.to_csv(folpath + sym + '-III.csv', mode = 'a', header = not os.path.exists(folpath + sym + '-III.csv'), index=False)
            
        for i in unique_val_list_a:
            temp_df_new = agb.get_group(i)
            temp_df_new = temp_df_new.drop(temp_df_new.columns[9:],axis=1)
            if i == 1 or i == -11:
                temp_df_new.to_csv(folpath + sym + '-I.csv', mode = 'a', header = not os.path.exists(folpath + sym + '-I.csv'), index=False)
            if i == 2 or i == -10:
                temp_df_new.to_csv(folpath + sym + '-II.csv', mode = 'a', header = not os.path.exists(folpath + sym + '-II.csv'), index=False)
            if i == 3 or i == -9:
                temp_df_new.to_csv(folpath + sym + '-III.csv', mode = 'a', header = not os.path.exists(folpath + sym + '-III.csv'), index=False)
        
        ##########################CREATING LABEL IN STANDARD FORM#####################
        for i in range(3):
            if i == 0:
                file='I'
            elif i == 1:
                file='II'
            elif i == 2:
                file='III'
            
            if os.path.exists(r'C:\users\Administrator\desktop\Pyspark\BankNifty\Monthly\Banknifty-'+file+".csv"):
                os.remove(r'C:\users\Administrator\desktop\Pyspark\BankNifty\Monthly\Banknifty-'+file+".csv")
        
            if os.path.exists(r'C:\users\Administrator\desktop\Pyspark_Contracts\BankNifty\Monthly_Data\BANKNIFTY-'+file+'.csv'):
                ddf = pd.read_csv(r'C:\users\Administrator\desktop\Pyspark_Contracts\BankNifty\Monthly_Data\BANKNIFTY-'+file+'.csv')
                ddf['Option_Type'] = ddf['ticker'].str[-2:]
                ddf['Strike'] = np.where((ddf['ticker'].str.len()==20) | (ddf['ticker'].str.len()==22) , ddf['ticker'].str[-6:-2] , ddf['ticker'].str[-7:-2])
                ddf['Symbol'] = 'BANKNIFTY' + 'MONTHLY-' + file + ddf['Strike'].astype(int).astype(str) + ddf['Option_Type']
                ddf['ticker'] = ddf['Symbol']
                ddf = ddf.drop(ddf.columns[9:],axis=1)
                ddf = ddf.rename(columns = {"ticker":"Ticker"})
                ddf.to_csv(r"C:\Users\Administrator\Desktop\Pyspark\BankNifty\Monthly\\Banknifty-"+file+".csv",index=False)
        print("BANKNIFTY MONTHLY CONTRACTS CREATED")

    def banknifty_weekly():
        ## CHECKING IF PATH DOES NOT EXIST, THEN CREATE PATH
        if not os.path.exists(r"C:\Users\Administrator\Desktop\Pyspark_Contracts\BankNifty\Weekly_data\\"):
            os.makedirs(r"C:\Users\Administrator\Desktop\Pyspark_Contracts\BankNifty\Weekly_data\\")
            
        if not os.path.exists(r"C:\users\Administrator\desktop\Pyspark\BankNifty\Weekly\\"):
            os.makedirs(r"C:\users\Administrator\desktop\Pyspark\BankNifty\Weekly\\")
            
        folpath = r"C:\users\Administrator\desktop\\Pyspark_Contracts\\BankNifty\\Weekly_Data\\"
        sym = 'BANKNIFTY'
        s = 'BANKNIFTY'
        expiry_time = datetime.strptime('15:29:59', '%H:%M:%S').time()
        
        ## CALLING FUNCTION NIFTY_DATA TO GENERATE ONLY NIFTY TICKERS
        df=banknifty_data()
        
        ## READING WEEKLY EXPIRY FILES
        exp_df = pd.read_csv(r"C:\Users\Administrator\Downloads\WeeklyExpiry.csv",parse_dates = ["date"],dayfirst =True,usecols= ['date', 'Week_number'])
        exp_date = pd.read_excel(r'C:\users\Administrator\desktop\Expiry_DT.xlsx',parse_dates = ['Exp_DT'],usecols = ['MonthYear','Exp_DT'])
        df['date'] = pd.to_datetime(df['date'])
        df['time'] = df['time'].str.replace(' 15:00:59','15:00:59')
        df['time'] = df['time'].str.replace(' 9:','09:',regex=True)
        df['time'] = pd.to_datetime(df['time']).dt.time
        df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
        df = df[df['time'] <= expiry_time]
        df['EXPIRY_DT'] = df['ticker'].str[9:16]
        df['EXPIRY_DT'] = pd.to_datetime(df['EXPIRY_DT'],dayfirst=True)
        df['OPTION_TYP'] = df['ticker'].str[-2:]
        df['STRIKE_PR'] = np.where(df['ticker'].str.len()==23,df['ticker'].str[-7:-2],df['ticker'].str[-7:-2])
        df['Month'] = df['ticker'].str[11:14]
        df['Year'] = np.where(df['ticker'].str.len()==23,df['ticker'].str[14:16],df['ticker'].str[9:11])
        df['MonthYear'] = df['Month'] + df['Year']
        df = df.rename(columns={'EXPIRY_DT' : 'expiry_date'})

        ## MERGING WITH EXPIRY SHEET TO GET EXPIRY DATE
        df1 = pd.merge(df,exp_df,on='date',how='left')
        df1 = df1.drop_duplicates()
        df1 = pd.merge(df1,exp_date,on='MonthYear',how='left')
        df1 = df1.drop(['Month','Year','MonthYear'],axis=1)

        ## GETTING THE EXPIRY DATES FOR MONTHLY CONTRACTS
        df1['expiry_date'] = np.where(df1['ticker'].str.len()>21,df1['expiry_date'],df1['Exp_DT'])
        df1 = df1.drop(['Exp_DT'],axis=1)
        exp_df = pd.read_csv(r"C:\Users\Administrator\Downloads\WeeklyExpiry.csv",parse_dates = ["Weekly_Expiry_Date"],dayfirst =True,usecols= ['Weekly_Expiry_Date', 'Expiry_Week_number'])
        exp_df = exp_df.dropna()
        exp_df = exp_df.rename(columns = {'Weekly_Expiry_Date': 'expiry_date'})
        df2 = pd.merge(df1, exp_df, on ='expiry_date', how ='left')
        df2 = df2.drop_duplicates()
        df2['week_diff'] = df2['Expiry_Week_number'] - df2['Week_number']

        final_df = df2[(df2["OPTION_TYP"] == "CE") | (df2["OPTION_TYP"] == "PE") ]
        final_df["week_diff"] = final_df['week_diff'].replace(np.nan,10000)
        
        agb = final_df.groupby(["week_diff"])
        unique_val_list_a = list(final_df["week_diff"].unique())
        unique_val_list_a = sorted([a for a in unique_val_list_a if a>=0])[0:12]
        print(unique_val_list_a)

        ## CREATING -I,-II AND SO ON BASED ON THE WEEK DIFFERENCES

        if os.path.exists(folpath+sym+'-I.csv'):
            os.remove(folpath+sym+'-I.csv')
        if os.path.exists(folpath+sym+'-II.csv'):
            os.remove(folpath+sym+'-II.csv')
        if os.path.exists(folpath+sym+'-III.csv'):
            os.remove(folpath+sym+'-III.csv')
        if os.path.exists(folpath+sym+'-IV.csv'):
            os.remove(folpath+sym+'-IV.csv')
        if os.path.exists(folpath+sym+'-V.csv'):
            os.remove(folpath+sym+'-V.csv')
        if os.path.exists(folpath+sym+'-VI.csv'):
            os.remove(folpath+sym+'-VI.csv')
        if os.path.exists(folpath+sym+'-VII.csv'):
            os.remove(folpath+sym+'-VII.csv')
        if os.path.exists(folpath+sym+'-VIII.csv'):
            os.remove(folpath+sym+'-VIII.csv')
        if os.path.exists(folpath+sym+'-IX.csv'):
            os.remove(folpath+sym+'-IX.csv')
        if os.path.exists(folpath+sym+'-X.csv'):
            os.remove(folpath+sym+'-X.csv')
        if os.path.exists(folpath+sym+'-XI.csv'):
            os.remove(folpath+sym+'-XI.csv')
        if os.path.exists(folpath+sym+'-XII.csv'):
            os.remove(folpath+sym+'-XII.csv')  
        if os.path.exists(folpath+sym+'-XIII.csv'):
            os.remove(folpath+sym+'-XIII.csv')  
        if os.path.exists(folpath+sym+'-XIV.csv'):
            os.remove(folpath+sym+'-XIV.csv')  
            
        for i in sorted(unique_val_list_a):
            temp_df = agb.get_group(i)
            temp_df = temp_df.drop(temp_df.columns[9:],axis=1)
            temp_df = temp_df.drop_duplicates()
            if i == 0:
                temp_df.to_csv(folpath + s + '-I.csv', mode = 'a', header = not os.path.exists(folpath + s + '-I.csv'), index=False)
            if i == 1:
                temp_df.to_csv(folpath + s + '-II.csv', mode = 'a', header = not os.path.exists(folpath + s + '-II.csv'), index=False)
            if i == 2:
                temp_df.to_csv(folpath + s + '-III.csv', mode = 'a', header = not os.path.exists(folpath + s + '-III.csv'), index=False)
            if i == 3:
                temp_df.to_csv(folpath + s + '-IV.csv', mode = 'a', header = not os.path.exists(folpath + s + '-IV.csv'), index=False)
            if i == 4:
                temp_df.to_csv(folpath + s + '-V.csv', mode = 'a', header = not os.path.exists(folpath + s + '-V.csv'), index=False)
            if i == 5:
                temp_df.to_csv(folpath + s + '-VI.csv', mode = 'a', header = not os.path.exists(folpath + s + '-VI.csv'), index=False)
            if i == 6:
                temp_df.to_csv(folpath + s + '-VII.csv', mode = 'a', header = not os.path.exists(folpath + s + '-VII.csv'), index=False)
            if i == 7:
                temp_df.to_csv(folpath + s + '-VIII.csv', mode = 'a', header = not os.path.exists(folpath + s + '-VIII.csv'), index=False)
            if i == 8:
                temp_df.to_csv(folpath + s + '-IX.csv', mode = 'a', header = not os.path.exists(folpath + s + '-IX.csv'), index=False)
            if i == 9:
                temp_df.to_csv(folpath + s + '-X.csv', mode = 'a', header = not os.path.exists(folpath + s + '-X.csv'), index=False)
            if i == 10:
                temp_df.to_csv(folpath + s + '-XI.csv', mode = 'a', header = not os.path.exists(folpath + s + '-XI.csv'), index=False)
            if i == 11:
                temp_df.to_csv(folpath + s + '-XII.csv', mode = 'a', header = not os.path.exists(folpath + s + '-XII.csv'), index=False)
            if i == 12:
                temp_df.to_csv(folpath + s + '-XIII.csv', mode = 'a', header = not os.path.exists(folpath + s + '-XIII.csv'), index=False)
            if i == 13:
                temp_df.to_csv(folpath + s + '-XIV.csv', mode = 'a', header = not os.path.exists(folpath + s + '-XIV.csv'), index=False)
        
        #################LABELLING FILES IN STANDARD FORM######################
        for i in range(15):
            if i==0:
                file='I'
            elif i==1:
                file='II'
            elif i==2:
                file='III'
            elif i==3:
                file='IV'
            elif i==4:
                file='V'
            elif i==5:
                file='VI'
            elif i==6:
                file='VII'
            elif i==7:
                file='VIII'
            elif i==8:
                file='IX'
            elif i==9:
                file='X'
            elif i==10:
                file='XI'
            elif i==11:
                file='XII'
            elif i==12:
                file='XIII'
            elif i==13:
                file='XIV'
            if os.path.exists(r'C:\users\Administrator\desktop\Pyspark\BankNifty\Weekly\BANKNIFTY-'+file+'.csv'):
                os.remove(r'C:\users\Administrator\desktop\Pyspark\BankNifty\Weekly\BANKNIFTY-'+file+'.csv')
            if os.path.exists(r'C:\users\Administrator\desktop\Pyspark_Contracts\BankNifty\Weekly_Data\BANKNIFTY-'+file+'.csv'):
                ddf = pd.read_csv(r'C:\users\Administrator\desktop\Pyspark_Contracts\BankNifty\Weekly_Data\BANKNIFTY-'+file+'.csv')
                ddf['Option_Type'] = ddf['ticker'].str[-2:]
                ddf['Strike'] = np.where((ddf['ticker'].str.len()==20) | (ddf['ticker'].str.len()==22) , ddf['ticker'].str[-6:-2] , ddf['ticker'].str[-7:-2])
                ddf['Symbol'] = 'BANKNIFTY' + 'WEEKLY-' + file + + ddf['Strike'].astype(int).astype(str) + ddf['Option_Type']
                ddf['ticker'] = ddf['Symbol']
                ddf = ddf.drop(ddf.columns[9:],axis=1)
                ddf = ddf.rename(columns = {'date':'Date','ticker':'Ticker'})
                ddf = ddf.drop_duplicates()
                ddf.to_csv(r"C:\Users\Administrator\Desktop\Pyspark\BankNifty\Weekly\BANKNIFTY-"+file+".csv",index=False)
        print("BANKNIFTY WEEKLY CONTRACTS CREATED")
        
    def banknifty_quarterly():
        ## CHECKING IF PATH DOES NOT EXIST, THEN CREATE PATH
        if not os.path.exists(r"C:\Users\Administrator\Desktop\Pyspark_Contracts\BankNifty\Quarterly_data\\"):
            os.makedirs(r"C:\Users\Administrator\Desktop\Pyspark_Contracts\BankNifty\Quarterly_data\\")
            
        if not os.path.exists(r"C:\users\Administrator\desktop\Pyspark\BankNifty\Quarterly\\"):
            os.makedirs(r"C:\users\Administrator\desktop\Pyspark\BankNifty\Quarterly\\")
            
        folpath = r"C:\users\Administrator\desktop\\Pyspark_Contracts\\BankNifty\\Quarterly_Data\\"
        sym = 'BANKNIFTY'
        start_time = datetime.strptime('09:15:00', '%H:%M:%S').time()
        end_time = datetime.strptime('15:30:00', '%H:%M:%S').time()

        ## CALLING FUNCTION BANKNIFTY_DATA TO GENERATE ONLY BANKNIFTY TICKERS
        temp = banknifty_data()
        
        def add(stri):
            obj = datetime.strptime(stri, "%b")
            month_number = obj.month
            return month_number

        exp_date = pd.read_excel(r'C:\users\Administrator\desktop\Expiry_DT.xlsx')    ## reading the expiry sheet file
        exp_file_path = r"C:\Users\Administrator\Downloads\MonthlyExpiry.csv"
        exp_df = pd.read_csv(exp_file_path,parse_dates = ["curr_exp_date","curr_date"],dayfirst =True,usecols = ["curr_exp_date","curr_date"]).dropna()
        exp_df.rename({'curr_date': 'New_date'}, axis=1, inplace=True)
        temp['time'] = temp['time'].astype(str).str.replace(' 15:00:59','15:00:59')
        temp['time'] = temp['time'].str.replace(' 9:','09:',regex=True)
        temp['ticker'] = temp['ticker'].str.replace('30MAR23','29MAR23',regex=True)
        temp['time'] = pd.to_datetime(temp['time']).dt.time
        temp['date'] = pd.to_datetime(temp['date'])
        temp = temp[(temp['time']>=start_time) & (temp['time']<=end_time)]
        temp = temp.loc[:, ~temp.columns.str.contains('^Unnamed')]
        temp['Option_type'] = temp['ticker'].str[-2:]
        temp["Temp"] = temp["ticker"].str.replace('BANKNIFTY',"")
        temp["Temp"] = temp["Temp"].str[:-2]
        temp["Strike"] = np.where((temp['Temp'].str.len()==12) | (temp['Temp'].str.len()==10),
                                    temp['Temp'].str[-5:],
                                    temp['Temp'].str[-4:])
        temp['Current_Year'] = temp['date'].dt.year
        temp['Current_Year'] = temp['Current_Year'].astype(str).str[-2:]
        temp["Exp_year"] = np.where(temp['Temp'].str.len()==12,temp["Temp"].str[5:7],temp['Temp'].str[:2])
        temp["Exp_month"] = temp["Temp"].str[2:5]
        temp['Length_of_Temp'] = np.where(temp['Temp'].str.len()==12,12,temp['Temp'].str.len())
        temp['Exp_year'] = temp['Exp_year'].astype('str')
        temp['MonthYear'] = temp['Exp_month']+temp['Exp_year']
        temp = pd.merge(temp,exp_date,on='MonthYear')
        temp = temp.drop(['MonthYear','Month','Year'],axis=1)

        temp['Length_of_Temp'] = temp['Length_of_Temp'].astype('int64')
        temp_10 = temp[(temp['Length_of_Temp']==10) | (temp['Length_of_Temp']==9)]
        temp_12 = temp[temp['Length_of_Temp']==12]
        temp_12['datedate'] = temp_12['Temp'].str[:2]
        temp_12['datedate'] = temp_12['datedate'].astype('int64')
        temp_12['Exp_DT'] = pd.to_datetime(temp['Exp_DT'],dayfirst=True)
        temp_12['Exp_Day'] = temp_12['Exp_DT'].dt.day
        temp_12 = temp_12[temp_12['Exp_Day']==temp_12['datedate']]

        temp_12 = temp_12.drop(['datedate','Exp_Day'],axis=1)
    #     temp = temp_10.append(temp_12,ignore_index=True)
        temp = pd.concat([temp_10,temp_12],axis=0,ignore_index=True)
        temp['exp_month_number'] = temp.apply(lambda row : add(row["Exp_month"]), axis = 1)
        temp['New_date'] = temp['date']
        temp["New_date"] = pd.to_datetime(temp["New_date"])
        temp["current_month_number"] = temp['New_date'].dt.month
        temp["difference"] = temp['exp_month_number'].astype(int) - temp["current_month_number"].astype(int)
        temp['Year_difference'] = temp['Exp_year'].astype(int) - temp['Current_Year'].astype(int)
        temp = temp[(temp['exp_month_number']==3) | (temp['exp_month_number']==6) | (temp['exp_month_number']==9) | (temp['exp_month_number']==12)]

        temp1 = pd.merge(temp, 
                             exp_df, 
                             on ='New_date', 
                             how ='left')

        temp1.drop(temp1.filter(regex="Unname"),axis=1, inplace=True)
        temp1["current_exp_month_number"] = temp1['curr_exp_date'].dt.month
        temp1["Diff_months"] = temp1["current_exp_month_number"] - temp1["current_month_number"]
        temp1["Diff_months"] = temp1["Diff_months"].astype(int) 

        temp1 = temp1[temp1['Exp_DT']>=temp1['curr_exp_date']]                 ## to filter out dates which have wrong ticker

        ## creating groups for generating contracts
        atemp = temp1[(temp1['Diff_months']==0) & (temp1['Year_difference']==0)]
        agb = atemp.groupby(['difference'])
        unique_a = list(atemp['difference'].unique())

        if os.path.exists(folpath+sym+'-I.csv'):
            os.remove(folpath+sym+'-I.csv')
        if os.path.exists(folpath+sym+'-II.csv'):
            os.remove(folpath+sym+'-II.csv')
        if os.path.exists(folpath+sym+'-III.csv'):
            os.remove(folpath+sym+'-III.csv')
        if os.path.exists(folpath+sym+'-IV.csv'):
            os.remove(folpath+sym+'-IV.csv')
        if os.path.exists(folpath+sym+'-V.csv'):
            os.remove(folpath+sym+'-V.csv')
        if os.path.exists(folpath+sym+'-VI.csv'):
            os.remove(folpath+sym+'-VI.csv')
        if os.path.exists(folpath+sym+'-VII.csv'):
            os.remove(folpath+sym+'-VII.csv')
        if os.path.exists(folpath+sym+'-VIII.csv'):
            os.remove(folpath+sym+'-VIII.csv')    

        for i in unique_a:
            temp_df = agb.get_group(i)
            temp_df = temp_df.drop(temp_df.columns[9:],axis=1)
            if i==0 or i==1 or i==2:
                temp_df.to_csv(folpath + sym + '-I.csv', mode='a', header=not os.path.exists(folpath + sym + '-I.csv'), index=False)
            if i==3 or i==4 or i==5:
                temp_df.to_csv(folpath + sym + '-II.csv', mode='a', header=not os.path.exists(folpath + sym + '-II.csv'), index=False)
            if i==6 or i==7 or i==8:
                temp_df.to_csv(folpath + sym + '-III.csv', mode='a', header=not os.path.exists(folpath + sym + '-III.csv'), index=False)
            if i==9 or i==10 or i==11:
                temp_df.to_csv(folpath + sym + '-IV.csv', mode='a', header=not os.path.exists(folpath + sym + '-IV.csv'), index=False)

        btemp = temp1[(temp1['Diff_months']==0) & (temp1['Year_difference']==1)]
        bgb = btemp.groupby(['difference'])
        unique_b = list(btemp['difference'].unique())        

        for i in unique_b:
            temp_df = bgb.get_group(i)
            temp_df = temp_df.drop(temp_df.columns[9:],axis=1)
            if i==-7 or i==-8 or i==-9:
                temp_df.to_csv(folpath + sym + '-II.csv', mode='a', header=not os.path.exists(folpath + sym + '-II.csv'), index=False)
            if i==-4 or i==-5 or i==-6:
                temp_df.to_csv(folpath + sym + '-III.csv', mode='a', header=not os.path.exists(folpath + sym + '-III.csv'), index=False)
            if i==-1 or i==-2 or i==-3:
                temp_df.to_csv(folpath + sym + '-IV.csv', mode='a', header=not os.path.exists(folpath + sym + '-IV.csv'), index=False)
            if i==0 or i==1 or i==2:
                temp_df.to_csv(folpath + sym + '-V.csv', mode='a', header=not os.path.exists(folpath + sym + '-V.csv'), index=False)
            if i==3 or i==4 or i==5:
                temp_df.to_csv(folpath + sym + '-VI.csv', mode='a', header=not os.path.exists(folpath + sym + '-VI.csv'), index=False)
            if i==6 or i==7 or i==8:
                temp_df.to_csv(folpath + sym + '-VII.csv', mode='a', header=not os.path.exists(folpath + sym + '-VII.csv'), index=False)
            if i==9 or i==10 or i==11:
                temp_df.to_csv(folpath + sym + '-VIII.csv', mode='a', header=not os.path.exists(folpath + sym + '-VIII.csv'), index=False)

        ctemp = temp1[(temp1['Diff_months']==0) & (temp1['Year_difference']==2)]
        cgb = ctemp.groupby(['difference'])
        unique_c = list(ctemp['difference'].unique())        

        for i in unique_c:
            temp_df = cgb.get_group(i)
            temp_df = temp_df.drop(temp_df.columns[9:],axis=1)
            if i==-7 or i==-8 or i==-9:
                temp_df.to_csv(folpath + sym + '-VI.csv', mode='a', header=not os.path.exists(folpath + sym + '-VI.csv'), index=False)
            if i==-4 or i==-5 or i==-6:
                temp_df.to_csv(folpath + sym + '-VII.csv', mode='a', header=not os.path.exists(folpath + sym + '-VII.csv'), index=False)
            if i==-1 or i==-2 or i==-3:
                temp_df.to_csv(folpath + sym + '-VIII.csv', mode='a', header=not os.path.exists(folpath + sym + '-VIII.csv'), index=False)

        dtemp = temp1[((temp1['Diff_months']==1) | (temp1['Diff_months']==-11)) & (temp1['Year_difference']==0)]
        dgb = dtemp.groupby(['difference'])
        unique_d = list(dtemp['difference'].unique())

        for i in unique_d:
            temp_df = dgb.get_group(i)
            temp_df = temp_df.drop(temp_df.columns[9:],axis=1)
            if i==1 or i==2 or i==3:
                temp_df.to_csv(folpath + sym + '-I.csv', mode='a', header=not os.path.exists(folpath + sym + '-I.csv'), index=False)
            if i==4 or i==5 or i==6:
                temp_df.to_csv(folpath + sym + '-II.csv', mode='a', header=not os.path.exists(folpath + sym + '-II.csv'), index=False)
            if i==7 or i==8 or i==9:
                temp_df.to_csv(folpath + sym + '-III.csv', mode='a', header=not os.path.exists(folpath + sym + '-III.csv'), index=False)
            if i==10 or i==11:
                temp_df.to_csv(folpath + sym + '-IV.csv', mode='a', header=not os.path.exists(folpath + sym + '-IV.csv'), index=False)

        etemp = temp1[((temp1['Diff_months']==1) | (temp1['Diff_months']==-11)) & (temp1['Year_difference']==1)]
        egb = etemp.groupby(['difference'])
        unique_e = list(etemp['difference'].unique())

        for i in unique_e:
            temp_df = egb.get_group(i)
            temp_df = temp_df.drop(temp_df.columns[9:],axis=1)
            if i==-9:
                temp_df.to_csv(folpath + sym + '-I.csv', mode='a', header=not os.path.exists(folpath + sym + '-I.csv'), index=False)
            if i==-6 or i==-7:
                temp_df.to_csv(folpath + sym + '-II.csv', mode='a', header=not os.path.exists(folpath + sym + '-II.csv'), index=False)
            if i==-3 or i==-4:
                temp_df.to_csv(folpath + sym + '-III.csv', mode='a', header=not os.path.exists(folpath + sym + '-III.csv'), index=False)
            if i==0 or i==-1 or i==-2:
                temp_df.to_csv(folpath + sym + '-IV.csv', mode='a', header=not os.path.exists(folpath + sym + '-IV.csv'), index=False)
            if i==1 or i==2 or i==3:
                temp_df.to_csv(folpath + sym + '-V.csv', mode='a', header=not os.path.exists(folpath + sym + '-V.csv'), index=False)
            if i==4 or i==5 or i==6:
                temp_df.to_csv(folpath + sym + '-VI.csv', mode='a', header=not os.path.exists(folpath + sym + '-VI.csv'), index=False)
            if i==7 or i==8 or i==9:
                temp_df.to_csv(folpath + sym + '-VII.csv', mode='a', header=not os.path.exists(folpath + sym + '-VII.csv'), index=False)
            if i==10 or i==11:
                temp_df.to_csv(folpath + sym + '-VIII.csv', mode='a', header=not os.path.exists(folpath + sym + '-VIII.csv'), index=False)

        ftemp = temp1[((temp1['Diff_months']==1) | (temp1['Diff_months']==-11)) & (temp1['Year_difference']==2)]
        fgb = ftemp.groupby(['difference'])
        unique_f = list(ftemp['difference'].unique())

        for i in unique_f:
            temp_df = fgb.get_group(i)
            temp_df = temp_df.drop(temp_df.columns[9:],axis=1)
            if i==-9:
                temp_df.to_csv(folpath + sym + '-V.csv', mode='a', header=not os.path.exists(folpath + sym + '-V.csv'), index=False)
            if i==-6 or i==-7:
                temp_df.to_csv(folpath + sym + '-VI.csv', mode='a', header=not os.path.exists(folpath + sym + '-VI.csv'), index=False)
            if i==-3 or i==-4:
                temp_df.to_csv(folpath + sym + '-VII.csv', mode='a', header=not os.path.exists(folpath + sym + '-VII.csv'), index=False)
            if i==0 or i==-1 or i==-2:
                temp_df.to_csv(folpath + sym + '-VIII.csv', mode='a', header=not os.path.exists(folpath + sym + '-VIII.csv'), index=False)
        
        ## CREATING THE TICKER AND REMOVING ADDITIONAL COLUMNS
        for i in range(4):
            if i==0:
                file='I'
            elif i==1:
                file='II'
            elif i==2:
                file='III'
            elif i==3:
                file='IV'
            if os.path.exists(r"C:\Users\Administrator\Desktop\Pyspark\BankNifty\Quarterly\Banknifty-"+file+".csv"):
                os.remove(r"C:\Users\Administrator\Desktop\Pyspark\BankNifty\Quarterly\Banknifty-"+file+".csv")
            if os.path.exists(r"C:\Users\Administrator\Desktop\Pyspark_Contracts\BankNifty\Quarterly_data\BANKNIFTY-"+file+".csv"):
                ddf = pd.read_csv(r"C:\Users\Administrator\Desktop\Pyspark_Contracts\BankNifty\Quarterly_data\BANKNIFTY-"+file+".csv")
                ddf['Option_Type'] = ddf['ticker'].str[-2:]
                ddf['Strike'] = np.where((ddf['ticker'].str.len()==20) | (ddf['ticker'].str.len()==22) , ddf['ticker'].str[-6:-2] , ddf['ticker'].str[-7:-2])
                ddf['Symbol'] = 'BANKNIFTY' + 'QUARTERLY-' + file + + ddf['Strike'].astype(int).astype(str) + ddf['Option_Type']
                ddf['ticker'] = ddf['Symbol']
                ddf = ddf.drop(ddf.columns[9:],axis=1)
                ddf = ddf.rename(columns = {'date':'Date','ticker':'Ticker'})
                ddf = ddf.drop_duplicates()
                ddf.to_csv(r"C:\Users\Administrator\Desktop\Pyspark\BankNifty\Quarterly\Banknifty-"+file+".csv",index=False)
        print("BANKNIFTY QUARTERLY CONTRACTS CREATED")
        
    def banknifty_halfyearly():
        ## CHECKING IF PATH DOES NOT EXIST, THEN CREATE PATH
        if not os.path.exists(r"C:\users\Administrator\desktop\Pyspark\BankNifty\Half_Yearly\\"):
            os.makedirs(r"C:\users\Administrator\desktop\Pyspark\BankNifty\Half_Yearly\\")
        
        if os.path.exists(r"C:\Users\Administrator\Desktop\Pyspark\BankNifty\\Half_Yearly\\Banknifty-I.csv"):
            os.remove(r"C:\Users\Administrator\Desktop\Pyspark\BankNifty\\Half_Yearly\\Banknifty-I.csv")

        if os.path.exists(r"C:\Users\Administrator\Desktop\Pyspark\BankNifty\\Half_Yearly\\Banknifty-II.csv"):
            os.remove(r"C:\Users\Administrator\Desktop\Pyspark\BankNifty\\Half_Yearly\\Banknifty-II.csv")
            
        final_df = pd.DataFrame()
        file = 'I'
        for i in range(2):
            df = pd.read_csv(r"C:\Users\Administrator\Desktop\Pyspark_Contracts\BankNifty\\Quarterly_Data\BANKNIFTY-"+str(file)+".csv")
            temp = df.copy()
            temp = temp.rename(columns = {'ticker':'Ticker','date':'Date','time':'Time','open':'Open','low':'Low','high':'High','close':'Close','volume':'Volume'})
            temp['Time'] = pd.to_datetime(temp['Time']).dt.time
            temp['Date'] = pd.to_datetime(temp['Date'])
            temp = temp.loc[:, ~temp.columns.str.contains('^Unnamed')]
            temp['Option_type'] = temp['Ticker'].str[-2:]
            temp["Temp"] = temp["Ticker"].str.replace('BANKNIFTY',"")
            temp["Temp"] = temp["Temp"].str[:-2]
            temp["Strike"] = np.where((temp['Temp'].str.len()==12) | (temp['Temp'].str.len()==10),
                                        temp['Temp'].str[-5:],
                                        temp['Temp'].str[-4:])
            temp['Current_Year'] = temp['Date'].dt.year
            temp['Current_Year'] = temp['Current_Year'].astype(str).str[-2:]
            temp["Exp_year"] = np.where(temp['Temp'].str.len()==12,temp["Temp"].str[5:7],temp['Temp'].str[:2])
            temp["Exp_month"] = temp["Temp"].str[2:5]
            temp['Length_of_Temp'] = np.where(temp['Temp'].str.len()==12,12,temp['Temp'].str.len())
            temp = temp[(temp['Exp_month']=='JUN') | (temp['Exp_month']=='DEC')]
            temp = temp.reset_index(drop=True)
    #         final_df = final_df.append(temp)
            final_df = pd.concat([final_df,temp],axis=0,ignore_index=True)
            final_df = final_df.reset_index(drop=True)
            final_df = final_df.drop(final_df.columns[9:],axis=1)
            file+=file

        if final_df.empty==False:    
            test = final_df.copy()
            test['Option_Type'] = test['Ticker'].str[-2:]
            test['Last'] = test['Ticker'].str[-7:]
            test['Strike'] = test['Last'].astype('str').str.extractall('(\d+)').unstack().fillna('').sum(axis=1).astype(int)
            test['Symbol'] = test['Ticker'].str[:9] + '-I' + test['Strike'].astype(str) + test['Option_Type'].astype(str)
            test['Ticker'] = test['Symbol']
            test = test.drop(test.columns[9:13],axis=1)
            print("BANKNIFTY HALF-YEARLY-I CREATED")
            test.to_csv(r"C:\Users\Administrator\Desktop\Pyspark\BankNifty\\Half_Yearly\\Banknifty-I.csv",index=False)


        final_df = pd.DataFrame()
        file = 'III'
        for i in range(2):
            if os.path.exists(r"C:\Users\Administrator\Desktop\Pyspark_Contracts\BankNifty\\Quarterly_Data\BANKNIFTY-"+str(file)+".csv"):
                df = pd.read_csv(r"C:\Users\Administrator\Desktop\Pyspark_Contracts\BankNifty\\Quarterly_Data\BANKNIFTY-"+str(file)+".csv")
                temp = df.copy()
                temp = temp.rename(columns = {'ticker':'Ticker','date':'Date','time':'Time','open':'Open','low':'Low','high':'High','close':'Close','volume':'Volume'})
                temp['Time'] = pd.to_datetime(temp['Time']).dt.time
                temp['Date'] = pd.to_datetime(temp['Date'])
                temp = temp.loc[:, ~temp.columns.str.contains('^Unnamed')]
                temp['Option_type'] = temp['Ticker'].str[-2:]
                temp["Temp"] = temp["Ticker"].str.replace('BANKNIFTY',"")
                temp["Temp"] = temp["Temp"].str[:-2]
                temp["Strike"] = np.where((temp['Temp'].str.len()==12) | (temp['Temp'].str.len()==10),
                                            temp['Temp'].str[-5:],
                                            temp['Temp'].str[-4:])
                temp['Current_Year'] = temp['Date'].dt.year
                temp['Current_Year'] = temp['Current_Year'].astype(str).str[-2:]
                temp["Exp_year"] = np.where(temp['Temp'].str.len()==12,temp["Temp"].str[5:7],temp['Temp'].str[:2])
                temp["Exp_month"] = temp["Temp"].str[2:5]
                temp['Length_of_Temp'] = np.where(temp['Temp'].str.len()==12,12,temp['Temp'].str.len())
                temp = temp[(temp['Exp_month']=='JUN') | (temp['Exp_month']=='DEC')]
                temp = temp.reset_index(drop=True)
    #             final_df = final_df.append(temp)
                final_df = pd.concat([final_df,temp],axis=0,ignore_index=True)
                final_df = final_df.reset_index(drop=True)
                final_df = final_df.drop(final_df.columns[9:],axis=1)
                file='IV'
            else:
                print("BANKNIFTY-QUARTERLY-"+str(file)+' not found')
                file='IV'

        if final_df.empty==False:
            test = final_df.copy()
            test['Option_Type'] = test['Ticker'].str[-2:]
            test['Last'] = test['Ticker'].str[-7:]
            test['Strike'] = test['Last'].astype('str').str.extractall('(\d+)').unstack().fillna('').sum(axis=1).astype(int)
            test['Symbol'] = test['Ticker'].str[:9] + '-II' + test['Strike'].astype(str) + test['Option_Type'].astype(str)
            test['Ticker'] = test['Symbol']
            test = test.drop(test.columns[9:13],axis=1)
            print("BANKNIFTY HALF-YEARLY-II CREATED")
            test.to_csv(r"C:\Users\Administrator\Desktop\Pyspark\BankNifty\\Half_Yearly\\Banknifty-II.csv",index=False)
        print("BANKNIFTY HALFYEARLY CONTRACTS CREATED")

    def banknifty_yearly():
        ## CHECKING IF PATH DOES NOT EXIST, THEN CREATE PATH
        if not os.path.exists(r"C:\users\Administrator\desktop\Pyspark\BankNifty\Yearly\\"):
            os.makedirs(r"C:\users\Administrator\desktop\Pyspark\BankNifty\Yearly\\")
            
        if os.path.exists(r"C:\Users\Administrator\Desktop\Pyspark\BankNifty\\Yearly\\Banknifty-I.csv"):
            os.remove(r"C:\Users\Administrator\Desktop\Pyspark\BankNifty\\Yearly\\Banknifty-I.csv")

        final_df = pd.DataFrame()
        file = 'I'
        add_file = 'I'
        for i in range(4):
            print(i,file)
            if os.path.exists(r"C:\Users\Administrator\Desktop\Pyspark_Contracts\BankNifty\\Quarterly_Data\BANKNIFTY-"+str(file)+".csv"):  
                df = pd.read_csv(r"C:\Users\Administrator\Desktop\Pyspark_Contracts\BankNifty\\Quarterly_Data\BANKNIFTY-"+str(file)+".csv")
                temp = df.copy()
                temp = temp.rename(columns = {'ticker':'Ticker','date':'Date','time':'Time','open':'Open','low':'Low','high':'High','close':'Close','volume':'Volume'})
                temp['Time'] = pd.to_datetime(temp['Time']).dt.time
                temp['Date'] = pd.to_datetime(temp['Date'])
                temp = temp.loc[:, ~temp.columns.str.contains('^Unnamed')]
                temp['Option_type'] = temp['Ticker'].str[-2:]
                temp["Temp"] = temp["Ticker"].str.replace('BANKNIFTY',"")
                temp["Temp"] = temp["Temp"].str[:-2]
                temp["Strike"] = np.where((temp['Temp'].str.len()==12) | (temp['Temp'].str.len()==10),
                                            temp['Temp'].str[-5:],
                                            temp['Temp'].str[-4:])
                temp['Current_Year'] = temp['Date'].dt.year
                temp['Current_Year'] = temp['Current_Year'].astype(str).str[-2:]
                temp["Exp_year"] = np.where(temp['Temp'].str.len()==12,temp["Temp"].str[5:7],temp['Temp'].str[:2])
                temp["Exp_month"] = temp["Temp"].str[2:5]
                temp['Length_of_Temp'] = np.where(temp['Temp'].str.len()==12,12,temp['Temp'].str.len())
                temp = temp[(temp['Exp_month']=='DEC')]
                temp = temp.reset_index(drop=True)
    #             final_df = final_df.append(temp)
                final_df = pd.concat([final_df,temp],axis=0,ignore_index=True)
                final_df = final_df.reset_index(drop=True)
                final_df = final_df.drop(final_df.columns[9:],axis=1)
                if i==2:
                    file='IV'
                else:
                    file+=add_file

            else:
                print("BANKNIFTY QUARTERLY-"+str(file)+" does not exist")

        if final_df.empty==False:
            test = final_df.copy()
            test['Option_Type'] = test['Ticker'].str[-2:]
            test['Last'] = test['Ticker'].str[-7:]
            test['Strike'] = test['Last'].astype('str').str.extractall('(\d+)').unstack().fillna('').sum(axis=1).astype(int)
            test['Symbol'] = test['Ticker'].str[:9] + '-I' + test['Strike'].astype(str) + test['Option_Type'].astype(str)
            test['Ticker'] = test['Symbol']
            test = test.drop(test.columns[9:13],axis=1)
            print("BANKNIFTY YEARLY-I CREATED")
            test.to_csv(r"C:\Users\Administrator\Desktop\Pyspark\BankNifty\\Yearly\\Banknifty-I.csv",index=False)

        else:
            print("Dataframe is empty!!!")
        print("BANKNIFTY YEARLY CONTRACTS CREATED")

    with open (r"C:\Data\CsvFiles\ErrorLog.txt", 'w') as file:
        file.write('BankNifty contracts created!')
        
    def nifty_data():
        spark = SparkSession.builder.config("spark.jars", "C:\\Users\\Administrator\\Downloads\\postgresql-42.5.1.jar") \
        .master("local").appName("PySpark_Postgres_test").getOrCreate()
        
        df = spark.read.format("jdbc").option("url", "jdbc:postgresql://swandatabase.cfehmk2wtejq.ap-south-1.rds.amazonaws.com/RawDataBase").option("user","postgres").option("password","swancap123")\
            .option("driver", "org.postgresql.Driver").option("dbtable", tablename)\
            .option("user", "postgres").option("password", "swancap123").load()

        ## GETTING ONLY TIME IN TIME COLUMN
        q = df.withColumn('time',date_format('time', 'HH:mm:ss'))
        ndata = q.filter(q.ticker.contains('NIFTY') & ((q.ticker.endswith('E.NFO'))| (q.ticker.endswith('E'))))
        ndata = ndata.withColumn('ticker_check',substring('ticker',1,5))
        ndata = ndata.filter(ndata.ticker_check.contains('NIFTY'))
        ## REPLACING .NFO IN ticker
        ndata = ndata.withColumn('ticker',regexp_replace('ticker','.NFO',''))
        ndata = ndata.drop(col('ticker_check'))

        ## CONVERTING PYSPARK DATAFRAME TO PANDAS DATAFRAME
        ndata = ndata.toPandas()
        return ndata
        
    def nifty_monthly():
        
        ## CHECKING IF PATH DOES NOT EXIST, THEN CREATE PATH
        if not os.path.exists(rf"C:\Users\{path}\Desktop\Pyspark_Contracts\Nifty\Monthly_data\\"):
            os.makedirs(rf"C:\Users\{path}\Desktop\Pyspark_Contracts\Nifty\Monthly_data\\")
            
        if not os.path.exists(rf"C:\users\{path}\desktop\Pyspark\Nifty\Monthly\\"):
            os.makedirs(rf"C:\users\{path}\desktop\Pyspark\Nifty\Monthly\\")
            
        folpath = rf"C:\Users\{path}\Desktop\Pyspark_Contracts\Nifty\Monthly_Data\\"
        sym = 'NIFTY'
        start_time = datetime.strptime('09:15:00', '%H:%M:%S').time()
        end_time = datetime.strptime('15:30:00', '%H:%M:%S').time()
        expiry_time = datetime.strptime('15:29:59', '%H:%M:%S').time()
        s = 'NIFTY'

        def add(stri):
            obj = datetime.strptime(stri, "%b")
            month_number = obj.month
            return month_number

        def get_symbol(tic):
            li = list(filter(None, re.split(r'(\d+)', tic)))
            return li[0]

        exp_file_path = rf"C:\Users\{path}\Downloads\MonthlyExpiry.csv"
        exp_df = pd.read_csv(exp_file_path,parse_dates = ["curr_exp_date","curr_date"],dayfirst =True).dropna()
        exp_df.rename({'curr_date': 'New_date'}, axis=1, inplace=True)
        exp_date = pd.read_excel(rf'C:\users\{path}\desktop\Expiry_DT.xlsx')    ## reading the expiry sheet file
        
        ## CALLING FUNCTION NIFTY_DATA TO GENERATE ONLY NIFTY TICKERS
        ndata = nifty_data()
        
        temp = ndata.copy()
        temp = temp.loc[:, ~temp.columns.str.contains('^Unnamed')]
        temp['Symbol'] = temp['ticker'].str[:7]
        temp = temp[temp['Symbol']!='NIFTYIT']
        temp['time'] = pd.to_datetime(temp['time']).dt.time
        temp['ticker'] = temp['ticker'].str.replace('30MAR23','29MAR23',regex=True)
        temp['Option_Type'] = temp['ticker'].str[-2:]
        temp['Temp'] = temp["ticker"].str.replace(s,"")
        temp['Temp'] = temp['Temp'].str[:-2]
        temp['Length_of_temp'] = temp['Temp'].str.len()
        temp['Strike'] = np.where((temp['Temp'].str.len()==9) | (temp['Temp'].str.len()==11) , 
                                  temp['Temp'].str[-4:] , 
                                  temp['Temp'].str[-5:])
        temp['Exp_Year'] = np.where((temp['Temp'].str.len()==9) | (temp['Temp'].str.len()==10) ,
                                   temp['Temp'].str[:2] ,
                                   temp['Temp'].str[5:7])
        temp['Exp_month'] = temp['Temp'].str[2:5]
        temp['Exp_Year'] = temp['Exp_Year'].astype('str')
        temp['MonthYear'] = temp['Exp_month']+temp['Exp_Year']
        temp = pd.merge(temp,exp_date,on='MonthYear')
        temp = temp.drop(['MonthYear','Month','Year','Next_Exp_DT'],axis=1)
        temp['Length_of_temp'] = temp['Length_of_temp'].astype('int64')
        temp_10 = temp[(temp['Length_of_temp']==10) | (temp['Length_of_temp']==9)]

        temp_12 = temp[(temp['Length_of_temp']==12) | (temp['Length_of_temp']==11)]
        temp_12['DateDate'] = temp_12['Temp'].str[:2]
        temp_12['DateDate'] = temp_12['DateDate'].astype('int64')
        temp_12['Exp_DT'] = pd.to_datetime(temp_12['Exp_DT'],dayfirst=True)
        temp_12['Exp_Day'] = temp_12['Exp_DT'].dt.day
        temp_12 = temp_12[temp_12['Exp_Day']==temp_12['DateDate']]
        temp_12 = temp_12.drop(['DateDate','Exp_Day'],axis=1)

        temp_df = pd.concat([temp_10,temp_12],axis=0,ignore_index=True)
    #     temp_df = temp_10.append(temp_12,ignore_index=True)

        temp_df['time'] = temp_df['time'].astype(str).str.replace(' 15:00:59','15:00:59')
        temp_df['time'] = temp_df['time'].astype(str).str.replace(' 9:','09:',regex=True)
        temp_df['time'] = pd.to_datetime(temp_df['time'], format='%H:%M:%S').dt.time
        temp_df = temp_df[(temp_df['time']>=start_time) & (temp_df['time']<=end_time)]

        temp_df['exp_month_number'] = temp_df.apply(lambda row : add(row["Exp_month"]), axis = 1)
        temp_df['New_date'] = temp_df['date']
        temp_df["New_date"] = pd.to_datetime(temp_df["New_date"])
        temp_df["current_month_number"] = temp_df['New_date'].dt.month
        temp_df["difference"] = temp_df['exp_month_number'].astype(int) - temp_df["current_month_number"].astype(int)
        temp_df1 = pd.merge(temp_df, 
                     exp_df, 
                     on ='New_date', 
                     how ='left')
        temp_df1.drop(temp_df1.filter(regex="Unname"),axis=1, inplace=True)
        temp_df1["current_exp_month_number"] = temp_df1['curr_exp_date'].dt.month
        temp_df1["Diff_months"] = temp_df1["current_exp_month_number"] - temp_df1["current_month_number"]
        temp_df1["Diff_months"] = temp_df1["Diff_months"].astype(int) 
        temp_df1['Current_Year'] = temp_df1['New_date'].dt.year.astype(str).str[-2:]
        temp_df1['Flag'] = np.where((temp_df1['Current_Year']==temp_df1['Exp_Year']) | (temp_df1['current_month_number']==12) & (temp_df1['exp_month_number']<=3),1,0)
        temp_df1 = temp_df1[temp_df1['Flag']==1]
        bdf = temp_df1[temp_df1["Diff_months"] == 0]
        adf = temp_df1[(temp_df1["Diff_months"] == 1) | (temp_df1["Diff_months"] == -11)]
        if bdf.shape[0] + adf.shape[0] == temp_df1.shape[0]:
            print("Sanity Check Success")
        else:
            print("Error")

        agb = adf.groupby(["difference"])
        unique_val_list_a = list(adf["difference"].unique())
        bgb = bdf.groupby(["difference"])
        unique_val_list_b = list(bdf["difference"].unique())

        if os.path.exists(folpath+sym+'-I.csv'):
            os.remove(folpath+sym+'-I.csv')
        if os.path.exists(folpath+sym+'-II.csv'):
            os.remove(folpath+sym+'-II.csv')
        if os.path.exists(folpath+sym+'-III.csv'):
            os.remove(folpath+sym+'-III.csv')
        if os.path.exists(folpath+sym+'-misc.csv'):
            os.remove(folpath+sym+'-misc.csv')

        for i in unique_val_list_b:
            temp_df_new = bgb.get_group(i)
            temp_df_new = temp_df_new.drop(temp_df_new.columns[9:],axis=1)
            if i == 0:
                temp_df_new.to_csv(folpath + sym + '-I.csv', mode = 'a', header = not os.path.exists(folpath + sym + '-I.csv'), index=False)

            if i == 1 or i == -11:
                temp_df_new.to_csv(folpath + sym + '-II.csv', mode = 'a', header = not os.path.exists(folpath + sym + '-II.csv'), index=False)

            if i == 2 or i == -10:
                temp_df_new.to_csv(folpath + sym + '-III.csv', mode = 'a', header = not os.path.exists(folpath + sym + '-III.csv'), index=False)

        for i in unique_val_list_a:
            temp_df_new = agb.get_group(i)
            temp_df_new = temp_df_new.drop(temp_df_new.columns[9:],axis=1)
            if i == 1 or i == -11:
                temp_df_new.to_csv(folpath + sym + '-I.csv', mode = 'a', header = not os.path.exists(folpath + sym + '-I.csv'), index=False)

            if i == 2 or i == -10:
                temp_df_new.to_csv(folpath + sym + '-II.csv', mode = 'a', header = not os.path.exists(folpath + sym + '-II.csv'), index=False)

            if i == 3 or i == -9:
                temp_df_new.to_csv(folpath + sym + '-III.csv', mode = 'a', header = not os.path.exists(folpath + sym + '-III.csv'), index=False)

        ##########################CREATING LABEL IN STANDARD FORM#####################
        for i in range(3):
            if i == 0:
                file='I'
            elif i == 1:
                file='II'
            elif i == 2:
                file='III'
            if os.path.exists(rf'C:\users\{path}\desktop\Pyspark\Nifty\Monthly\NIFTY-'+file+".csv"):
                os.remove(rf'C:\users\{path}\desktop\Pyspark\Nifty\Monthly\NIFTY-'+file+".csv")
            if os.path.exists(rf'C:\users\{path}\desktop\Pyspark_Contracts\Nifty\Monthly_Data\NIFTY-'+file+'.csv'):
                ddf = pd.read_csv(rf'C:\users\{path}\desktop\Pyspark_Contracts\Nifty\Monthly_Data\NIFTY-'+file+'.csv')
                ddf['Option_Type'] = ddf['ticker'].str[-2:]
                ddf['Strike'] = np.where((ddf['ticker'].str.len()==16) | (ddf['ticker'].str.len()==18) , ddf['ticker'].str[-6:-2] , ddf['ticker'].str[-7:-2])
                ddf['Symbol'] = 'NIFTY' + 'MONTHLY-' + file + ddf['Strike'].astype(int).astype(str) + ddf['Option_Type']
                ddf['ticker'] = ddf['Symbol']
                ddf = ddf.drop(ddf.columns[9:],axis=1)
                ddf = ddf.rename(columns = {"ticker":"Ticker"})
                ddf.to_csv(rf"C:\Users\{path}\Desktop\Pyspark\Nifty\Monthly\\NIFTY-"+file+".csv",index=False)
        print("NIFTY MONTHLY CONTRACTS CREATED")

    def nifty_weekly():
        if not os.path.exists(rf"C:\Users\{path}\Desktop\Pyspark_Contracts\Nifty\Weekly_data\\"):
            os.makedirs(rf"C:\Users\{path}\Desktop\Pyspark_Contracts\Nifty\Weekly_data\\")
            
        if not os.path.exists(rf"C:\users\{path}\desktop\Pyspark\Nifty\Weekly\\"):
            os.makedirs(rf"C:\users\{path}\desktop\Pyspark\Nifty\Weekly\\")
            
        folpath = rf"C:\Users\{path}\Desktop\Pyspark_Contracts\Nifty\Weekly_Data\\"
        sym = 'NIFTY'
        start_time = datetime.strptime('09:15:00', '%H:%M:%S').time()
        end_time = datetime.strptime('15:30:00', '%H:%M:%S').time()
        expiry_time = datetime.strptime('15:29:59', '%H:%M:%S').time()
        s = 'NIFTY'
        def add(stri):
            obj = datetime.strptime(stri, "%b")
            month_number = obj.month
            return month_number
        def get_symbol(tic):
            li = list(filter(None, re.split(r'(\d+)', tic)))
            return li[0]
        exp_file_path = rf"C:\Users\{path}\Downloads\WeeklyExpiry.csv"
        exp_date = pd.read_excel(rf'C:\users\{path}\desktop\Expiry_DT.xlsx')    ## reading the expiry sheet file
        exp_df = pd.read_csv(exp_file_path,parse_dates = ["date"],dayfirst =True,usecols= ['date', 'Week_number'])
        exp_df.rename({'curr_date': 'New_date'}, axis=1, inplace=True)
        
        ## CALLING FUNCTION NIFTY_DATA TO GENERATE ONLY NIFTY TICKERS
        ndata=nifty_data()
        
        temp = ndata.copy()
        temp = temp.loc[:, ~temp.columns.str.contains('^Unnamed')]
        temp['Symbol'] = temp['ticker'].str[:7]
        temp = temp[temp['Symbol']!='NIFTYIT']
        temp = temp.reset_index(drop=True)
        temp['time'] = temp['time'].str.replace(' 15:00:59','15:00:59')
        temp['time'] = temp['time'].str.replace(' 9:','09:',regex=True)
        temp['time'] = pd.to_datetime(temp['time']).dt.time
        temp['date'] = pd.to_datetime(temp['date'])
        temp['ticker'] = temp['ticker'].str.replace('30MAR23','29MAR23',regex=True)
        temp['Option_Type'] = temp['ticker'].str[-2:]
        temp['Temp'] = temp["ticker"].str.replace(s,"")
        temp['Temp'] = temp['Temp'].str[:-2]
        temp['EXPIRY_DT'] = temp['Temp'].str[:7]
        temp['EXPIRY_DT'] = pd.to_datetime(temp['EXPIRY_DT'],dayfirst=True)
        temp['Length_of_temp'] = temp['Temp'].str.len()
        temp['Strike'] = np.where((temp['Temp'].str.len()==9) | (temp['Temp'].str.len()==11) , 
                                  temp['Temp'].str[-4:] , 
                                  temp['Temp'].str[-5:])
        temp['Exp_year'] = np.where((temp['Temp'].str.len()==9) | (temp['Temp'].str.len()==10) ,
                                   temp['Temp'].str[:2] ,
                                   temp['Temp'].str[5:7])
        temp['Exp_month'] = temp['Temp'].str[2:5]
        temp['MonthYear'] = temp['Exp_month'] + temp['Exp_year']
        temp = temp.rename(columns={'EXPIRY_DT' : 'expiry_date'})
        temp1 = pd.merge(temp,exp_df,on='date',how='left')
        temp1 = temp1.drop_duplicates()
        temp1 = pd.merge(temp1,exp_date,on='MonthYear',how='left')
        temp1 = temp1.drop(['Month','Year','MonthYear','Next_Exp_DT'],axis=1)

        ## GETTING EXPIRY DATES FOR MONTHLY CONTRACTS
        temp1['expiry_date'] = np.where(temp1['Length_of_temp']>=11,temp1['expiry_date'],temp1['Exp_DT'])
        temp1 = temp1.drop(['Exp_DT'],axis=1)
        exp_df = pd.read_csv(exp_file_path,parse_dates = ["Weekly_Expiry_Date"],dayfirst =True,usecols= ['Weekly_Expiry_Date', 'Expiry_Week_number'])
        exp_df = exp_df.dropna()
        exp_df = exp_df.rename(columns = {'Weekly_Expiry_Date':'expiry_date'})
        temp2 = pd.merge(temp1, exp_df, on = 'expiry_date', how = 'left')
        temp2 = temp2.drop_duplicates()
        temp2['week_diff'] = temp2['Expiry_Week_number'] - temp2['Week_number']
        final_df = temp2.copy()
        final_df["week_diff"] = final_df['week_diff'].replace(np.nan,10000)

        agb = final_df.groupby(["week_diff"])
        unique_val_list_a = list(final_df["week_diff"].unique())
        unique_val_list_a = sorted([a for a in unique_val_list_a if a>=0])[0:12]
        print(unique_val_list_a)

        ############CREATING -I,-II BASED ON WEEK DIFFERENCES###################
        if os.path.exists(folpath+sym+'-I.csv'):
            os.remove(folpath+sym+'-I.csv')
        if os.path.exists(folpath+sym+'-II.csv'):
            os.remove(folpath+sym+'-II.csv')
        if os.path.exists(folpath+sym+'-III.csv'):
            os.remove(folpath+sym+'-III.csv')
        if os.path.exists(folpath+sym+'-IV.csv'):
            os.remove(folpath+sym+'-IV.csv')
        if os.path.exists(folpath+sym+'-V.csv'):
            os.remove(folpath+sym+'-V.csv')
        if os.path.exists(folpath+sym+'-VI.csv'):
            os.remove(folpath+sym+'-VI.csv')
        if os.path.exists(folpath+sym+'-VII.csv'):
            os.remove(folpath+sym+'-VII.csv')
        if os.path.exists(folpath+sym+'-VIII.csv'):
            os.remove(folpath+sym+'-VIII.csv')
        if os.path.exists(folpath+sym+'-IX.csv'):
            os.remove(folpath+sym+'-IX.csv')
        if os.path.exists(folpath+sym+'-X.csv'):
            os.remove(folpath+sym+'-X.csv')
        if os.path.exists(folpath+sym+'-XI.csv'):
            os.remove(folpath+sym+'-XI.csv')
        if os.path.exists(folpath+sym+'-XII.csv'):
            os.remove(folpath+sym+'-XII.csv')  
        if os.path.exists(folpath+sym+'-XIII.csv'):
            os.remove(folpath+sym+'-XIII.csv')  
        if os.path.exists(folpath+sym+'-XIV.csv'):
            os.remove(folpath+sym+'-XIV.csv')  
            
        for i in sorted(unique_val_list_a):
            temp_df = agb.get_group(i)
            temp_df = temp_df.drop(temp_df.columns[9:],axis=1)
            temp_df = temp_df.drop_duplicates()
            if i == 0:
                temp_df.to_csv(folpath + s + '-I.csv', mode = 'a', header = not os.path.exists(folpath + s + '-I.csv'), index=False)

            if i == 1:
                temp_df.to_csv(folpath + s + '-II.csv', mode = 'a', header = not os.path.exists(folpath + s + '-II.csv'), index=False)

            if i == 2:
                temp_df.to_csv(folpath + s + '-III.csv', mode = 'a', header = not os.path.exists(folpath + s + '-III.csv'), index=False)

            if i == 3:
                temp_df.to_csv(folpath + s + '-IV.csv', mode = 'a', header = not os.path.exists(folpath + s + '-IV.csv'), index=False)

            if i == 4:
                temp_df.to_csv(folpath + s + '-V.csv', mode = 'a', header = not os.path.exists(folpath + s + '-V.csv'), index=False)

            if i == 5:
                temp_df.to_csv(folpath + s + '-VI.csv', mode = 'a', header = not os.path.exists(folpath + s + '-VI.csv'), index=False)

            if i == 6:
                temp_df.to_csv(folpath + s + '-VII.csv', mode = 'a', header = not os.path.exists(folpath + s + '-VII.csv'), index=False)

            if i == 7:
                temp_df.to_csv(folpath + s + '-VIII.csv', mode = 'a', header = not os.path.exists(folpath + s + '-VIII.csv'), index=False)

            if i == 8:
                temp_df.to_csv(folpath + s + '-IX.csv', mode = 'a', header = not os.path.exists(folpath + s + '-IX.csv'), index=False)

            if i == 9:
                temp_df.to_csv(folpath + s + '-X.csv', mode = 'a', header = not os.path.exists(folpath + s + '-X.csv'), index=False)

            if i == 10:
                temp_df.to_csv(folpath + s + '-XI.csv', mode = 'a', header = not os.path.exists(folpath + s + '-XI.csv'), index=False)

            if i == 11:
                temp_df.to_csv(folpath + s + '-XII.csv', mode = 'a', header = not os.path.exists(folpath + s + '-XII.csv'), index=False)

            if i == 12:
                temp_df.to_csv(folpath + s + '-XIII.csv', mode = 'a', header = not os.path.exists(folpath + s + '-XIII.csv'), index=False)

            if i == 13:
                temp_df.to_csv(folpath + s + '-XIV.csv', mode = 'a', header = not os.path.exists(folpath + s + '-XIV.csv'), index=False)

        #################LABELLING FILES IN STANDARD FORM######################
        for i in range(15):
            if i==0:
                file='I'
            elif i==1:
                file='II'
            elif i==2:
                file='III'
            elif i==3:
                file='IV'
            elif i==4:
                file='V'
            elif i==5:
                file='VI'
            elif i==6:
                file='VII'
            elif i==7:
                file='VIII'
            elif i==8:
                file='IX'
            elif i==9:
                file='X'
            elif i==10:
                file='XI'
            elif i==11:
                file='XII'
            elif i==12:
                file='XIII'
            elif i==13:
                file='XIV'
            if os.path.exists(rf'C:\users\{path}\desktop\Pyspark\Nifty\Weekly\NIFTY-'+file+'.csv'):
                os.remove(rf'C:\users\{path}\desktop\Pyspark\Nifty\Weekly\NIFTY-'+file+'.csv')
            if os.path.exists(rf'C:\users\{path}\desktop\Pyspark_Contracts\Nifty\Weekly_Data\NIFTY-'+file+'.csv'):
                ddf = pd.read_csv(rf'C:\users\{path}\desktop\Pyspark_Contracts\Nifty\Weekly_Data\NIFTY-'+file+'.csv')
                ddf['Option_Type'] = ddf['ticker'].str[-2:]
                ddf['Strike'] = np.where((ddf['ticker'].str.len()==16) | (ddf['ticker'].str.len()==18) , ddf['ticker'].str[-6:-2] , ddf['ticker'].str[-7:-2])
                ddf['Symbol'] = 'NIFTY' + 'WEEKLY-' + file + + ddf['Strike'].astype(int).astype(str) + ddf['Option_Type']
                ddf['ticker'] = ddf['Symbol']
                ddf = ddf.drop(ddf.columns[9:],axis=1)
                ddf = ddf.rename(columns = {'date':'Date','ticker':'Ticker'})
                ddf = ddf.drop_duplicates()
                ddf.to_csv(rf"C:\Users\{path}\Desktop\Pyspark\Nifty\Weekly\NIFTY-"+file+".csv",index=False)
        print("NIFTY WEEKLY CONTRACTS CREATED")
        
    def nifty_quarterly():
        if not os.path.exists(rf"C:\Users\{path}\Desktop\Pyspark_Contracts\Nifty\Quarterly_data\\"):
            os.makedirs(rf"C:\Users\{path}\Desktop\Pyspark_Contracts\Nifty\Quarterly_data\\")
            
        if not os.path.exists(rf"C:\users\{path}\desktop\Pyspark\Nifty\Quarterly\\"):
            os.makedirs(rf"C:\users\{path}\desktop\Pyspark\Nifty\Quarterly\\")
            
        folpath = rf"C:\Users\{path}\Desktop\Pyspark_Contracts\Nifty\Quarterly_Data\\"
        sym = 'NIFTY'
        start_time = datetime.strptime('09:15:00', '%H:%M:%S').time()
        end_time = datetime.strptime('15:30:00', '%H:%M:%S').time()
        expiry_time = datetime.strptime('15:29:59', '%H:%M:%S').time()
        s = 'NIFTY'

        def add(stri):
            obj = datetime.strptime(stri, "%b")
            month_number = obj.month
            return month_number

        def get_symbol(tic):
            li = list(filter(None, re.split(r'(\d+)', tic)))
            return li[0]

        exp_date = pd.read_excel(rf'C:\users\{path}\desktop\Expiry_DT.xlsx')    ## reading the expiry sheet file
        exp_file_path = rf"C:\Users\{path}\Downloads\MonthlyExpiry.csv"
        exp_df = pd.read_csv(exp_file_path,parse_dates = ["curr_exp_date","curr_date"],dayfirst =True,usecols = ["curr_exp_date","curr_date"]).dropna()
        exp_df.rename({'curr_date': 'New_date'}, axis=1, inplace=True)
        
        ndata = nifty_data()
        temp = ndata.copy()
        temp = temp.loc[:, ~temp.columns.str.contains('^Unnamed')]
        temp = temp.reset_index(drop=True)
        temp['time'] = temp['time'].str.replace(' 15:00:59','15:00:59')
        temp['time'] = temp['time'].str.replace(' 9:','09:',regex=True)
        temp['time'] = pd.to_datetime(temp['time']).dt.time
        temp['date'] = pd.to_datetime(temp['date'],dayfirst=True)
        temp['ticker'] = temp['ticker'].str.replace('30MAR23','29MAR23',regex=True)
        temp['Option_Type'] = temp['ticker'].str[-2:]
        temp['Temp'] = temp["ticker"].str.replace(s,"")
        temp['Temp'] = temp['Temp'].str[:-2]
        temp['Length_of_temp'] = temp['Temp'].str.len()
        temp['Strike'] = np.where((temp['Temp'].str.len()==9) | (temp['Temp'].str.len()==11) , 
                                  temp['Temp'].str[-4:] , 
                                  temp['Temp'].str[-5:])
        temp['Exp_Year'] = np.where((temp['Temp'].str.len()==9) | (temp['Temp'].str.len()==10) ,
                                   temp['Temp'].str[:2] ,
                                   temp['Temp'].str[5:7])
        temp['Current_Year'] = temp['date'].dt.year
        temp['Current_Year'] = temp['Current_Year'].astype(str).str[-2:]
        temp['Exp_month'] = temp['Temp'].str[2:5]
        temp['Exp_Year'] = temp['Exp_Year'].astype('str')
        temp['MonthYear'] = temp['Exp_month']+temp['Exp_Year']
        temp = pd.merge(temp,exp_date,on='MonthYear')
        temp = temp.drop(['MonthYear','Month','Year','Next_Exp_DT'],axis=1)

        temp['Length_of_temp'] = temp['Length_of_temp'].astype('int64')
        temp_10 = temp[(temp['Length_of_temp']==10) | (temp['Length_of_temp']==9)]

        temp_12 = temp[(temp['Length_of_temp']==12) | (temp['Length_of_temp']==11)]

        temp_12['DateDate'] = temp_12['Temp'].str[:2]
        temp_12['DateDate'] = temp_12['DateDate'].astype('int64')
        temp_12['Exp_DT'] = pd.to_datetime(temp['Exp_DT'],dayfirst=True)
        temp_12['Exp_Day'] = temp_12['Exp_DT'].dt.day
        temp_12 = temp_12[temp_12['Exp_Day']==temp_12['DateDate']]
        temp_12 = temp_12.drop(['DateDate','Exp_Day'],axis=1)

        temp_df = pd.concat([temp_10,temp_12],axis=0,ignore_index=True)
    #     temp_df = temp_10.append(temp_12,ignore_index=True)

        temp_df['exp_month_number'] = temp_df.apply(lambda row : add(row["Exp_month"]), axis = 1)
        temp_df['New_date'] = temp_df['date']
        temp_df["New_date"] = pd.to_datetime(temp_df["New_date"])
        temp_df["current_month_number"] = temp_df['New_date'].dt.month
        temp_df["difference"] = temp_df['exp_month_number'].astype(int) - temp_df["current_month_number"].astype(int)
        temp_df['Year_difference'] = temp_df['Exp_Year'].astype(int) - temp_df['Current_Year'].astype(int)
        temp_df = temp_df[(temp_df['exp_month_number']==3) | (temp_df['exp_month_number']==6) | (temp_df['exp_month_number']==9) | (temp_df['exp_month_number']==12)]

        temp1 = pd.merge(temp_df, 
                         exp_df, 
                         on ='New_date', 
                         how ='left')

        temp1.drop(temp1.filter(regex="Unname"),axis=1, inplace=True)
        temp1["current_exp_month_number"] = temp1['curr_exp_date'].dt.month
        temp1["Diff_months"] = temp1["current_exp_month_number"] - temp1["current_month_number"]
        temp1["Diff_months"] = temp1["Diff_months"].astype(int) 

        temp1 = temp1[temp1['Exp_DT']>=temp1['curr_exp_date']]                 ## to filter out dates which have wrong ticker

        if os.path.exists(folpath+sym+'-I.csv'):
            os.remove(folpath+sym+'-I.csv')
        if os.path.exists(folpath+sym+'-II.csv'):
            os.remove(folpath+sym+'-II.csv')
        if os.path.exists(folpath+sym+'-III.csv'):
            os.remove(folpath+sym+'-III.csv')
        if os.path.exists(folpath+sym+'-IV.csv'):
            os.remove(folpath+sym+'-IV.csv')


        atemp = temp1[(temp1['Diff_months']==0) & (temp1['Year_difference']==0)]
        agb = atemp.groupby(['difference'])
        unique_a = list(atemp['difference'].unique())

        for i in unique_a:
            temp_df = agb.get_group(i)
            temp_df = temp_df.drop(temp_df.columns[9:],axis=1)
            if i==0 or i==1 or i==2:
                temp_df.to_csv(folpath + sym + '-I.csv', mode='a', header=not os.path.exists(folpath + sym + '-I.csv'), index=False)

            if i==3 or i==4 or i==5:
                temp_df.to_csv(folpath + sym + '-II.csv', mode='a', header=not os.path.exists(folpath + sym + '-II.csv'), index=False)

            if i==6 or i==7 or i==8:
                temp_df.to_csv(folpath + sym + '-III.csv', mode='a', header=not os.path.exists(folpath + sym + '-III.csv'), index=False)

            if i==9 or i==10 or i==11:
                temp_df.to_csv(folpath + sym + '-IV.csv', mode='a', header=not os.path.exists(folpath + sym + '-IV.csv'), index=False)


        btemp = temp1[(temp1['Diff_months']==0) & (temp1['Year_difference']==1)]
        bgb = btemp.groupby(['difference'])
        unique_b = list(btemp['difference'].unique())        

        for i in unique_b:
            temp_df = bgb.get_group(i)
            temp_df = temp_df.drop(temp_df.columns[9:],axis=1)

            if i==-7 or i==-8 or i==-9:
                temp_df.to_csv(folpath + sym + '-II.csv', mode='a', header=not os.path.exists(folpath + sym + '-II.csv'), index=False)

            if i==-4 or i==-5 or i==-6:
                temp_df.to_csv(folpath + sym + '-III.csv', mode='a', header=not os.path.exists(folpath + sym + '-III.csv'), index=False)

            if i==-1 or i==-2 or i==-3:
                temp_df.to_csv(folpath + sym + '-IV.csv', mode='a', header=not os.path.exists(folpath + sym + '-IV.csv'), index=False)



        ctemp = temp1[((temp1['Diff_months']==1) | (temp1['Diff_months']==-11)) & (temp1['Year_difference']==0)]
        cgb = ctemp.groupby(['difference'])
        unique_c = list(ctemp['difference'].unique())

        for i in unique_c:
            temp_df = cgb.get_group(i)
            temp_df = temp_df.drop(temp_df.columns[9:],axis=1)

            if i==1 or i==2 or i==3:
                temp_df.to_csv(folpath + sym + '-I.csv', mode='a', header=not os.path.exists(folpath + sym + '-I.csv'), index=False)

            if i==4 or i==5 or i==6:
                temp_df.to_csv(folpath + sym + '-II.csv', mode='a', header=not os.path.exists(folpath + sym + '-II.csv'), index=False)

            if i==7 or i==8 or i==9:
                temp_df.to_csv(folpath + sym + '-III.csv', mode='a', header=not os.path.exists(folpath + sym + '-III.csv'), index=False)

            if i==10 or i==11:
                temp_df.to_csv(folpath + sym + '-IV.csv', mode='a', header=not os.path.exists(folpath + sym + '-IV.csv'), index=False)


        dtemp = temp1[((temp1['Diff_months']==1) | (temp1['Diff_months']==-11)) & (temp1['Year_difference']==1)]
        dgb = dtemp.groupby(['difference'])
        unique_d = list(dtemp['difference'].unique())

        for i in unique_d:
            temp_df = dgb.get_group(i)
            temp_df = temp_df.drop(temp_df.columns[9:],axis=1)

            if i==-9:
                temp_df.to_csv(folpath + sym + '-I.csv', mode='a', header=not os.path.exists(folpath + sym + '-I.csv'), index=False)

            if i==-6 or i==-7:
                temp_df.to_csv(folpath + sym + '-II.csv', mode='a', header=not os.path.exists(folpath + sym + '-II.csv'), index=False)

            if i==-3 or i==-4:
                temp_df.to_csv(folpath + sym + '-III.csv', mode='a', header=not os.path.exists(folpath + sym + '-III.csv'), index=False)

            if i==0 or i==-1 or i==-2:
                temp_df.to_csv(folpath + sym + '-IV.csv', mode='a', header=not os.path.exists(folpath + sym + '-IV.csv'), index=False)

        for i in range(4):
            if i==0:
                file='I'
            elif i==1:
                file='II'
            elif i==2:
                file='III'
            elif i==3:
                file='IV'
            if os.path.exists(rf'C:\users\{path}\desktop\Pyspark\Nifty\Quarterly\NIFTY-'+file+'.csv'):
                os.remove(rf'C:\users\{path}\desktop\Pyspark\Nifty\Quarterly\NIFTY-'+file+'.csv')
            if os.path.exists(rf'C:\users\{path}\desktop\Pyspark_Contracts\Nifty\Quarterly_Data\NIFTY-'+file+'.csv'):
                ddf = pd.read_csv(rf'C:\users\{path}\desktop\Pyspark_Contracts\Nifty\Quarterly_Data\NIFTY-'+file+'.csv')
                ddf['Option_Type'] = ddf['ticker'].str[-2:]
                ddf['Strike'] = np.where((ddf['ticker'].str.len()==16) | (ddf['ticker'].str.len()==18) , ddf['ticker'].str[-6:-2] , ddf['ticker'].str[-7:-2])
                ddf['Symbol'] = 'NIFTY' + 'QUARTERLY-' + file + ddf['Strike'].astype(int).astype(str) + ddf['Option_Type']
                ddf['ticker'] = ddf['Symbol']
                ddf = ddf.drop(ddf.columns[9:],axis=1)
                ddf = ddf.rename(columns = {'date':'Date','ticker':'Ticker'})
                ddf = ddf.drop_duplicates()
                ddf.to_csv(rf"C:\Users\{path}\Desktop\Pyspark\Nifty\Quarterly\NIFTY-"+file+".csv",index=False)
        print("NIFTY QUARTERLY CONTRACTS CREATED")
        
    def nifty_halfyearly():
        if not os.path.exists(rf"C:\Users\{path}\Desktop\Pyspark_Contracts\Nifty\Half_Yearly_Data\\"):
            os.makedirs(rf"C:\Users\{path}\Desktop\Pyspark_Contracts\Nifty\Half_Yearly_Data\\")
            
        if not os.path.exists(rf"C:\users\{path}\desktop\Pyspark\Nifty\Half_Yearly\\"):
            os.makedirs(rf"C:\users\{path}\desktop\Pyspark\Nifty\Half_Yearly\\")
            
        folpath = rf"C:\Users\{path}\Desktop\Pyspark_Contracts\Nifty\Half_Yearly_Data\\"
        sym = 'NIFTY'
        s = 'NIFTY'
        def add(stri):
            obj = datetime.strptime(stri, "%b")
            month_number = obj.month
            return month_number
        def get_symbol(tic):
            li = list(filter(None, re.split(r'(\d+)', tic)))
            return li[0]

        exp_date = pd.read_excel(rf'C:\users\{path}\desktop\Expiry_DT.xlsx')    ## reading the expiry sheet file

        hy_exp_date = pd.read_csv(rf"C:\Users\{path}\Downloads\half_yearly_expiry.csv",dayfirst=True,parse_dates=['Date','E1'], usecols =['Date','E1'])
        
        ndata = nifty_data()
        temp = ndata.copy()
        temp = temp.loc[:, ~temp.columns.str.contains('^Unnamed')]
        temp = temp.reset_index(drop=True)
        temp['time'] = temp['time'].str.replace(' 15:00:59','15:00:59')
        temp['time'] = temp['time'].str.replace(' 9:','09:',regex=True)
        temp['time'] = pd.to_datetime(temp['time']).dt.time
        temp['date'] = pd.to_datetime(temp['date'],dayfirst=True)
        temp['ticker'] = temp['ticker'].str.replace('30MAR23','29MAR23',regex=True)
        temp['Option_Type'] = temp['ticker'].str[-2:]
        temp['Temp'] = temp["ticker"].str.replace(s,"")
        temp['Temp'] = temp['Temp'].str[:-2]
        temp['Length_of_temp'] = temp['Temp'].str.len()
        temp['Strike'] = np.where((temp['Temp'].str.len()==9) | (temp['Temp'].str.len()==11) , 
                                  temp['Temp'].str[-4:] , 
                                  temp['Temp'].str[-5:])
        temp['Exp_Year'] = np.where((temp['Temp'].str.len()==9) | (temp['Temp'].str.len()==10) ,
                                   temp['Temp'].str[:2] ,
                                   temp['Temp'].str[5:7])
        temp['Current_Year'] = temp['date'].dt.year
        temp['Current_Year'] = temp['Current_Year'].astype(str).str[-2:]
        temp['Exp_month'] = temp['Temp'].str[2:5]
        temp['Exp_Year'] = temp['Exp_Year'].astype('str')
        temp['MonthYear'] = temp['Exp_month']+temp['Exp_Year']
        temp = pd.merge(temp,exp_date,on='MonthYear')
        temp = temp.drop(['MonthYear','Month','Year','Next_Exp_DT'],axis=1)
        temp['Length_of_temp'] = temp['Length_of_temp'].astype('int64')
        temp_10 = temp[(temp['Length_of_temp']==10) | (temp['Length_of_temp']==9)]

        temp_12 = temp[(temp['Length_of_temp']==12) | (temp['Length_of_temp']==11)]
        temp_12['DateDate'] = temp_12['Temp'].str[:2]
        temp_12['DateDate'] = temp_12['DateDate'].astype('int64')
        temp_12['Exp_DT'] = pd.to_datetime(temp['Exp_DT'],dayfirst=True)
        temp_12['Exp_Day'] = temp_12['Exp_DT'].dt.day
        temp_12 = temp_12[temp_12['Exp_Day']==temp_12['DateDate']]
        temp_12 = temp_12.drop(['DateDate','Exp_Day'],axis=1)

        temp_df = pd.concat([temp_10,temp_12],axis=0,ignore_index=True)
    #     temp_df = temp_10.append(temp_12,ignore_index=True)

        temp_df['exp_month_number'] = temp_df.apply(lambda row : add(row["Exp_month"]), axis = 1)
        temp_df['New_date'] = temp_df['date']
        temp_df["New_date"] = pd.to_datetime(temp_df["New_date"])
        temp_df["current_month_number"] = temp_df['New_date'].dt.month
        temp_df["difference"] = temp_df['exp_month_number'].astype(int) - temp_df["current_month_number"].astype(int)
        temp_df['Year_difference'] = temp_df['Exp_Year'].astype(int) - temp_df['Current_Year'].astype(int)
        temp_df = temp_df[(temp_df['exp_month_number']==6) | (temp_df['exp_month_number']==12)]

        hy_exp_date = hy_exp_date.rename({'Date':'New_date'},axis=1)
        temp1 = pd.merge(temp_df, 
                             hy_exp_date, 
                             on ='New_date', 
                             how ='left')

        temp1.drop(temp1.filter(regex="Unname"),axis=1, inplace=True)
        temp1["current_exp_month_number"] = temp1['E1'].dt.month
        temp1["Diff_months"] = temp1["current_exp_month_number"] - temp1["current_month_number"]
        temp1["Diff_months"] = temp1["Diff_months"].astype(int) 

        temp1 = temp1[temp1['Exp_DT']>=temp1['E1']]                 ## to filter out dates which have wrong ticker

        corner_case = temp1[((temp1['current_month_number']==12) & (temp1['current_exp_month_number']==6)) | ((temp1['current_month_number']==6) & (temp1['current_exp_month_number']==12))]
        normal_case = temp1[((temp1['current_month_number']>6) & (temp1['current_month_number']<=12) & (temp1['current_exp_month_number']==12)) | ((temp1['current_month_number']>0) & (temp1['current_month_number']<=6) & (temp1['current_exp_month_number']==6))]
        if(normal_case.shape[0]+corner_case.shape[0]==temp1.shape[0]):
            print("Success")

        if os.path.exists(folpath+sym+'-I.csv'):
            os.remove(folpath+sym+'-I.csv')
        if os.path.exists(folpath+sym+'-II.csv'):
            os.remove(folpath+sym+'-II.csv')
        if os.path.exists(folpath+sym+'-III.csv'):
            os.remove(folpath+sym+'-III.csv')
        if os.path.exists(folpath+sym+'-IV.csv'):
            os.remove(folpath+sym+'-IV.csv')
        if os.path.exists(folpath+sym+'-V.csv'):
            os.remove(folpath+sym+'-V.csv')
        if os.path.exists(folpath+sym+'-VI.csv'):
            os.remove(folpath+sym+'-VI.csv')
        if os.path.exists(folpath+sym+'-VII.csv'):
            os.remove(folpath+sym+'-VII.csv')
        if os.path.exists(folpath+sym+'-VIII.csv'):
            os.remove(folpath+sym+'-VIII.csv')
        if os.path.exists(folpath+sym+'-IX.csv'):
            os.remove(folpath+sym+'-IX.csv')
        if os.path.exists(folpath+sym+'-X.csv'):
            os.remove(folpath+sym+'-X.csv')

        ## NORMAL CASE HY1
        atemp = normal_case[(normal_case['Diff_months']<=5) & (normal_case['Diff_months']>=0) & (normal_case['Year_difference']==0) & ((normal_case['difference']>=0) & (normal_case['difference']<6))]
        agb = atemp.groupby(['Diff_months'])
        unique_a = list(atemp['Diff_months'].unique())
        for i in unique_a:
            temp_df = agb.get_group(i)
            temp_df = temp_df.drop(temp_df.columns[9:],axis=1)
            if i<=5 and i>=0:
                temp_df.to_csv(folpath + sym + '-I.csv', mode='a', header=not os.path.exists(folpath + sym + '-I.csv'), index=False)

        ## NORMAL CASE HY2
        btemp = normal_case[((normal_case['Diff_months']<=5) & (normal_case['Year_difference']==0) & (normal_case['difference']>=6)) | ((normal_case['Diff_months']<=5) & (normal_case['Year_difference']==1) & (normal_case['difference']<=-6))]
        bgb = btemp.groupby(['Diff_months'])
        unique_b = list(btemp['Diff_months'].unique())
        for i in unique_b:
            temp_df = bgb.get_group(i)
            temp_df = temp_df.drop(temp_df.columns[9:],axis=1)
            if i<=6 and i>=0:
                temp_df.to_csv(folpath + sym + '-II.csv', mode='a', header=not os.path.exists(folpath + sym + '-II.csv'), index=False)

        ## NORMAL CASE HY3
        ctemp = normal_case[(normal_case['Diff_months']<=5) & (normal_case['Diff_months']>=0) & (normal_case['Year_difference']==1) & ((normal_case['difference']>=0) & (normal_case['difference']<6))]
        cgb = ctemp.groupby(['Diff_months'])
        unique_c = list(ctemp['Diff_months'].unique())
        for i in unique_c:
            temp_df = cgb.get_group(i)
            temp_df = temp_df.drop(temp_df.columns[9:],axis=1)
            if i<=5 and i>=0:
                temp_df.to_csv(folpath + sym + '-III.csv', mode='a', header=not os.path.exists(folpath + sym + '-III.csv'), index=False)

        ## NORMAL CASE HY4
        dtemp = normal_case[((normal_case['Diff_months']<=5) & (normal_case['Year_difference']==1) & (normal_case['difference']>=6)) | ((normal_case['Diff_months']<=5) & (normal_case['Year_difference']==2) & (normal_case['difference']<=-6))]
        dgb = dtemp.groupby(['Diff_months'])
        unique_d = list(dtemp['Diff_months'].unique())
        for i in unique_d:
            temp_df = dgb.get_group(i)
            temp_df = temp_df.drop(temp_df.columns[9:],axis=1)
            if i<=6 and i>=0:
                temp_df.to_csv(folpath + sym + '-IV.csv', mode='a', header=not os.path.exists(folpath + sym + '-IV.csv'), index=False)

        ## NORMAL CASE HY5
        etemp = normal_case[(normal_case['Diff_months']<=5) & (normal_case['Diff_months']>=0) & (normal_case['Year_difference']==2) & ((normal_case['difference']>=0) & (normal_case['difference']<6))]
        egb = etemp.groupby(['Diff_months'])
        unique_e = list(etemp['Diff_months'].unique())
        for i in unique_e:
            temp_df = egb.get_group(i)
            temp_df = temp_df.drop(temp_df.columns[9:],axis=1)
            if i<=5 and i>=0:
                temp_df.to_csv(folpath + sym + '-V.csv', mode='a', header=not os.path.exists(folpath + sym + '-V.csv'), index=False)

        ## NORMAL CASE HY6
        ftemp = normal_case[((normal_case['Diff_months']<=5) & (normal_case['Year_difference']==2) & (normal_case['difference']>=6)) | ((normal_case['Diff_months']<=5) & (normal_case['Year_difference']==3) & (normal_case['difference']<=-6))]
        fgb = ftemp.groupby(['Diff_months'])
        unique_f = list(ftemp['Diff_months'].unique())
        for i in unique_f:
            temp_df = fgb.get_group(i)
            temp_df = temp_df.drop(temp_df.columns[9:],axis=1)
            if i<=6 and i>=0:
                temp_df.to_csv(folpath + sym + '-VI.csv', mode='a', header=not os.path.exists(folpath + sym + '-VI.csv'), index=False)

        ## NORMAL CASE HY7
        gtemp = normal_case[(normal_case['Diff_months']<=5) & (normal_case['Diff_months']>=0) & (normal_case['Year_difference']==3) & ((normal_case['difference']>=0) & (normal_case['difference']<6))]
        ggb = gtemp.groupby(['Diff_months'])
        unique_g = list(gtemp['Diff_months'].unique())
        for i in unique_g:
            temp_df = ggb.get_group(i)
            temp_df = temp_df.drop(temp_df.columns[9:],axis=1)
            if i<=5 and i>=0:
                temp_df.to_csv(folpath + sym + '-VII.csv', mode='a', header=not os.path.exists(folpath + sym + '-VII.csv'), index=False)

        ## NORMAL CASE HY8
        htemp = normal_case[((normal_case['Diff_months']<=5) & (normal_case['Year_difference']==3) & (normal_case['difference']>=6)) | ((normal_case['Diff_months']<=5) & (normal_case['Year_difference']==4) & (normal_case['difference']<=-6))]
        hgb = htemp.groupby(['Diff_months'])
        unique_h = list(htemp['Diff_months'].unique())
        for i in unique_h:
            temp_df = hgb.get_group(i)
            temp_df = temp_df.drop(temp_df.columns[9:],axis=1)
            if i<=6 and i>=0:
                temp_df.to_csv(folpath + sym + '-VIII.csv', mode='a', header=not os.path.exists(folpath + sym + '-VIII.csv'), index=False)

        ## NORMAL CASE HY9
        itemp = normal_case[(normal_case['Diff_months']<=5) & (normal_case['Diff_months']>=0) & (normal_case['Year_difference']==4) & ((normal_case['difference']>=0) & (normal_case['difference']<6))]
        igb = itemp.groupby(['Diff_months'])
        unique_i = list(itemp['Diff_months'].unique())
        for i in unique_i:
            temp_df = igb.get_group(i)
            temp_df = temp_df.drop(temp_df.columns[9:],axis=1)
            if i<=5 and i>=0:
                temp_df.to_csv(folpath + sym + '-IX.csv', mode='a', header=not os.path.exists(folpath + sym + '-IX.csv'), index=False)

        ## NORMAL CASE HY10
        jtemp = normal_case[((normal_case['Diff_months']<=5) & (normal_case['Year_difference']==4) & (normal_case['difference']>=6)) | ((normal_case['Diff_months']<=5) & (normal_case['Year_difference']==5) & (normal_case['difference']<=-6))]
        jgb = jtemp.groupby(['Diff_months'])
        unique_j = list(jtemp['Diff_months'].unique())
        for i in unique_j:
            temp_df = jgb.get_group(i)
            temp_df = temp_df.drop(temp_df.columns[9:],axis=1)
            if i<=6 and i>=0:
                temp_df.to_csv(folpath + sym + '-X.csv', mode='a', header=not os.path.exists(folpath + sym + '-X.csv'), index=False)

        ## CORNER CASE HY1
        ktemp = corner_case[((corner_case['Diff_months']==6) & (corner_case['Year_difference']==0) & (corner_case['difference']==6)) | ((corner_case['Diff_months']==-6) & (corner_case['Year_difference']==1) & (corner_case['difference']==-6))]
        kgb = ktemp.groupby(['Diff_months'])
        unique_k = list(ktemp['Diff_months'].unique())
        for i in unique_k:
            temp_df = kgb.get_group(i)
            temp_df = temp_df.drop(temp_df.columns[9:],axis=1)
            if i==-6 or i==6:
                temp_df.to_csv(folpath + sym + '-I.csv', mode='a', header=not os.path.exists(folpath + sym + '-I.csv'), index=False)

        ## CORNER CASE HY2
        ltemp = corner_case[((corner_case['Diff_months']==6) & (corner_case['difference']==0) & (corner_case['Year_difference']==1)) | ((corner_case['Diff_months']==-6) & (corner_case['difference']==0) & (corner_case['Year_difference']==1))]
        lgb = ltemp.groupby(['Diff_months'])
        unique_l = list(ltemp['Diff_months'].unique())
        for i in unique_l:
            temp_df = lgb.get_group(i)
            temp_df = temp_df.drop(temp_df.columns[9:],axis=1)
            if i==-6 or i==6:
                temp_df.to_csv(folpath + sym + '-II.csv', mode='a', header=not os.path.exists(folpath + sym + '-II.csv'), index=False)

        ## CORNER CASE HY3
        mtemp = corner_case[((corner_case['Diff_months']==6) & (corner_case['Year_difference']==1) & (corner_case['difference']==6)) | ((corner_case['Diff_months']==-6) & (corner_case['Year_difference']==2) & (corner_case['difference']==-6))]
        mgb = mtemp.groupby(['Diff_months'])
        unique_m = list(mtemp['Diff_months'].unique())
        for i in unique_m:
            temp_df = mgb.get_group(i)
            temp_df = temp_df.drop(temp_df.columns[9:],axis=1)
            if i==-6 or i==6:
                temp_df.to_csv(folpath + sym + '-III.csv', mode='a', header=not os.path.exists(folpath + sym + '-III.csv'), index=False)

        ## CORNER CASE HY4
        ntemp = corner_case[((corner_case['Diff_months']==6) & (corner_case['difference']==0) & (corner_case['Year_difference']==2)) | ((corner_case['Diff_months']==-6) & (corner_case['difference']==0) & (corner_case['Year_difference']==2))]
        ngb = ntemp.groupby(['Diff_months'])
        unique_n = list(ntemp['Diff_months'].unique())
        for i in unique_n:
            temp_df = ngb.get_group(i)
            temp_df = temp_df.drop(temp_df.columns[9:],axis=1)
            if i==-6 or i==6:
                temp_df.to_csv(folpath + sym + '-IV.csv', mode='a', header=not os.path.exists(folpath + sym + '-IV.csv'), index=False)

        ## CORNER CASE HY5
        otemp = corner_case[((corner_case['Diff_months']==6) & (corner_case['Year_difference']==2) & (corner_case['difference']==6)) | ((corner_case['Diff_months']==-6) & (corner_case['Year_difference']==3) & (corner_case['difference']==-6))]
        ogb = otemp.groupby(['Diff_months'])
        unique_o = list(otemp['Diff_months'].unique())
        for i in unique_o:
            temp_df = ogb.get_group(i)
            temp_df = temp_df.drop(temp_df.columns[9:],axis=1)
            if i==-6 or i==6:
                temp_df.to_csv(folpath + sym + '-V.csv', mode='a', header=not os.path.exists(folpath + sym + '-V.csv'), index=False)

        ## CORNER CASE HY6
        ptemp = corner_case[((corner_case['Diff_months']==6) & (corner_case['difference']==0) & (corner_case['Year_difference']==3)) | ((corner_case['Diff_months']==-6) & (corner_case['difference']==0) & (corner_case['Year_difference']==3))]
        pgb = ptemp.groupby(['Diff_months'])
        unique_p = list(ptemp['Diff_months'].unique())
        for i in unique_p:
            temp_df = pgb.get_group(i)
            temp_df = temp_df.drop(temp_df.columns[9:],axis=1)
            if i==-6 or i==6:
                temp_df.to_csv(folpath + sym + '-VI.csv', mode='a', header=not os.path.exists(folpath + sym + '-VI.csv'), index=False)

        ## CORNER CASE HY7
        qtemp = corner_case[((corner_case['Diff_months']==6) & (corner_case['Year_difference']==3) & (corner_case['difference']==6)) | ((corner_case['Diff_months']==-6) & (corner_case['Year_difference']==4) & (corner_case['difference']==-6))]
        qgb = qtemp.groupby(['Diff_months'])
        unique_q = list(qtemp['Diff_months'].unique())
        for i in unique_q:
            temp_df = qgb.get_group(i)
            temp_df = temp_df.drop(temp_df.columns[9:],axis=1)
            if i==-6 or i==6:
                temp_df.to_csv(folpath + sym + '-VII.csv', mode='a', header=not os.path.exists(folpath + sym + '-VII.csv'), index=False)

        ## CORNER CASE HY8
        rtemp = corner_case[((corner_case['Diff_months']==6) & (corner_case['difference']==0) & (corner_case['Year_difference']==4)) | ((corner_case['Diff_months']==-6) & (corner_case['difference']==0) & (corner_case['Year_difference']==4))]
        rgb = rtemp.groupby(['Diff_months'])
        unique_r = list(rtemp['Diff_months'].unique())
        for i in unique_r:
            temp_df = rgb.get_group(i)
            temp_df = temp_df.drop(temp_df.columns[9:],axis=1)
            if i==-6 or i==6:
                temp_df.to_csv(folpath + sym + '-VIII.csv', mode='a', header=not os.path.exists(folpath + sym + '-VIII.csv'), index=False)

        ## CORNER CASE HY9
        stemp = corner_case[((corner_case['Diff_months']==6) & (corner_case['Year_difference']==4) & (corner_case['difference']==6)) | ((corner_case['Diff_months']==-6) & (corner_case['Year_difference']==5) & (corner_case['difference']==-6))]
        sgb = stemp.groupby(['Diff_months'])
        unique_s = list(stemp['Diff_months'].unique())
        for i in unique_s:
            temp_df = sgb.get_group(i)
            temp_df = temp_df.drop(temp_df.columns[9:],axis=1)
            if i==-6 or i==6:
                temp_df.to_csv(folpath + sym + '-IX.csv', mode='a', header=not os.path.exists(folpath + sym + '-IX.csv'), index=False)

        ## CORNER CASE HY10
        ttemp = corner_case[((corner_case['Diff_months']==6) & (corner_case['difference']==0) & (corner_case['Year_difference']==5)) | ((corner_case['Diff_months']==-6) & (corner_case['difference']==0) & (corner_case['Year_difference']==5))]
        tgb = ttemp.groupby(['Diff_months'])
        unique_t = list(ttemp['Diff_months'].unique())
        for i in unique_t:
            temp_df = tgb.get_group(i)
            temp_df = temp_df.drop(temp_df.columns[9:],axis=1)
            if i==-6 or i==6:
                temp_df.to_csv(folpath + sym + '-X.csv', mode='a', header=not os.path.exists(folpath + sym + '-X.csv'), index=False)

        for i in range(10):
            if(i==0):
                file='I'
            elif(i==1):
                file='II'
            elif(i==2):
                file='III'
            elif(i==3):
                file='IV'
            elif(i==4):
                file='V'
            elif(i==5):
                file='VI'
            elif(i==6):
                file='VII'
            elif(i==7):
                file='VIII'
            elif(i==8):
                file='IX'
            else:
                file='X'
            if os.path.exists(rf"C:\Users\{path}\Desktop\Pyspark\Nifty\Half_Yearly\\Nifty-"+file+'.csv'):
                os.remove(rf"C:\Users\{path}\Desktop\Pyspark\Nifty\Half_Yearly\\Nifty-"+file+'.csv')
            if os.path.exists(rf"C:\Users\{path}\Desktop\Pyspark_Contracts\Nifty\Half_Yearly_Data\NIFTY-"+file+".csv"):
                df1 = pd.read_csv(rf"C:\Users\{path}\Desktop\Pyspark_Contracts\Nifty\Half_Yearly_Data\NIFTY-"+file+".csv")
                df1['Option_Type'] = df1['ticker'].str[-2:]
                df1['Strike'] = np.where((df1['ticker'].str.len()==16) | (df1['ticker'].str.len()==18) , df1['ticker'].str[-6:-2] , df1['ticker'].str[-7:-2])
                df1['Symbol'] = 'NIFTY-' + file + df1['Strike'].astype(int).astype(str) + df1['Option_Type']
                df1['ticker'] = df1['Symbol']
                df1 = df1.drop(df1.columns[9:],axis=1)
                df1.to_csv(rf"C:\Users\{path}\Desktop\Pyspark\Nifty\Half_Yearly\\Nifty-"+file+'.csv',index=False)
        print("NIFTY HALF YEARLY CONTRACTS CREATED")
        
    def nifty_yearly():
        if not os.path.exists(rf"C:\Users\{path}\Desktop\Pyspark_Contracts\Nifty\Yearly_Data\\"):
            os.makedirs(rf"C:\Users\{path}\Desktop\Pyspark_Contracts\Nifty\Yearly_Data\\")
            
        if not os.path.exists(rf"C:\users\{path}\desktop\Pyspark\Nifty\Yearly\\"):
            os.makedirs(rf"C:\users\{path}\desktop\Pyspark\Nifty\Yearly\\")
            
        for i in range(5):
            if(i==0):
                file='I'
                file1='I'
                file2='II'
                ## REMOVING YEARLY CONTRACT FILE 
                if os.path.exists(rf"C:\Users\{path}\Desktop\Pyspark_Contracts\Nifty\Yearly_Data\\Nifty-"+file+'.csv'):
                    os.remove(rf"C:\Users\{path}\Desktop\Pyspark_Contracts\Nifty\Yearly_Data\\Nifty-"+file+'.csv')
                ## CHECKING IF HALFYEARLY FILE EXISTS
                if os.path.exists(rf'C:\Users\{path}\Desktop\Pyspark_Contracts\Nifty\Half_Yearly_Data\NIFTY-'+file1+'.csv'):
                    df1 = pd.read_csv(rf'C:\Users\{path}\Desktop\Pyspark_Contracts\Nifty\Half_Yearly_Data\NIFTY-'+file1+'.csv')
                else:
                    df1 = pd.DataFrame()
                ## CHECKING IF HALFYEARLY FILE EXISTS
                if os.path.exists(rf'C:\Users\{path}\Desktop\Pyspark_Contracts\Nifty\Half_Yearly_Data\NIFTY-'+file2+'.csv'):
                    df2 = pd.read_csv(rf'C:\Users\{path}\Desktop\Pyspark_Contracts\Nifty\Half_Yearly_Data\NIFTY-'+file2+'.csv')
                else:
                    df2 = pd.DataFrame()
            elif(i==1):
                file='II'
                file1='III'
                file2='IV'
                ## REMOVING YEARLY CONTRACT FILE 
                if os.path.exists(rf"C:\Users\{path}\Desktop\Pyspark_Contracts\Nifty\Yearly_Data\\Nifty-"+file+'.csv'):
                    os.remove(rf"C:\Users\{path}\Desktop\Pyspark_Contracts\Nifty\Yearly_Data\\Nifty-"+file+'.csv')
                ## CHECKING IF HALFYEARLY FILE EXISTS
                if os.path.exists(rf'C:\Users\{path}\Desktop\Pyspark_Contracts\Nifty\Half_Yearly_Data\NIFTY-'+file1+'.csv'):
                    df1 = pd.read_csv(rf'C:\Users\{path}\Desktop\Pyspark_Contracts\Nifty\Half_Yearly_Data\NIFTY-'+file1+'.csv')
                else:
                    df1 = pd.DataFrame()
                ## CHECKING IF HALFYEARLY FILE EXISTS
                if os.path.exists(rf'C:\Users\{path}\Desktop\Pyspark_Contracts\Nifty\Half_Yearly_Data\NIFTY-'+file2+'.csv'):
                    df2 = pd.read_csv(rf'C:\Users\{path}\Desktop\Pyspark_Contracts\Nifty\Half_Yearly_Data\NIFTY-'+file2+'.csv')
                else:
                    df2 = pd.DataFrame()
            elif(i==2):
                file='III'
                file1='V'
                file2='VI'
                ## REMOVING YEARLY CONTRACT FILE 
                if os.path.exists(rf"C:\Users\{path}\Desktop\Pyspark_Contracts\Nifty\Yearly_Data\\Nifty-"+file+'.csv'):
                    os.remove(rf"C:\Users\{path}\Desktop\Pyspark_Contracts\Nifty\Yearly_Data\\Nifty-"+file+'.csv')
                if os.path.exists(rf'C:\Users\{path}\Desktop\Pyspark_Contracts\Nifty\Half_Yearly_Data\NIFTY-'+file1+'.csv'):
                    df1 = pd.read_csv(rf'C:\Users\{path}\Desktop\Pyspark_Contracts\Nifty\Half_Yearly_Data\NIFTY-'+file1+'.csv')
                else:
                    df1 = pd.DataFrame()
                if os.path.exists(rf'C:\Users\{path}\Desktop\Pyspark_Contracts\Nifty\Half_Yearly_Data\NIFTY-'+file2+'.csv'):
                    df2 = pd.read_csv(rf'C:\Users\{path}\Desktop\Pyspark_Contracts\Nifty\Half_Yearly_Data\NIFTY-'+file2+'.csv')
                else:
                    df2 = pd.DataFrame()
            elif(i==3):
                file='IV'
                file1='VII'
                file2='VIII'
                ## REMOVING YEARLY CONTRACT FILE 
                if os.path.exists(rf"C:\Users\{path}\Desktop\Pyspark_Contracts\Nifty\Yearly_Data\\Nifty-"+file+'.csv'):
                    os.remove(rf"C:\Users\{path}\Desktop\Pyspark_Contracts\Nifty\Yearly_Data\\Nifty-"+file+'.csv')
                ## CHECKING IF HALFYEARLY FILE EXISTS
                if os.path.exists(rf'C:\Users\{path}\Desktop\Pyspark_Contracts\Nifty\Half_Yearly_Data\NIFTY-'+file1+'.csv'):
                    df1 = pd.read_csv(rf'C:\Users\{path}\Desktop\Pyspark_Contracts\Nifty\Half_Yearly_Data\NIFTY-'+file1+'.csv')
                else:
                    df1 = pd.DataFrame()
                ## CHECKING IF HALFYEARLY FILE EXISTS
                if os.path.exists(rf'C:\Users\{path}\Desktop\Pyspark_Contracts\Nifty\Half_Yearly_Data\NIFTY-'+file2+'.csv'):
                    df2 = pd.read_csv(rf'C:\Users\{path}\Desktop\Pyspark_Contracts\Nifty\Half_Yearly_Data\NIFTY-'+file2+'.csv')
                else:
                    df2 = pd.DataFrame()
            elif(i==4):
                file='V'
                file1='IX'
                file2='X'
                ## REMOVING YEARLY CONTRACT FILE 
                if os.path.exists(rf"C:\Users\{path}\Desktop\Pyspark_Contracts\Nifty\Yearly_Data\\Nifty-"+file+'.csv'):
                    os.remove(rf"C:\Users\{path}\Desktop\Pyspark_Contracts\Nifty\Yearly_Data\\Nifty-"+file+'.csv')
                ## CHECKING IF HALFYEARLY FILE EXISTS
                if os.path.exists(rf'C:\Users\{path}\Desktop\Pyspark_Contracts\Nifty\Half_Yearly_Data\NIFTY-'+file1+'.csv'):
                    df1 = pd.read_csv(rf'C:\Users\{path}\Desktop\Pyspark_Contracts\Nifty\Half_Yearly_Data\NIFTY-'+file1+'.csv')
                else:
                    df1 = pd.DataFrame()
                ## CHECKING IF HALFYEARLY FILE EXISTS
                if os.path.exists(rf'C:\Users\{path}\Desktop\Pyspark_Contracts\Nifty\Half_Yearly_Data\NIFTY-'+file2+'.csv'):
                    df2 = pd.read_csv(rf'C:\Users\{path}\Desktop\Pyspark_Contracts\Nifty\Half_Yearly_Data\NIFTY-'+file2+'.csv')
                else:
                    df2 = pd.DataFrame()
            
            if df1.empty and df2.empty:
                print("No contracts for Half yearly",file1,file2)
            else:
                print(i,"YEARLY",file,"HALFYEARLY",file1,file2)
                if not df1.empty:
                    df1 = df1.sort_values(by='date')
                    df1['Month'] = df1['ticker'].str[7:10]
                    df1 = df1[df1['Month']=='DEC']
                if not df2.empty:
                    df2 = df2.sort_values(by='date')
                    df2['Month'] = df2['ticker'].str[7:10]
                    df2 = df2[df2['Month']=='DEC']
                final_df = pd.concat([df1,df2],axis=0,ignore_index=True)
    #             final_df = df1.append(df2,ignore_index=True)
                if not final_df.empty:
                    final_df = final_df.drop_duplicates()
                    final_df = final_df.drop(final_df.columns[9:],axis=1)
                    final_df.to_csv(rf"C:\Users\{path}\Desktop\Pyspark_Contracts\Nifty\Yearly_Data\\Nifty-"+file+'.csv',index=False)

        for i in range(5):
            if(i==0):
                file='I'
            if(i==1):
                file='II'
            if(i==2):
                file='III'
            if(i==3):
                file='IV'
            if(i==4):
                file='V'
            if os.path.exists(rf"C:\Users\{path}\Desktop\Pyspark\Nifty\Yearly\\Nifty-"+file+".csv"):
                os.remove(rf"C:\Users\{path}\Desktop\Pyspark\Nifty\Yearly\\Nifty-"+file+".csv")
            if os.path.exists(rf"C:\Users\{path}\Desktop\Pyspark_Contracts\Nifty\Yearly_Data\\Nifty-"+file+'.csv'):
                df = pd.read_csv(rf"C:\Users\{path}\Desktop\Pyspark_Contracts\Nifty\Yearly_Data\\Nifty-"+file+'.csv')
                df['Option_Type'] = df['ticker'].str[-2:]
                df['Strike'] = np.where((df['ticker'].str.len()==16) | (df['ticker'].str.len()==18) , df['ticker'].str[-6:-2] , df['ticker'].str[-7:-2])
                df['Symbol'] = 'NIFTY-' + file + df['Strike'].astype(int).astype(str) + df['Option_Type']
                df['ticker'] = df['Symbol']
                df = df.drop(df.columns[9:],axis=1)
                df.to_csv(rf"C:\Users\{path}\Desktop\Pyspark\Nifty\Yearly\\Nifty-"+file+".csv",index=False)
        print("YEARLY CONTRACTS GENERATED")

    with open (r"C:\Data\CsvFiles\ErrorLog.txt", 'w') as file:
        file.write('Nifty contracts created!')

    def finnifty_data():
        spark = SparkSession.builder.config("spark.jars", "C:\\Users\\Administrator\\Downloads\\postgresql-42.5.1.jar") \
        .master("local").appName("PySpark_Postgres_test").getOrCreate()
        #spark.sparkContext.setLogLevel("WARN")
        df = spark.read.format("jdbc").option("url", "jdbc:postgresql://swandatabase.cfehmk2wtejq.ap-south-1.rds.amazonaws.com/RawDataBase").option("user","postgres").option("password","swancap123")\
            .option("driver", "org.postgresql.Driver").option("dbtable", tablename)\
            .option("user", "postgres").option("password", "swancap123").load()
        ## GETTING ONLY TIME IN TIME COLUMN
        q = df.withColumn('time',date_format('time', 'HH:mm:ss'))
        ## FILTERING FINNIFTY DATA
        fndata = q.filter(q.ticker.contains('FINNIFTY') & ((q.ticker.endswith('E.NFO'))| (q.ticker.endswith('E'))))
        ## REPLACING .NFO IN ticker
        fndata = fndata.withColumn('ticker',regexp_replace('ticker','.NFO',''))
        fndata = fndata.drop(col('ticker_check'))
        ## CONVERTING PYSPARK DATAFRAME TO PANDAS DATAFRAME
        fndata=fndata.toPandas()
        return fndata

    def finnifty_monthly():
        
        ## CHECKING IF PATH DOES NOT EXIST, THEN CREATE PATH
        if not os.path.exists(rf"C:\Users\{path}\Desktop\Pyspark_Contracts\FinNifty\Monthly_data\\"):
            os.makedirs(rf"C:\Users\{path}\Desktop\Pyspark_Contracts\FinNifty\Monthly_data\\")
            
        if not os.path.exists(rf"C:\users\{path}\desktop\Pyspark\FinNifty\Monthly\\"):
            os.makedirs(rf"C:\users\{path}\desktop\Pyspark\FinNifty\Monthly\\")
            
        exp_df = pd.read_excel(rf"C:\Users\{path}\Downloads\Finnifty.xlsx",sheet_name='Monthly')
        exp_df.rename({'curr_date': 'New_date'}, axis=1, inplace=True)
        exp_date = pd.read_excel(rf"C:\Users\{path}\Downloads\Finnifty_Monthly_Expiry.xlsx")

        folpath = rf"C:\Users\{path}\Desktop\Pyspark_Contracts\FinNifty\Monthly_Data\\"
        finalpath = rf"C:\Users\{path}\Desktop\Pyspark\FinNifty\Monthly\\"
        s = 'FINNIFTY'
        def add(stri):
            obj = datetime.strptime(stri, "%b")
            month_number = obj.month
            return month_number
        sym = 'FINNIFTY'
        fndata = finnifty_data()
        dff = fndata.copy()
        dff = dff[dff['ticker'].str.contains('FINNIFTY')]
        dff = dff[(dff['ticker'].str.endswith('E')) | (dff['ticker'].str.endswith('E.NFO'))]
        dff = dff.loc[:, ~dff.columns.str.contains('^Unnamed')]
        dff['time'] = pd.to_datetime(dff['time']).dt.time
        dff['date'] = pd.to_datetime(dff['date'],dayfirst=True)
        dff['ticker'] = dff['ticker'].str.replace('30MAR23','29MAR23',regex=True)

        dff['Option_Type'] = dff['ticker'].str[-2:]
        dff['Temp'] = dff["ticker"].str.replace(s,"")
        dff['Temp'] = dff['Temp'].str[:-2]
        dff['Length_of_Temp'] = dff['Temp'].str.len()
        dff['Strike'] = np.where((dff['Temp'].str.len()==9) | (dff['Temp'].str.len()==11) , 
                                  dff['Temp'].str[-4:] , 
                                  dff['Temp'].str[-5:])
        dff['Exp_Year'] = np.where((dff['Temp'].str.len()==9) | (dff['Temp'].str.len()==10) ,
                                   dff['Temp'].str[:2] ,
                                   dff['Temp'].str[5:7])
        dff['Exp_month'] = dff['Temp'].str[2:5]

        dff['Exp_Year'] = dff['Exp_Year'].astype('str')
        dff['MonthYear'] = dff['Exp_month']+dff['Exp_Year']
        dff = pd.merge(dff,exp_date,on='MonthYear')
        dff = dff.drop(['MonthYear','Month','Year'],axis=1)
        dff = dff.rename(columns={'Exp_DT':'Monthly_Expiry'})

        dff['Length_of_Temp'] = dff['Length_of_Temp'].astype('int64')
        temp_10 = dff[(dff['Length_of_Temp']==10) | (dff['Length_of_Temp']==9)]

        temp_12 = dff[(dff['Length_of_Temp']==12) | (dff['Length_of_Temp']==11)]
        temp_12['DateDate'] = temp_12['Temp'].str[:2]
        temp_12['DateDate'] = temp_12['DateDate'].astype('int64')
        temp_12['Exp_DT'] = pd.to_datetime(temp_12['Monthly_Expiry'],dayfirst=True)
        temp_12['Exp_Day'] = temp_12['Exp_DT'].dt.day
        temp_12 = temp_12[temp_12['Exp_Day']==temp_12['DateDate']]
        temp_12 = temp_12.drop(['DateDate','Exp_Day'],axis=1)

        temp_df = pd.concat([temp_10,temp_12],axis=0,ignore_index=True)
    #     temp_df = temp_10.append(temp_12,ignore_index=True)
        temp_df['exp_month_number'] = temp_df.apply(lambda row : add(row["Exp_month"]), axis = 1)
        temp_df['New_date'] = temp_df['date']
        temp_df["New_date"] = pd.to_datetime(temp_df["New_date"])
        
        temp_df["current_month_number"] = temp_df['New_date'].dt.month
        temp_df["difference"] = temp_df['exp_month_number'].astype(int) - temp_df["current_month_number"].astype(int)

        temp_df1 = pd.merge(temp_df, 
                         exp_df, 
                         on ='New_date', 
                         how ='left')
        temp_df1.drop(temp_df1.filter(regex="Unname"),axis=1, inplace=True)
        temp_df1["current_exp_month_number"] = temp_df1['curr_exp_date'].dt.month
        temp_df1["Diff_months"] = temp_df1["current_exp_month_number"] - temp_df1["current_month_number"]
        temp_df1["Diff_months"] = temp_df1["Diff_months"].astype(int) 
        temp_df1['Current_Year'] = temp_df1['New_date'].dt.year.astype(str).str[-2:]

        bdf = temp_df1[temp_df1["Diff_months"] == 0]
        adf = temp_df1[(temp_df1["Diff_months"] == 1) | (temp_df1["Diff_months"] == -11)]
        agb = adf.groupby(["difference"])
        unique_val_list_a = list(adf["difference"].unique())

        bgb = bdf.groupby(["difference"])
        unique_val_list_b = list(bdf["difference"].unique())

        ## TO AVOID OVERWRITING, REMOVING FILE
        for i in range(3):
            if(i==0):
                file='-I'
            if(i==1):
                file='-II'
            if(i==2):
                file='-III'
            if(os.path.exists(folpath+sym+file+".csv")):
                os.remove(folpath+sym+file+".csv")

        for i in unique_val_list_b:
            temp_df_new = bgb.get_group(i)
            temp_df_new = temp_df_new.drop(temp_df_new.columns[9:],axis=1)
            if i == 0:
                temp_df_new.to_csv(folpath + sym + '-I.csv', mode = 'a', header = not os.path.exists(folpath + sym + '-I.csv'), index=False)

            if i == 1 or i == -11:
                temp_df_new.to_csv(folpath + sym + '-II.csv', mode = 'a', header = not os.path.exists(folpath + sym + '-II.csv'), index=False)

            if i == 2 or i == -10:
                temp_df_new.to_csv(folpath + sym + '-III.csv', mode = 'a', header = not os.path.exists(folpath + sym + '-III.csv'), index=False)

        for i in unique_val_list_a:
            temp_df_new = agb.get_group(i)
            temp_df_new = temp_df_new.drop(temp_df_new.columns[9:],axis=1)
            if i == 1 or i == -11:
                temp_df_new.to_csv(folpath + sym + '-I.csv', mode = 'a', header = not os.path.exists(folpath + sym + '-I.csv'), index=False)

            if i == 2 or i == -10:
                temp_df_new.to_csv(folpath + sym + '-II.csv', mode = 'a', header = not os.path.exists(folpath + sym + '-II.csv'), index=False)

            if i == 3 or i == -9:
                temp_df_new.to_csv(folpath + sym + '-III.csv', mode = 'a', header = not os.path.exists(folpath + sym + '-III.csv'), index=False)

        for i in range(3):
            if(i==0):
                file='-I'
            if(i==1):
                file='-II'
            if(i==2):
                file='-III'

            if os.path.exists(finalpath+sym+file+".csv"):
                os.remove(finalpath+sym+file+".csv")
            if os.path.exists(folpath+sym+file+".csv"):
                df = pd.read_csv(folpath+sym+file+".csv")
                df['Option_Type'] = df['ticker'].str[-2:]
                df['Strike'] = np.where((df['ticker'].str.len()==19) | (df['ticker'].str.len()==21) , df['ticker'].str[-6:-2] , df['ticker'].str[-7:-2])
                df['Symbol'] = 'FINNIFTYMONTHLY' + file + df['Strike'].astype(int).astype(str) + df['Option_Type']
                df['ticker'] = df['Symbol']
                df = df.drop(df.columns[9:],axis=1)
                df = df.sort_values(by='date')
                print(file,df.shape[0])
                df = df.drop_duplicates()
                print(file,df.shape[0])
                df.to_csv(finalpath+sym+file+".csv",index=False)
        print("FINNIFTY MONTHLY CONTRACTS GENERATED")
        
    def finnifty_weekly():
        
        ## CHECKING IF PATH DOES NOT EXIST, THEN CREATE PATH
        if not os.path.exists(rf"C:\Users\{path}\Desktop\Pyspark_Contracts\FinNifty\Weekly_data\\"):
            os.makedirs(rf"C:\Users\{path}\Desktop\Pyspark_Contracts\FinNifty\Weekly_data\\")
            
        if not os.path.exists(rf"C:\users\{path}\desktop\Pyspark\FinNifty\Weekly\\"):
            os.makedirs(rf"C:\users\{path}\desktop\Pyspark\FinNifty\Weekly\\")
            
        exp_df1 = pd.read_excel(rf"C:\Users\{path}\Downloads\Finnifty.xlsx",sheet_name='Weekly',parse_dates=['date'],usecols= ['date','Week_number'])
        exp_df2 = pd.read_excel(rf"C:\Users\{path}\Downloads\Finnifty.xlsx",sheet_name='Weekly',parse_dates=['Weekly_Expiry_Date'],usecols= ['Weekly_Expiry_Date', 'Expiry_Week_number'])
        exp_date = pd.read_excel(rf"C:\Users\{path}\Downloads\Finnifty_Monthly_Expiry.xlsx")

        folpath = rf"C:\Users\{path}\Desktop\Pyspark_Contracts\FinNifty\Weekly_Data\\"
        finalpath = rf"C:\Users\{path}\Desktop\Pyspark\FinNifty\Weekly\\"
        s = 'FINNIFTY'
        def add(stri):
            obj = datetime.strptime(stri, "%b")
            month_number = obj.month
            return month_number
        sym = 'FINNIFTY'
        fndata = finnifty_data()
        dff = fndata.copy()
        dff = dff[dff['ticker'].str.contains('FINNIFTY')]
        dff = dff[(dff['ticker'].str.endswith('E')) | (dff['ticker'].str.endswith('E.NFO'))]
        dff = dff.loc[:, ~dff.columns.str.contains('^Unnamed')]
        dff['time'] = pd.to_datetime(dff['time']).dt.time
        dff['date'] = pd.to_datetime(dff['date'],dayfirst=True)
        dff['ticker'] = dff['ticker'].str.replace('30MAR23','29MAR23',regex=True)

        dff['Option_Type'] = dff['ticker'].str[-2:]
        dff['Temp'] = dff["ticker"].str.replace(s,"")
        dff['Temp'] = dff['Temp'].str[:-2]
        dff['Length_of_Temp'] = dff['Temp'].str.len()
        dff['Strike'] = np.where((dff['Temp'].str.len()==9) | (dff['Temp'].str.len()==11) , 
                                  dff['Temp'].str[-4:] , 
                                  dff['Temp'].str[-5:])
        dff['Exp_Year'] = np.where((dff['Temp'].str.len()==9) | (dff['Temp'].str.len()==10) ,
                                   dff['Temp'].str[:2] ,
                                   dff['Temp'].str[5:7])
        dff['Exp_month'] = dff['Temp'].str[2:5]
        dff['EXPIRY_DT'] = dff['Temp'].str[:7]
        dff['EXPIRY_DT'] = pd.to_datetime(dff['EXPIRY_DT'],dayfirst=True)

        dff['Exp_Year'] = dff['Exp_Year'].astype('str')
        dff['MonthYear'] = dff['Exp_month']+dff['Exp_Year']
        dff = pd.merge(dff,exp_date,on='MonthYear')
        dff = dff.drop(['MonthYear','Month','Year'],axis=1)
        dff = dff.rename(columns={'Exp_DT':'Monthly_Expiry'})

        ## GETTING EXPIRY DATES FOR MONTHLY CONTRACTS
        dff['expiry_date'] = np.where(dff['Length_of_Temp']>=11,dff['EXPIRY_DT'],dff['Monthly_Expiry'])
        dff = dff.drop(['EXPIRY_DT',"Monthly_Expiry"],axis=1)
        ## GETTING WEEK NUMBER OF CURRENT DATE
        dff = dff.rename(columns={'Date':'date'})
        dff = pd.merge(dff,exp_df1,on='date',how='left')

        ## GETTING WEEK NUMBER OF EXPIRY DATES
        exp_df2['Weekly_Expiry_Date'] = pd.to_datetime(exp_df2['Weekly_Expiry_Date'],dayfirst=True)
        exp_df2 = exp_df2.dropna()
        exp_df2 = exp_df2.rename(columns = {'Weekly_Expiry_Date':'expiry_date'})
        temp_df = pd.merge(dff, exp_df2, on = 'expiry_date', how = 'left')
        temp_df = temp_df.drop_duplicates()
        temp_df['week_diff'] = temp_df['Expiry_Week_number'] - temp_df['Week_number']
        final_df = temp_df.copy()
        final_df["week_diff"] = final_df['week_diff'].replace(np.nan,10000)
        
        agb = final_df.groupby(["week_diff"])
        unique_val_list_a = list(final_df["week_diff"].unique())
        unique_val_list_a = sorted([a for a in unique_val_list_a if a>=0])[0:14]
        print(unique_val_list_a)
        for i in range(12):
            if(i==0):
                file='-I'
            if(i==1):
                file='-II'
            if(i==2):
                file='-III'
            if(i==3):
                file='-IV'
            if(i==4):
                file='-V'
            if(i==5):
                file='-VI'
            if(i==6):
                file='-VII'
            if(i==7):
                file='-VIII'
            if(i==8):
                file='-IX'
            if(i==9):
                file='-X'
            if(i==10):
                file='-XI'
            if(i==11):
                file='-XII'
                
            if(os.path.exists(folpath+sym+file+".csv")):
                os.remove(folpath+sym+file+".csv")

        for i in sorted(unique_val_list_a):
            temp_df = agb.get_group(i)
            temp_df = temp_df.drop(temp_df.columns[9:],axis=1)
            if i == 0:
                temp_df.to_csv(folpath + s + '-I.csv', mode = 'a', header = not os.path.exists(folpath + s + '-I.csv'), index=False)

            if i == 1:
                temp_df.to_csv(folpath + s + '-II.csv', mode = 'a', header = not os.path.exists(folpath + s + '-II.csv'), index=False)

            if i == 2:
                temp_df.to_csv(folpath + s + '-III.csv', mode = 'a', header = not os.path.exists(folpath + s + '-III.csv'), index=False)

            if i == 3:
                temp_df.to_csv(folpath + s + '-IV.csv', mode = 'a', header = not os.path.exists(folpath + s + '-IV.csv'), index=False)

            if i == 4:
                temp_df.to_csv(folpath + s + '-V.csv', mode = 'a', header = not os.path.exists(folpath + s + '-V.csv'), index=False)

            if i == 5:
                temp_df.to_csv(folpath + s + '-VI.csv', mode = 'a', header = not os.path.exists(folpath + s + '-VI.csv'), index=False)

            if i == 6:
                temp_df.to_csv(folpath + s + '-VII.csv', mode = 'a', header = not os.path.exists(folpath + s + '-VII.csv'), index=False)

            if i == 7:
                temp_df.to_csv(folpath + s + '-VIII.csv', mode = 'a', header = not os.path.exists(folpath + s + '-VIII.csv'), index=False)

            if i == 8:
                temp_df.to_csv(folpath + s + '-IX.csv', mode = 'a', header = not os.path.exists(folpath + s + '-IX.csv'), index=False)

            if i == 9:
                temp_df.to_csv(folpath + s + '-X.csv', mode = 'a', header = not os.path.exists(folpath + s + '-X.csv'), index=False)

            if i == 10:
                temp_df.to_csv(folpath + s + '-XI.csv', mode = 'a', header = not os.path.exists(folpath + s + '-XI.csv'), index=False)

            if i == 11:
                temp_df.to_csv(folpath + s + '-XII.csv', mode = 'a', header = not os.path.exists(folpath + s + '-XII.csv'), index=False)

            if i == 12:
                temp_df.to_csv(folpath + s + '-XIII.csv', mode = 'a', header = not os.path.exists(folpath + s + '-XIII.csv'), index=False)

            if i == 13:
                temp_df.to_csv(folpath + s + '-XIV.csv', mode = 'a', header = not os.path.exists(folpath + s + '-XIV.csv'), index=False)

        for i in range(12):
            if(i==0):
                file='-I'
            if(i==1):
                file='-II'
            if(i==2):
                file='-III'
            if(i==3):
                file='-IV'
            if(i==4):
                file='-V'
            if(i==5):
                file='-VI'
            if(i==6):
                file='-VII'
            if(i==7):
                file='-VIII'
            if(i==8):
                file='-IX'
            if(i==9):
                file='-X'
            if(i==10):
                file='-XI'
            if(i==11):
                file='-XII'
            if(i==12):
                file='-XIII'
            if(i==13):
                file='-XIV'
            if os.path.exists(finalpath+sym+file+".csv"):
                os.remove(finalpath+sym+file+".csv")
            if(os.path.exists(folpath+sym+file+".csv")):
                print(file)
                df = pd.read_csv(folpath+sym+str(file)+".csv")
                df['Option_Type'] = df['ticker'].str[-2:]
                df['Strike'] = np.where((df['ticker'].str.len()==19) | (df['ticker'].str.len()==21) , df['ticker'].str[-6:-2] , df['ticker'].str[-7:-2])
                df['Symbol'] = 'FINNIFTYWEEKLY' + file + df['Strike'].astype(int).astype(str) + df['Option_Type']
                df['ticker'] = df['Symbol']
                df = df.drop(df.columns[9:],axis=1)
                df = df.sort_values(by='date')
                print(df.shape[0])
                df = df.drop_duplicates()
                print(df.shape[0])
                df.to_csv(finalpath+sym+str(file)+".csv",index=False)
            else:
                print(file,"not exists")
        print("FINNIFTY WEEKLY CONTRACTS CREATED")

    print("BANKNIFTY CONTRACTS BEING CREATED")
    banknifty_monthly()
    banknifty_weekly()
    banknifty_quarterly()
    banknifty_halfyearly()
    banknifty_yearly()
    print("\nBANKNIFTY CONTRACTS CREATED")

    print("\n\nNIFTY CONTRACTS BEING CREATED")
    nifty_monthly()
    nifty_weekly()
    nifty_quarterly()
    nifty_halfyearly()
    nifty_yearly()
    print("\nNIFTY CONTRACTS CREATED")

    print("\n\nFINNIFTY CONTRACTS BEING CREATED")
    finnifty_monthly()
    finnifty_weekly()
    print("\nFINNIFTY CONTRACTS CREATED")

    with open (r"C:\Data\CsvFiles\ErrorLog.txt", 'w') as file:
        file.write('Nifty contracts created!')

    et = time.time()
    print("ELAPSED TIME",et-st)


    # In[ ]:

    #############################################################################
    #############################################################################

    #!/usr/bin/env python
    # coding: utf-8

    # In[3]:


    # -*- coding: utf-8 -*-
    """
    Created on Mon Feb 13 10:02:35 2023

    @author: admin
    """

    import psycopg2
    import time
    import pandas as pd
    from io import StringIO
    import re
    import os
    from os import walk
    from datetime import date

    st=time.time()
    #date1 = date.today()
    date1 = date(2023,5,19)
    day = date1.strftime('%d')
    nummonth=date1.strftime("%m")
    year=date1.strftime('%Y')
    datestr=str(year)+"-"+str(nummonth)+"-"+str(day)
    print(datestr)
    #choice = int(input("1 for BANKNIFTY\n2 for NIFTY\n3 for FINNIFTY\n4 to UPDATE ALL\n5 to EXIT\n"))
    choice = 4 # Updates all the indices
    path = "Administrator"

    def banknifty_updation():
        conn = psycopg2.connect(database="BankNiftydb",
                                                    user='postgres', password='swancap123',
                                                    host='swandatabase.cfehmk2wtejq.ap-south-1.rds.amazonaws.com', port='5432'
        )
        conn.autocommit = True
        cursor = conn.cursor()
        
        cursor = conn.cursor()
        print("APPENDING BANKNIFTY MONTHLY CONTRACTS")
        for i in range(1,4):
            if(i==1):
                numberstring="I"
                schema="BankNiftyMonthlyI"
                infotable="BANKNIFTYMONTHLY-Iinfo"
            if(i==2):
                numberstring="II"
                schema="BankNiftyMonthlyII"
                infotable="BANKNIFTYMONTHLY-IIinfo"
            if(i==3):
                numberstring="III"
                schema="BankNiftyMonthlyIII"
                infotable="BANKNIFTYMONTHLY-IIIinfo"
                
            bndf=pd.read_csv(rf"C:\\Users\\{path}\\Desktop\\Pyspark\\BankNifty\Monthly\Banknifty-"+numberstring+".csv")
            df=bndf.groupby(['Ticker'])
            for name,group in df:
                group.reset_index(drop=True,inplace=True)
                name = group.loc[0,'Ticker']
                sql2='''CREATE TABLE IF NOT EXISTS "'''+schema+'''"."'''+name+'''"(Ticker varchar(50) NOT NULL,Date Date NOT NULL,Time time NOT NULL,Open float NOT NULL,High float NOT NULL,Low float NOT NULL,Close float NOT NULL,Volume float NOT NULL,"Open Int" float NOT NULL);'''
                cursor.execute(sql2)
                conn.commit()
                noofrows=group.shape[0]
    #             print(noofrows)
                buffer = StringIO()
                group.to_csv(buffer, index = False)
                buffer.seek(0)
                sql = "COPY %s FROM STDIN WITH CSV HEADER DELIMITER AS ','"
                table='"'+schema+'"."'+name+'"'
                with conn.cursor() as cur:
                    #cur.execute("truncate " + table + ";")
                    cur.copy_expert(sql=sql % table, file=buffer)
                    conn.commit()
                cursor = conn.cursor()
                s='''Select 1 from "'''+schema+'''"."'''+infotable+'''" where ticker=\''''+name+'''\';'''
                cursor.execute(s)
                k=cursor.fetchall()
    #             print(k)
                if(k==[]):
                    num=re.findall(r"[-+]?(?:\d*\.*\d+)", name)
                    strike=num[0]
                    typecepe=name[-2:]
                    sql3 ='''INSERT INTO "'''+schema+'''"."'''+infotable+'''"(Ticker,Strike,Type)VALUES (%s,%s,%s)'''
                    record_to_insert = (name,strike,typecepe)
                    cursor.execute(sql3, record_to_insert)
                    conn.commit()
                sql4='''Select count(*) from "'''+schema+'''"."'''+name+'''" where date=\''''+datestr+'''\';'''
                cursor.execute(sql4)
                r=cursor.fetchall()
    #             print(r[0][0])
                if(r[0][0]!=noofrows):
                    print("numberofrows not matching")
                    break

        print("APPENDING BANKNIFTY WEEKLY CONTRACTS")
        for i in range(1,15):
            if(i==1):
                numberstring="I"
                schema="BankNiftyWeeklyI"
                infotable="BANKNIFTYWEEKLY-Iinfo"
            if(i==2):
                numberstring="II"
                schema="BankNiftyWeeklyII"
                infotable="BANKNIFTYWEEKLY-IIinfo"
            if(i==3):
                numberstring="III"
                schema="BankNiftyWeeklyIII"
                infotable="BANKNIFTYWEEKLY-IIIinfo"
            if(i==4):
                numberstring="IV"
                schema="BankNiftyWeeklyIV"
                infotable="BANKNIFTYWEEKLY-IVinfo"
            if(i==5):
                numberstring="V"
                schema="BankNiftyWeeklyV"
                infotable="BANKNIFTYWEEKLY-Vinfo"
            if(i==6):
                numberstring="VI"
                schema="BankNiftyWeeklyVI"
                infotable="BANKNIFTYWEEKLY-VIinfo"
            if(i==7):
                numberstring="VII"
                schema="BankNiftyWeeklyVII"
                infotable="BANKNIFTYWEEKLY-VIIinfo"
            if(i==8):
                numberstring="VIII"
                schema="BankNiftyWeeklyVIII"
                infotable="BANKNIFTYWEEKLY-VIIIinfo"
            if(i==9):
                numberstring="IX"
                schema="BankNiftyWeeklyIX"
                infotable="BANKNIFTYWEEKLY-IXinfo"
            if(i==10):
                numberstring="X"
                schema="BankNiftyWeeklyX"
                infotable="BANKNIFTYWEEKLY-Xinfo"
            if(i==11):
                numberstring="XI"
                schema="BankNiftyWeeklyXI"
                infotable="BANKNIFTYWEEKLY-XIinfo"
            if(i==12):
                numberstring="XII"
                schema="BankNiftyWeeklyXII"
                infotable="BANKNIFTYWEEKLY-XIIinfo"
            if(i==13):
                numberstring="XIII"
                schema="BankNiftyWeeklyXIII"
                infotable="BANKNIFTYWEEKLY-XIIIinfo"
            if(i==14):
                numberstring="XIV"
                schema="BankNiftyWeeklyXIV"
                infotable="BANKNIFTYWEEKLY-XIVinfo"
        
            if os.path.exists(rf"C:\Users\{path}\Desktop\Pyspark\BankNifty\Weekly\Banknifty-"+numberstring+".csv"):
                bndf=pd.read_csv(rf"C:\\Users\\{path}\\Desktop\\Pyspark\BankNifty\Weekly\\Banknifty-"+numberstring+".csv")
                df=bndf.groupby(['Ticker'])
                for name,group in df:
                    group.reset_index(drop=True,inplace=True)
                    name = group.loc[0,'Ticker']
                    sql2='''CREATE TABLE IF NOT EXISTS "'''+schema+'''"."'''+name+'''"(Ticker varchar(50) NOT NULL,Date Date NOT NULL,Time time NOT NULL,Open float NOT NULL,High float NOT NULL,Low float NOT NULL,Close float NOT NULL,Volume float NOT NULL,"Open Int" float NOT NULL);'''
                    cursor.execute(sql2)
                    conn.commit()
                    noofrows=group.shape[0]
    #                 print(noofrows)
                    buffer = StringIO()
                    group.to_csv(buffer, index = False)
                    buffer.seek(0)
                    sql = "COPY %s FROM STDIN WITH CSV HEADER DELIMITER AS ','"
                    table='"'+schema+'"."'+name+'"'
                    with conn.cursor() as cur:
                        #cur.execute("truncate " + table + ";")
                        cur.copy_expert(sql=sql % table, file=buffer)
                        conn.commit()
                    cursor = conn.cursor()
                    s='''Select 1 from "'''+schema+'''"."'''+infotable+'''" where ticker=\''''+name+'''\';'''
                    cursor.execute(s)
                    k=cursor.fetchall()
    #                 print(k)
                    if(k==[]):
                        num=re.findall(r"[-+]?(?:\d*\.*\d+)", name)
                        strike=num[0]
                        typecepe=name[-2:]
                        sql3 ='''INSERT INTO "'''+schema+'''"."'''+infotable+'''"(Ticker,Strike,Type)VALUES (%s,%s,%s)'''
                        record_to_insert = (name,strike,typecepe)
                        cursor.execute(sql3, record_to_insert)
                        conn.commit()
                    sql4='''Select count(*) from "'''+schema+'''"."'''+name+'''" where date=\''''+datestr+'''\';'''
                    cursor.execute(sql4)
                    r=cursor.fetchall()
    #                 print(r[0][0])
                    if(r[0][0]!=noofrows):
                        print("numberofrows not matching")
                        break

        print("APPENDING BANKNIFTY QUARTERLY CONTRACTS")
        for i in range(1,9):
            if(i==1):
                numberstring="I"
                schema="BankNiftyQuarterlyI"
                infotable="BANKNIFTYQUARTERLY-Iinfo"
            if(i==2):
                numberstring="II"
                schema="BankNiftyQuarterlyII"
                infotable="BANKNIFTYQUARTERLY-IIinfo"
            if(i==3):
                numberstring="III"
                schema="BankNiftyQuarterlyIII"
                infotable="BANKNIFTYQUARTERLY-IIIinfo"
            if(i==4):
                numberstring="IV"
                schema="BankNiftyQuarterlyIV"
                infotable="BANKNIFTYQUARTERLY-IVinfo"
            if(i==5):
                numberstring="V"
                schema="BankNiftyQuarterlyV"
                infotable="BANKNIFTYQUARTERLY-Vinfo"
            if(i==6):
                numberstring="VI"
                schema="BankNiftyQuarterlyVI"
                infotable="BANKNIFTYQUARTERLY-VIinfo"
            if(i==7):
                numberstring="VII"
                schema="BankNiftyQuarterlyVII"
                infotable="BANKNIFTYQUARTERLY-VIIinfo"
            if(i==8):
                numberstring="VIII"
                schema="BankNiftyQuarterlyVIII"
                infotable="BANKNIFTYQUARTERLY-VIIIinfo"
       
            if os.path.exists(rf"C:\Users\{path}\Desktop\Pyspark\BankNifty\Quarterly\Banknifty-"+numberstring+".csv"):
                bndf=pd.read_csv(rf"C:\\Users\\{path}\\Desktop\\Pyspark\BankNifty\Quarterly\\Banknifty-"+numberstring+".csv")
                df=bndf.groupby(['Ticker'])
                for name,group in df:
                    group.reset_index(drop=True,inplace=True)
                    name = group.loc[0,'Ticker']
                    sql2='''CREATE TABLE IF NOT EXISTS "'''+schema+'''"."'''+name+'''"(Ticker varchar(50) NOT NULL,Date Date NOT NULL,Time time NOT NULL,Open float NOT NULL,High float NOT NULL,Low float NOT NULL,Close float NOT NULL,Volume float NOT NULL,"Open Int" float NOT NULL);'''
                    cursor.execute(sql2)
                    conn.commit()
                    noofrows=group.shape[0]
    #                 print(noofrows)
                    buffer = StringIO()
                    group.to_csv(buffer, index = False)
                    buffer.seek(0)
                    sql = "COPY %s FROM STDIN WITH CSV HEADER DELIMITER AS ','"
                    table='"'+schema+'"."'+name+'"'
                    with conn.cursor() as cur:
                        #cur.execute("truncate " + table + ";")
                        cur.copy_expert(sql=sql % table, file=buffer)
                        conn.commit()
                    cursor = conn.cursor()
                    s='''Select 1 from "'''+schema+'''"."'''+infotable+'''" where ticker=\''''+name+'''\';'''
                    cursor.execute(s)
                    k=cursor.fetchall()
    #                 print(k)
                    if(k==[]):
                        num=re.findall(r"[-+]?(?:\d*\.*\d+)", name)
                        strike=num[0]
                        typecepe=name[-2:]
                        sql3 ='''INSERT INTO "'''+schema+'''"."'''+infotable+'''"(Ticker,Strike,Type)VALUES (%s,%s,%s)'''
                        record_to_insert = (name,strike,typecepe)
                        cursor.execute(sql3, record_to_insert)
                        conn.commit()
                    sql4='''Select count(*) from "'''+schema+'''"."'''+name+'''" where date=\''''+datestr+'''\';'''
                    cursor.execute(sql4)
                    r=cursor.fetchall()
    #                 print(r[0][0])
                    if(r[0][0]!=noofrows):
                        print("numberofrows not matching")
                        break

        print("APPENDING BANKNIFTY HALFYEARLY CONTRACTS")
        for i in range(1,5):
            if(i==1):
                numberstring="I"
                schema="BankNiftyHalfYearlyI"
                infotable="BANKNIFTYHALFYEARLY-IInfo"
            if(i==2):
                numberstring="II"
                schema="BankNiftyHalfYearlyII"
                infotable="BANKNIFTYHALFYEARLY-IIInfo"
            if(i==3):
                numberstring="III"
                schema="BankNiftyHalfYearlyIII"
                infotable="BANKNIFTYHALFYEARLY-IIIInfo"
            if(i==4):
                numberstring="IV"
                schema="BankNiftyHalfYearlyIV"
                infotable="BANKNIFTYHALFYEARLY-IVInfo"
        
            if os.path.exists(rf"C:\Users\{path}\Desktop\Pyspark\BankNifty\Half_Yearly\Banknifty-"+numberstring+".csv"):
                bndf=pd.read_csv(rf"C:\\Users\\{path}\\Desktop\\Pyspark\BankNifty\Half_Yearly\\Banknifty-"+numberstring+".csv")
                df=bndf.groupby(['Ticker'])
                for name,group in df:
                    group.reset_index(drop=True,inplace=True)
                    name = group.loc[0,'Ticker']
                    sql2='''CREATE TABLE IF NOT EXISTS "'''+schema+'''"."'''+name+'''"(Ticker varchar(50) NOT NULL,Date Date NOT NULL,Time time NOT NULL,Open float NOT NULL,High float NOT NULL,Low float NOT NULL,Close float NOT NULL,Volume float NOT NULL,"Open Int" float NOT NULL);'''
                    cursor.execute(sql2)
                    conn.commit()
                    noofrows=group.shape[0]
    #                 print(noofrows)
                    buffer = StringIO()
                    group.to_csv(buffer, index = False)
                    buffer.seek(0)
                    sql = "COPY %s FROM STDIN WITH CSV HEADER DELIMITER AS ','"
                    table='"'+schema+'"."'+name+'"'
                    with conn.cursor() as cur:
                        #cur.execute("truncate " + table + ";")
                        cur.copy_expert(sql=sql % table, file=buffer)
                        conn.commit()
                    cursor = conn.cursor()
                    s='''Select 1 from "'''+schema+'''"."'''+infotable+'''" where ticker=\''''+name+'''\';'''
                    cursor.execute(s)
                    k=cursor.fetchall()
    #                 print(k)
                    if(k==[]):
                        num=re.findall(r"[-+]?(?:\d*\.*\d+)", name)
                        strike=num[0]
                        typecepe=name[-2:]
                        sql3 ='''INSERT INTO "'''+schema+'''"."'''+infotable+'''"(Ticker,Strike,Type)VALUES (%s,%s,%s)'''
                        record_to_insert = (name,strike,typecepe)
                        cursor.execute(sql3, record_to_insert)
                        conn.commit()
                    sql4='''Select count(*) from "'''+schema+'''"."'''+name+'''" where date=\''''+datestr+'''\';'''
                    cursor.execute(sql4)
                    r=cursor.fetchall()
    #                 print(r[0][0])
                    if(r[0][0]!=noofrows):
                        print("numberofrows not matching")
                        break

        print("APPENDING BANKNIFTY YEARLY CONTRACTS")
        for i in range(1,3):
            if(i==1):
                numberstring="I"
                schema="BankNiftyYearlyI"
                infotable="BANKNIFTYYEARLY-IInfo"
            if(i==2):
                numberstring="II"
                schema="BankNiftyYearlyII"
                infotable="BANKNIFTYYEARLY-IIInfo"
                
            if os.path.exists(rf"C:\Users\{path}\Desktop\Pyspark\BankNifty\Yearly\Banknifty-"+numberstring+".csv"):
                bndf=pd.read_csv(rf"C:\\Users\\{path}\\Desktop\\Pyspark\BankNifty\Yearly\\Banknifty-"+numberstring+".csv")
                df=bndf.groupby(['Ticker'])
                for name,group in df:
                    group.reset_index(drop=True,inplace=True)
                    name = group.loc[0,'Ticker']
                    sql2='''CREATE TABLE IF NOT EXISTS "'''+schema+'''"."'''+name+'''"(Ticker varchar(50) NOT NULL,Date Date NOT NULL,Time time NOT NULL,Open float NOT NULL,High float NOT NULL,Low float NOT NULL,Close float NOT NULL,Volume float NOT NULL,"Open Int" float NOT NULL);'''
                    cursor.execute(sql2)
                    conn.commit()
                    noofrows=group.shape[0]
    #                 print(noofrows)
                    buffer = StringIO()
                    group.to_csv(buffer, index = False)
                    buffer.seek(0)
                    sql = "COPY %s FROM STDIN WITH CSV HEADER DELIMITER AS ','"
                    table='"'+schema+'"."'+name+'"'
                    with conn.cursor() as cur:
                        #cur.execute("truncate " + table + ";")
                        cur.copy_expert(sql=sql % table, file=buffer)
                        conn.commit()
                    cursor = conn.cursor()
                    s='''Select 1 from "'''+schema+'''"."'''+infotable+'''" where ticker=\''''+name+'''\';'''
                    cursor.execute(s)
                    k=cursor.fetchall()
    #                 print(k)
                    if(k==[]):
                        num=re.findall(r"[-+]?(?:\d*\.*\d+)", name)
                        strike=num[0]
                        typecepe=name[-2:]
                        sql3 ='''INSERT INTO "'''+schema+'''"."'''+infotable+'''"(Ticker,Strike,Type)VALUES (%s,%s,%s)'''
                        record_to_insert = (name,strike,typecepe)
                        cursor.execute(sql3, record_to_insert)
                        conn.commit()
                    sql4='''Select count(*) from "'''+schema+'''"."'''+name+'''" where date=\''''+datestr+'''\';'''
                    cursor.execute(sql4)
                    r=cursor.fetchall()
    #                 print(r[0][0])
                    if(r[0][0]!=noofrows):
                        print("numberofrows not matching")
                        break
        conn.close()

############################################### NIFTY UPDATION #####################################################
    def nifty_updation():
        conn = psycopg2.connect(database="Niftydb",
                                user='postgres', password='swancap123',
                                host='swandatabase.cfehmk2wtejq.ap-south-1.rds.amazonaws.com', port='5432'
        )
        conn.autocommit = True
        cursor = conn.cursor()
        print("APPENDING NIFTY MONTHLY CONTRACTS")
        for i in range(1,4):
            if(i==1):
                numberstring="I"
                schema="NiftyMonthlyI"
                infotable="NIFTYMONTHLY-IInfo"
            if(i==2):
                numberstring="II"
                schema="NiftyMonthlyII"
                infotable="NIFTYMONTHLY-IIInfo"
            if(i==3):
                numberstring="III"
                schema="NiftyMonthlyIII"
                infotable="NIFTYMONTHLY-IIIInfo"
            bndf=pd.read_csv(rf"C:\\Users\\{path}\\Desktop\\Pyspark\\Nifty\\Monthly\\NIFTY-"+numberstring+".csv")
            df=bndf.groupby(['Ticker'])
            for name,group in df:
                group.reset_index(drop=True,inplace=True)
                name = group.loc[0,'Ticker']
                sql2='''CREATE TABLE IF NOT EXISTS "'''+schema+'''"."'''+name+'''"(Ticker varchar(50) NOT NULL,Date Date NOT NULL,Time time NOT NULL,Open float NOT NULL,High float NOT NULL,Low float NOT NULL,Close float NOT NULL,Volume float NOT NULL,"Open Int" float NOT NULL);'''
                cursor.execute(sql2)
                conn.commit()
                noofrows=group.shape[0]
    #             print(noofrows)
                buffer = StringIO()
                group.to_csv(buffer, index = False)
                buffer.seek(0)
                sql = "COPY %s FROM STDIN WITH CSV HEADER DELIMITER AS ','"
                table='"'+schema+'"."'+name+'"'
                with conn.cursor() as cur:
                    #cur.execute("truncate " + table + ";")
                    cur.copy_expert(sql=sql % table, file=buffer)
                    conn.commit()
                cursor = conn.cursor()
                s='''Select 1 from "'''+schema+'''"."'''+infotable+'''" where ticker=\''''+name+'''\';'''
                cursor.execute(s)
                k=cursor.fetchall()
    #             print(k)
                if(k==[]):
                    num=re.findall(r"[-+]?(?:\d*\.*\d+)", name)
                    strike=num[0]
                    typecepe=name[-2:]
                    sql3 ='''INSERT INTO "'''+schema+'''"."'''+infotable+'''"(Ticker,Strike,Type)VALUES (%s,%s,%s)'''
                    record_to_insert = (name,strike,typecepe)
                    cursor.execute(sql3, record_to_insert)
                    conn.commit()
                sql4='''Select count(*) from "'''+schema+'''"."'''+name+'''" where date=\''''+datestr+'''\';'''
                cursor.execute(sql4)
                r=cursor.fetchall()
    #             print(r[0][0])
                if(r[0][0]!=noofrows):
                    print("numberofrows not matching")
                    break
        
        print("APPENDING NIFTY WEEKLY CONTRACTS")
        for i in range(1,15):
            if(i==1):
                numberstring="I"
                schema="NiftyWeeklyI"
                infotable="NIFTYWEEKLY-IInfo"
            if(i==2):
                numberstring="II"
                schema="NiftyWeeklyII"
                infotable="NIFTYWEEKLY-IIInfo"
            if(i==3):
                numberstring="III"
                schema="NiftyWeeklyIII"
                infotable="NIFTYWEEKLY-IIIInfo"
            if(i==4):
                numberstring="IV"
                schema="NiftyWeeklyIV"
                infotable="NIFTYWEEKLY-IVInfo"
            if(i==5):
                numberstring="V"
                schema="NiftyWeeklyV"
                infotable="NIFTYWEEKLY-VInfo"
            if(i==6):
                numberstring="VI"
                schema="NiftyWeeklyVI"
                infotable="NIFTYWEEKLY-VIInfo"
            if(i==7):
                numberstring="VII"
                schema="NiftyWeeklyVII"
                infotable="NIFTYWEEKLY-VIIInfo"
            if(i==8):
                numberstring="VIII"
                schema="NiftyWeeklyVIII"
                infotable="NIFTYWEEKLY-VIIIInfo"
            if(i==9):
                numberstring="IX"
                schema="NiftyWeeklyIX"
                infotable="NIFTYWEEKLY-IXInfo"
            if(i==10):
                numberstring="X"
                schema="NiftyWeeklyX"
                infotable="NIFTYWEEKLY-XInfo"
            if(i==11):
                numberstring="XI"
                schema="NiftyWeeklyXI"
                infotable="NIFTYWEEKLY-XIInfo"
            if(i==12):
                numberstring="XII"
                schema="NiftyWeeklyXII"
                infotable="NIFTYWEEKLY-XIIInfo"
            if(i==13):
                numberstring="XIII"
                schema="NiftyWeeklyXIII"
                infotable="NIFTYWEEKLY-XIIIInfo"
            if(i==14):
                numberstring="XIV"
                schema="NiftyWeeklyXIV"
                infotable="NIFTYWEEKLY-XIVInfo"
            if os.path.exists(rf"C:\Users\{path}\Desktop\Pyspark\Nifty\Weekly\NIFTY-"+numberstring+".csv"):
                bndf=pd.read_csv(rf"C:\Users\{path}\Desktop\Pyspark\Nifty\Weekly\NIFTY-"+numberstring+".csv")
                df=bndf.groupby(['Ticker'])
                for name,group in df:
                    group.reset_index(drop=True,inplace=True)
                    name = group.loc[0,'Ticker']
                    sql2='''CREATE TABLE IF NOT EXISTS "'''+schema+'''"."'''+name+'''"(Ticker varchar(50) NOT NULL,Date Date NOT NULL,Time time NOT NULL,Open float NOT NULL,High float NOT NULL,Low float NOT NULL,Close float NOT NULL,Volume float NOT NULL,"Open Int" float NOT NULL);'''
                    cursor.execute(sql2)
                    conn.commit()
                    noofrows=group.shape[0]
    #                 print(noofrows)
                    buffer = StringIO()
                    group.to_csv(buffer, index = False)
                    buffer.seek(0)
                    sql = "COPY %s FROM STDIN WITH CSV HEADER DELIMITER AS ','"
                    table='"'+schema+'"."'+name+'"'
                    with conn.cursor() as cur:
                        #cur.execute("truncate " + table + ";")
                        cur.copy_expert(sql=sql % table, file=buffer)
                        conn.commit()
                    cursor = conn.cursor()
                    s='''Select 1 from "'''+schema+'''"."'''+infotable+'''" where ticker=\''''+name+'''\';'''
                    cursor.execute(s)
                    k=cursor.fetchall()
    #                 print(k)
                    if(k==[]):
                        num=re.findall(r"[-+]?(?:\d*\.*\d+)", name)
                        strike=num[0]
                        typecepe=name[-2:]
                        sql3 ='''INSERT INTO "'''+schema+'''"."'''+infotable+'''"(Ticker,Strike,Type)VALUES (%s,%s,%s)'''
                        record_to_insert = (name,strike,typecepe)
                        cursor.execute(sql3, record_to_insert)
                        conn.commit()
                    sql4='''Select count(*) from "'''+schema+'''"."'''+name+'''" where date=\''''+datestr+'''\';'''
                    cursor.execute(sql4)
                    r=cursor.fetchall()
    #                 print(r[0][0])
                    if(r[0][0]!=noofrows):
                        print("numberofrows not matching")
                        break
        
        print("APPENDING NIFTY QUARTERLY CONTRACTS")
        for i in range(1,5):
            if(i==1):
                numberstring="I"
                schema="NiftyQuarterlyI"
                infotable="NIFTYQUARTERLY-Iinfo"
            if(i==2):
                numberstring="II"
                schema="NiftyQuarterlyII"
                infotable="NIFTYQUARTERLY-IIinfo"
            if(i==3):
                numberstring="III"
                schema="NiftyQuarterlyIII"
                infotable="NIFTYQUARTERLY-IIIinfo"
            if(i==4):
                numberstring="IV"
                schema="NiftyQuarterlyIV"
                infotable="NIFTYQUARTERLY-IVinfo"
            if os.path.exists(rf"C:\Users\{path}\Desktop\Pyspark\Nifty\Quarterly\NIFTY-"+numberstring+".csv"):
                bndf=pd.read_csv(rf"C:\\Users\\{path}\\Desktop\\Pyspark\Nifty\Quarterly\\NIFTY-"+numberstring+".csv")
                df=bndf.groupby(['Ticker'])
                for name,group in df:
                    group.reset_index(drop=True,inplace=True)
                    name = group.loc[0,'Ticker']
                    sql2='''CREATE TABLE IF NOT EXISTS "'''+schema+'''"."'''+name+'''"(Ticker varchar(50) NOT NULL,Date Date NOT NULL,Time time NOT NULL,Open float NOT NULL,High float NOT NULL,Low float NOT NULL,Close float NOT NULL,Volume float NOT NULL,"Open Int" float NOT NULL);'''
                    cursor.execute(sql2)
                    conn.commit()
                    noofrows=group.shape[0]
    #                 print(noofrows)
                    buffer = StringIO()
                    group.to_csv(buffer, index = False)
                    buffer.seek(0)
                    sql = "COPY %s FROM STDIN WITH CSV HEADER DELIMITER AS ','"
                    table='"'+schema+'"."'+name+'"'
                    with conn.cursor() as cur:
                        #cur.execute("truncate " + table + ";")
                        cur.copy_expert(sql=sql % table, file=buffer)
                        conn.commit()
                    cursor = conn.cursor()
                    s='''Select 1 from "'''+schema+'''"."'''+infotable+'''" where ticker=\''''+name+'''\';'''
                    cursor.execute(s)
                    k=cursor.fetchall()
    #                 print(k)
                    if(k==[]):
                        num=re.findall(r"[-+]?(?:\d*\.*\d+)", name)
                        strike=num[0]
                        typecepe=name[-2:]
                        sql3 ='''INSERT INTO "'''+schema+'''"."'''+infotable+'''"(Ticker,Strike,Type)VALUES (%s,%s,%s)'''
                        record_to_insert = (name,strike,typecepe)
                        cursor.execute(sql3, record_to_insert)
                        conn.commit()
                    sql4='''Select count(*) from "'''+schema+'''"."'''+name+'''" where date=\''''+datestr+'''\';'''
                    cursor.execute(sql4)
                    r=cursor.fetchall()
    #                 print(r[0][0])
                    if(r[0][0]!=noofrows):
                        print("numberofrows not matching")
                        break

        print("APPENDING NIFTY HALFYEARLY CONTRACTS")
        for i in range(1,11):
            if(i==1):
                numberstring="I"
                schema="NiftyHalfYearlyI"
                infotable="NIFTYHALFYEARLY-Iinfo"
            if(i==2):
                numberstring="II"
                schema="NiftyHalfYearlyII"
                infotable="NIFTYHALFYEARLY-IIinfo"
            if(i==3):
                numberstring="III"
                schema="NiftyHalfYearlyIII"
                infotable="NIFTYHALFYEARLY-IIIinfo"
            if(i==4):
                numberstring="IV"
                schema="NiftyHalfYearlyIV"
                infotable="NIFTYHALFYEARLY-IVinfo"
            if(i==5):
                numberstring="V"
                schema="NiftyHalfYearlyV"
                infotable="NIFTYHALFYEARLY-Vinfo"
            if(i==6):
                numberstring="VI"
                schema="NiftyHalfYearlyVI"
                infotable="NIFTYHALFYEARLY-VIinfo"
            if(i==7):
                numberstring="VII"
                schema="NiftyHalfYearlyVII"
                infotable="NIFTYHALFYEARLY-VIIinfo"
            if(i==8):
                numberstring="VIII"
                schema="NiftyHalfYearlyVIII"
                infotable="NIFTYHALFYEARLY-VIIIinfo"
            if(i==9):
                numberstring="IX"
                schema="NiftyHalfYearlyIX"
                infotable="NIFTYHALFYEARLY-IXinfo"
            if(i==10):
                numberstring="X"
                schema="NiftyHalfYearlyX"
                infotable="NIFTYHALFYEARLY-Xinfo"
            if os.path.exists(rf"C:\Users\{path}\Desktop\Pyspark\Nifty\Half_Yearly\NIFTY-"+numberstring+".csv"):
                bndf=pd.read_csv(rf"C:\\Users\\{path}\\Desktop\\Pyspark\Nifty\Half_Yearly\\NIFTY-"+numberstring+".csv")
                df=bndf.groupby(['ticker'])
                for name,group in df:
                    group.reset_index(drop=True,inplace=True)
                    name = group.loc[0,'ticker']
                    sql2='''CREATE TABLE IF NOT EXISTS "'''+schema+'''"."'''+name+'''"(Ticker varchar(50) NOT NULL,Date Date NOT NULL,Time time NOT NULL,Open float NOT NULL,High float NOT NULL,Low float NOT NULL,Close float NOT NULL,Volume float NOT NULL,"Open Int" float NOT NULL);'''
                    cursor.execute(sql2)
                    conn.commit()
                    noofrows=group.shape[0]
    #                 print(noofrows)
                    buffer = StringIO()
                    group.to_csv(buffer, index = False)
                    buffer.seek(0)
                    sql = "COPY %s FROM STDIN WITH CSV HEADER DELIMITER AS ','"
                    table='"'+schema+'"."'+name+'"'
                    with conn.cursor() as cur:
                        #cur.execute("truncate " + table + ";")
                        cur.copy_expert(sql=sql % table, file=buffer)
                        conn.commit()
                    cursor = conn.cursor()
                    s='''Select 1 from "'''+schema+'''"."'''+infotable+'''" where ticker=\''''+name+'''\';'''
                    cursor.execute(s)
                    k=cursor.fetchall()
    #                 print(k)
                    if(k==[]):
                        num=re.findall(r"[-+]?(?:\d*\.*\d+)", name)
                        strike=num[0]
                        typecepe=name[-2:]
                        sql3 ='''INSERT INTO "'''+schema+'''"."'''+infotable+'''"(Ticker,Strike,Type)VALUES (%s,%s,%s)'''
                        record_to_insert = (name,strike,typecepe)
                        cursor.execute(sql3, record_to_insert)
                        conn.commit()
                    sql4='''Select count(*) from "'''+schema+'''"."'''+name+'''" where date=\''''+datestr+'''\';'''
                    cursor.execute(sql4)
                    r=cursor.fetchall()
    #                 print(r[0][0])
                    if(r[0][0]!=noofrows):
                        print("numberofrows not matching")
                        break

        print("APPENDING NIFTY YEARLY CONTRACTS")
        for i in range(1,6):
            if(i==1):
                numberstring="I"
                schema="NiftyYearlyI"
                infotable="NIFTYYEARLY-Iinfo"
            if(i==2):
                numberstring="II"
                schema="NiftyYearlyII"
                infotable="NIFTYYEARLY-IIinfo"
            if(i==3):
                numberstring="III"
                schema="NiftyYearlyIII"
                infotable="NIFTYYEARLY-IIIinfo"
            if(i==4):
                numberstring="IV"
                schema="NiftyYearlyIV"
                infotable="NIFTYYEARLY-IVinfo"
            if(i==5):
                numberstring="V"
                schema="NiftyYearlyV"
                infotable="NIFTYYEARLY-Vinfo"
            if os.path.exists(rf"C:\Users\{path}\Desktop\Pyspark\Nifty\Yearly\NIFTY-"+numberstring+".csv"):
                bndf=pd.read_csv(rf"C:\\Users\\{path}\\Desktop\\Pyspark\Nifty\Yearly\\NIFTY-"+numberstring+".csv")
                df=bndf.groupby(['ticker'])
                for name,group in df:
                    group.reset_index(drop=True,inplace=True)
                    name = group.loc[0,'ticker']
                    sql2='''CREATE TABLE IF NOT EXISTS "'''+schema+'''"."'''+name+'''"(Ticker varchar(50) NOT NULL,Date Date NOT NULL,Time time NOT NULL,Open float NOT NULL,High float NOT NULL,Low float NOT NULL,Close float NOT NULL,Volume float NOT NULL,"Open Int" float NOT NULL);'''
                    cursor.execute(sql2)
                    conn.commit()
                    noofrows=group.shape[0]
    #                 print(noofrows)
                    buffer = StringIO()
                    group.to_csv(buffer, index = False)
                    buffer.seek(0)
                    sql = "COPY %s FROM STDIN WITH CSV HEADER DELIMITER AS ','"
                    table='"'+schema+'"."'+name+'"'
                    with conn.cursor() as cur:
                        #cur.execute("truncate " + table + ";")
                        cur.copy_expert(sql=sql % table, file=buffer)
                        conn.commit()
                    cursor = conn.cursor()
                    s='''Select 1 from "'''+schema+'''"."'''+infotable+'''" where ticker=\''''+name+'''\';'''
                    cursor.execute(s)
                    k=cursor.fetchall()
    #                 print(k)
                    if(k==[]):
                        num=re.findall(r"[-+]?(?:\d*\.*\d+)", name)
                        strike=num[0]
                        typecepe=name[-2:]
                        sql3 ='''INSERT INTO "'''+schema+'''"."'''+infotable+'''"(Ticker,Strike,Type)VALUES (%s,%s,%s)'''
                        record_to_insert = (name,strike,typecepe)
                        cursor.execute(sql3, record_to_insert)
                        conn.commit()
                    sql4='''Select count(*) from "'''+schema+'''"."'''+name+'''" where date=\''''+datestr+'''\';'''
                    cursor.execute(sql4)
                    r=cursor.fetchall()
    #                 print(r[0][0])
                    if(r[0][0]!=noofrows):
                        print("numberofrows not matching")
                        break
        conn.close()
    
    def finnifty_updation():
        conn = psycopg2.connect(database="FinNiftydb",
                                                    user='postgres', password='swancap123',
                                                    host='swandatabase.cfehmk2wtejq.ap-south-1.rds.amazonaws.com', port='5432'
        )
        conn.autocommit = True
        cursor = conn.cursor()
        folpath = rf"C:\Users\{path}\Desktop\Pyspark\FinNifty\Monthly\\"
        sym='FINNIFTY-'
        print("APPENDING FINNIFTY MONTHLY CONTRACTS")
        for i in range(1,4):
            if(i==1):
                numberstring="I"
                schema="FinNiftyMonthlyI"
                infotable="FINNIFTYMONTHLY-Iinfo"
            if(i==2):
                numberstring="II"
                schema="FinNiftyMonthlyII"
                infotable="FINNIFTYMONTHLY-IIinfo"
            if(i==3):
                numberstring="III"
                schema="FinNiftyMonthlyIII"
                infotable="FINNIFTYMONTHLY-IIIinfo"
        
            if os.path.exists(folpath+sym+numberstring+".csv"):    
                bndf=pd.read_csv(folpath+sym+numberstring+".csv")
                df=bndf.groupby(['ticker'])
                for name,group in df:
                    group.reset_index(drop=True,inplace=True)
                    name = group.loc[0,'ticker']
                    sql2='''CREATE TABLE IF NOT EXISTS "'''+schema+'''"."'''+name+'''"(Ticker varchar(50) NOT NULL,Date Date NOT NULL,Time time NOT NULL,Open float NOT NULL,High float NOT NULL,Low float NOT NULL,Close float NOT NULL,Volume float NOT NULL,"Open Int" float NOT NULL);'''
                    cursor.execute(sql2)
                    conn.commit()
                    noofrows=group.shape[0]
    #                 print(noofrows)
                    buffer = StringIO()
                    group.to_csv(buffer, index = False)
                    buffer.seek(0)
                    sql = "COPY %s FROM STDIN WITH CSV HEADER DELIMITER AS ','"
                    table='"'+schema+'"."'+name+'"'
                    with conn.cursor() as cur:
                        #cur.execute("truncate " + table + ";")
                        cur.copy_expert(sql=sql % table, file=buffer)
                        conn.commit()
                    cursor = conn.cursor()
                    s='''Select 1 from "'''+schema+'''"."'''+infotable+'''" where ticker=\''''+name+'''\';'''
                    cursor.execute(s)
                    k=cursor.fetchall()
    #                 print(k)
                    if(k==[]):
                        num=re.findall(r"[-+]?(?:\d*\.*\d+)", name)
                        strike=num[0]
                        typecepe=name[-2:]
                        sql3 ='''INSERT INTO "'''+schema+'''"."'''+infotable+'''"(Ticker,Strike,Type)VALUES (%s,%s,%s)'''
                        record_to_insert = (name,strike,typecepe)
                        cursor.execute(sql3, record_to_insert)
                        conn.commit()
                    sql4='''Select count(*) from "'''+schema+'''"."'''+name+'''" where date=\''''+datestr+'''\';'''
                    cursor.execute(sql4)
                    r=cursor.fetchall()
    #                 print(r[0][0])
                    if(r[0][0]!=noofrows):
                        print("numberofrows not matching")
                        break

        folpath = rf"C:\Users\{path}\Desktop\Pyspark\FinNifty\Weekly\\"
        sym='FINNIFTY-'
        print("APPENDING FINNIFTY WEEKLY CONTRACTS")
        for i in range(1,15):
            if(i==1):
                numberstring="I"
                schema="FinNiftyWeeklyI"
                infotable="FINNIFTYWEEKLY-Iinfo"
            if(i==2):
                numberstring="II"
                schema="FinNiftyWeeklyII"
                infotable="FINNIFTYWEEKLY-IIinfo"
            if(i==3):
                numberstring="III"
                schema="FinNiftyWeeklyIII"
                infotable="FINNIFTYWEEKLY-IIIinfo"
            if(i==4):
                numberstring="IV"
                schema="FinNiftyWeeklyIV"
                infotable="FINNIFTYWEEKLY-IVinfo"
            if(i==5):
                numberstring="V"
                schema="FinNiftyWeeklyV"
                infotable="FINNIFTYWEEKLY-Vinfo"
            if(i==6):
                numberstring="VI"
                schema="FinNiftyWeeklyVI"
                infotable="FINNIFTYWEEKLY-VIinfo"
            if(i==7):
                numberstring="VII"
                schema="FinNiftyWeeklyVII"
                infotable="FINNIFTYWEEKLY-VIIinfo"
            if(i==8):
                numberstring="VIII"
                schema="FinNiftyWeeklyVIII"
                infotable="FINNIFTYWEEKLY-VIIIinfo"
            if(i==9):
                numberstring="IX"
                schema="FinNiftyWeeklyIX"
                infotable="FINNIFTYWEEKLY-IXinfo"
            if(i==10):
                numberstring="X"
                schema="FinNiftyWeeklyX"
                infotable="FINNIFTYWEEKLY-Xinfo"
            if(i==11):
                numberstring="XI"
                schema="FinNiftyWeeklyXI"
                infotable="FINNIFTYWEEKLY-XIinfo"
            if(i==12):
                numberstring="XII"
                schema="FinNiftyWeeklyXII"
                infotable="FINNIFTYWEEKLY-XIIinfo"
            if(i==13):
                numberstring="XIII"
                schema="FinNiftyWeeklyXIII"
                infotable="FINNIFTYWEEKLY-XIIIinfo"
            if(i==14):
                numberstring="XIV"
                schema="FinNiftyWeeklyXIV"
                infotable="FINNIFTYWEEKLY-XIVinfo"

            if os.path.exists(folpath+sym+numberstring+".csv"):    
                bndf=pd.read_csv(folpath+sym+numberstring+".csv")
                df=bndf.groupby(['ticker'])
                for name,group in df:
                    group.reset_index(drop=True,inplace=True)
                    name = group.loc[0,'ticker']
                    sql2='''CREATE TABLE IF NOT EXISTS "'''+schema+'''"."'''+name+'''"(Ticker varchar(50) NOT NULL,Date Date NOT NULL,Time time NOT NULL,Open float NOT NULL,High float NOT NULL,Low float NOT NULL,Close float NOT NULL,Volume float NOT NULL,"Open Int" float NOT NULL);'''
                    cursor.execute(sql2)
                    conn.commit()
                    noofrows=group.shape[0]
    #                 print(noofrows)
                    buffer = StringIO()
                    group.to_csv(buffer, index = False)
                    buffer.seek(0)
                    sql = "COPY %s FROM STDIN WITH CSV HEADER DELIMITER AS ','"
                    table='"'+schema+'"."'+name+'"'
                    with conn.cursor() as cur:
                        #cur.execute("truncate " + table + ";")
                        cur.copy_expert(sql=sql % table, file=buffer)
                        conn.commit()
                    cursor = conn.cursor()
                    s='''Select 1 from "'''+schema+'''"."'''+infotable+'''" where ticker=\''''+name+'''\';'''
                    cursor.execute(s)
                    k=cursor.fetchall()
    #                 print(k)
                    if(k==[]):
                        num=re.findall(r"[-+]?(?:\d*\.*\d+)", name)
                        strike=num[0]
                        typecepe=name[-2:]
                        sql3 ='''INSERT INTO "'''+schema+'''"."'''+infotable+'''"(Ticker,Strike,Type)VALUES (%s,%s,%s)'''
                        record_to_insert = (name,strike,typecepe)
                        cursor.execute(sql3, record_to_insert)
                        conn.commit()
                    sql4='''Select count(*) from "'''+schema+'''"."'''+name+'''" where date=\''''+datestr+'''\';'''
                    cursor.execute(sql4)
                    r=cursor.fetchall()
    #                 print(r[0][0])
                    if(r[0][0]!=noofrows):
                        print("numberofrows not matching")
                        break
        conn.close()      


    
    with open (r"C:\Data\CsvFiles\ErrorLog.txt", 'w') as file:
        file.write('FinNifty contracts appended!')
          

    def option(num):
        if num==1:
            ## CONNECTION FOR BANKNIFTY
            def function_call():
                val = int(input("1 for MONTHLY\n2 for WEEKLY\n3 for QUARTERLY\n4 for HALF YEARLY\n5 for YEARLY\n6 to update ALL\n7 to EXIT\n"))
                if val==1:
                    banknifty_monthly()
                elif val==2:
                    banknifty_weekly()
                elif val==3:
                    banknifty_quarterly()
                elif val==4:
                    banknifty_halfyearly()
                elif val==5:
                    banknifty_yearly()
                elif val==6:
                    banknifty_monthly()
                    banknifty_weekly()
                    banknifty_quarterly()
                    banknifty_halfyearly()
                    banknifty_yearly()
                elif val==7:
                    print("EXIT!!!")
        
                else:
                    print("WRONG OPTION")
                    function_call()
            function_call()
                    
        elif num==2:
            ## CONNECTION FOR NIFTY
            def function_call():
                val = int(input("1 for MONTHLY\n2 for WEEKLY\n3 for QUARTERLY\n4 for HALF YEARLY\n5 for YEARLY\n6 to update ALL\n7 to EXIT\n"))
                if val==1:
                    nifty_monthly()
                elif val==2:
                    nifty_weekly()
                elif val==3:
                    nifty_quarterly()
                elif val==4:
                    nifty_halfyearly()
                elif val==5:
                    nifty_yearly()
                elif val==6:
                    nifty_monthly()
                    nifty_weekly()
                    nifty_quarterly()
                    nifty_halfyearly()
                    nifty_yearly()
                elif val==7:
                    print("EXIT!!!")
        
                else:
                    print("WRONG OPTION")
                    function_call()
            function_call()
        
        elif num==3:
            ## CONNECTION FOR FINNIFTY
            def function_call():
                val = int(input("1 for MONTHLY\n2 for WEEKLY\n3 to update BOTH\n4 to EXIT\n"))
                if val==1:
                    finnifty_monthly()
                        
                elif val==2:
                    finnifty_weekly()
            
                elif val==3:
                    finnifty_monthly()
                    finnifty_weekly()
                elif val==4:
                    print("EXIT!!!")

                else:
                    print("WRONG OPTION")
                    function_call()
            function_call()
            
        elif num==4:
            banknifty_updation()
            nifty_updation()
            finnifty_updation()
            
        
        elif num==5:
            print("BREAKING")
            
        else:
            print("Wrong option.\nPlease enter correct option")
            num = int(input("1 for BANKNIFTY\n2 for NIFTY\n3 for FINNIFTY\n4 to UPDATE ALL\n5 to EXIT\n"))
            return option(num)
        
    option(choice)

    et=time.time()

    elapsed_time=et-st;
    print("elapsed_time:",elapsed_time)


    ###############################################################################
    ###############################################################################
                               ## TELEGRAM UPDATION
    import requests
    from telethon.sync import TelegramClient
    import pandas as pd
    import datetime as dt

    link1 = 'https://www.dropbox.com/sh/wizrnlt2ru13dgr/AADA2I7VvQpTqfb6s93CS2a7a?dl=0'

    link1 = 'Data updation completed successfully!'
    base_url =f"https://api.telegram.org/bot6237928541:AAHl267HrSFBRFE-iIajz_x8eNkPydiQEEs/sendMessage?chat_id=-939411532&text={link1}"
    requests.get(base_url)

    ###############################################################################
    ###############################################################################

except Exception as e:

    print('Error : ', e)

    link1 = 'Data updation failed!'
    base_url =f"https://api.telegram.org/bot6237928541:AAHl267HrSFBRFE-iIajz_x8eNkPydiQEEs/sendMessage?chat_id=-939411532&text={link1}"
    requests.get(base_url)
        


      


