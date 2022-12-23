# Daily Updation of Rawdata: 

## Pseudo Code:     
  1.Fetch the csv file for the given date(or today) from the given path.Path is dynamic depending upon year month and expiry.   
  2.Connect to rds RawDataBase.     
  3.Create the table according to the date(tablename contains the date).    
  4.Before creating the table we drop it because if due to some error the table is not complete we need to delete it and again create and update the data.    
  5.Copy from the csv file to buffer and than put the data in the table  
  6.After putting all the data in the table update the rawinfo table which contents the tablename and date.   
  7.If for any reason the tablename exist in the rawinfo it will not insert same tablename again.   
  8.Commit and Close the connection   
 
## New functions used:   
 1.psycopg2.connect-It is used to build connection with sql.     
 2.Conn.autocommit=true-The AUTOCOMMIT connection attribute controls whether INSERT, ALTER, COPY and other data-manipulation statements are automatically 
                         committed after they complete.   
 3.cursore=conn.cursor:It is an object that is used to make the connection for executing SQL queries.   
 4.cursor.execute-To run the query   
 5.conn.commit-To save  
 6.conn.close-To close the connection.  