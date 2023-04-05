import threading
import nzpy
import pandas as pd
from sqlalchemy import create_engine
import os
import time
import json

configfile = open("compare_tables.conf", "rb")
connection_options = json.load(configfile)
configfile.close()


class myThread (threading.Thread):
   def __init__(self, nz_db, nz_schema, nz_table, gp_db, gp_schema, gp_table):
      threading.Thread.__init__(self)
      self.nz_db = nz_db
      self.nz_schema = nz_schema
      self.nz_table = nz_table
      self.gp_db = gp_db
      self.gp_schema = gp_schema
      self.gp_table = gp_table


   def run(self):
       start_time = time.time()
       # Read Netezza table by chunks and Load to GP + Save to CSV file
       print(f'-------------- TABLE: {self.nz_schema}.{self.nz_table}--------------')

       for chunk_dataframe in pd.read_sql(
               "SELECT * FROM %s.%s" % (self.nz_schema, self.nz_table), con=nzpy.connect(
                   user=connection_options['netezza']['login'],
                   password=connection_options['netezza']['password'],
                   host=connection_options['netezza']['server'],
                   port=5480,
                   database=self.nz_db,
               ), chunksize=10000):
           print('==== Chunk ====')
           print(
               f"Got dataframe w/{len(chunk_dataframe)} rows"
           )

           # Lowercase column names
           chunk_dataframe.rename(columns={i: i.lower() for i in chunk_dataframe.columns}, inplace=True)

           # File name for table
           file_name = "./files_csv/" + self.gp_schema + "_" + self.gp_table + '.csv'

           # If data file for table does not exist => create it with headers = columns
           if not os.path.isfile(file_name):
               chunk_dataframe.to_csv(
                   file_name
                   , header=chunk_dataframe.columns, index=False)
           else:
               chunk_dataframe.to_csv(
                   file_name
                   , mode='a', header=False, index=False)

           print(
               f"Saved dataframe w/{len(chunk_dataframe)} rows to file {file_name}"
           )
           try:
               # Load to GP
               chunk_dataframe.to_sql(self.gp_table.lower(),
                                      schema=self.gp_schema.lower(), if_exists='append',
                                      con=create_engine(
                                          'postgresql+psycopg2://gpadmin@127.0.0.1/' + self.gp_db),
                                      index=False, method='multi')

               print(
                   f"Loaded dataframe to Greenplum w/{len(chunk_dataframe)} rows"
               )
               print("--- %s seconds ---" % (time.time() - start_time))
           except:
               try:
                   # Load to GP
                   chunk_dataframe.to_sql(self.gp_table.lower(),
                                          schema=self.gp_schema.lower(), if_exists='append',
                                          con=create_engine(
                                              'postgresql+psycopg2://gpadmin@127.0.0.1/' + self.gp_db),
                                          index=False)

                   print(
                       f"Loaded dataframe to Greenplum w/{len(chunk_dataframe)} rows"
                   )
                   print("--- %s seconds ---" % (time.time() - start_time))
               except:
                   print('Failed to load dataframe to Greenplum')
                   print("--- %s seconds ---" % (time.time() - start_time))



threads = []

df = pd.read_csv('inputs/data_inputs.csv')
for index, row in df.iterrows():
    print(row)
    # Create new threads
    thread_i =  myThread(row['source_database'],row['source_schema'],row['source_table'],row['target_database'],row['target_schema'],row['target_table'], )

    # Start new Threads
    thread_i.start()

    # Add threads to thread list
    threads.append(thread_i)


# Wait for all threads to complete
for t in threads:
    t.join()

print("Exiting Main Thread")