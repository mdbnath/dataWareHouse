import datetime
import os,sys
import traceback
import random
from numpy import NaN
import pandas as pd
import re
import tabula
import os
from dataWarehouseProject import dbInstance
con = dbInstance.getConnection(engine = True)
    

def create_staging_endpoint(df,schema_name):
    df['EudraCT_number'] = df['content'].str.extract(pat='EudraCT number *(.*)')
    df.dropna(subset = ['EudraCT_number'], inplace= True)
    df['Endpoint_title'] = df['content'].str.extract(pat='End point title *(.*)') 
    df['Endpoint_type']= df['content'].str.extract(pat='End point type  *(.*)') 
    
    return(df)


def run_staging_process(directory):
    schema_name =f'staging_{directory.replace("-", "_")}'
    print(schema_name)
    ## Read from Ingestion Table
    df = pd.read_sql_table('load_txt',con,schema=schema_name)
    #build all the staging tables
   


 





