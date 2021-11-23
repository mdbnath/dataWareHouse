# Dimension Table
import datetime
from genericpath import exists
import os,sys
import traceback
from numpy import NaN
import pandas as pd
from dataWarehouseProject import dbInstance
con = dbInstance.getConnection(engine = True)

def run_dim_tables(subDir):
    try:
        schema_name =f'staging_{subDir.replace("-", "_")}'        
        dbInstance.run_procedure('public.insert_dim_data',schema_name) 
        print("Successfully ran" +schema_name)
    except:
        print(f'Failed to load:' + os.path.join(schema_name))
        print(traceback.format_exc()) 

run_dim_tables('15_09_2021')
