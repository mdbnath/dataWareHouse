import datetime
import os,sys
import traceback
from numpy import NaN
import pandas as pd
from dataWarehouseProject.database import DatabaseInterface

# get all files in a list from the directory.
rootDir = "C:/Users/manja/OneDrive/Documents/Advanced Database/clinical_trials_dump/"
#fullPath=path+'/'+f

def read_content(f,fileName):
    ext =f.split('.')[1]
    ### Handle files that are text type .txt format
    if ext  == "txt" :        
        with open(f, 'r',encoding='utf-8') as fh:
            contents = fh.read()
        df = pd.DataFrame(contents.split('Summary\n'), columns= ['content'])
        df['file_Name']=fileName
        df['run_date']=datetime.datetime.now()
        df.to_sql('load_txt', DatabaseInterface().getConnection(engine = True), index = False, if_exists='append',schema='staging')
    return df

subDirectories = os.listdir(rootDir)
DatabaseInterface().drop_table('staging.load_txt')
for subDir in subDirectories:
    files= os.listdir(os.path.join(rootDir, subDir))
    print(files)
    for file in files:
        try:
           read_content(os.path.join(rootDir, subDir,file),file)
           print("Successfully ran" +file)
        except:
           print(f'Failed to load:' + os.path.join(rootDir, subDir,file))
           print(traceback.format_exc())
       
            