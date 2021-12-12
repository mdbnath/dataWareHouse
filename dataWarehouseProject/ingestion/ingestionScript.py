import datetime
from genericpath import exists
import os,sys
import traceback
from numpy import NaN
import pandas as pd
import random
import re
import tabula
import shutil
import os
from dataWarehouseProject import dbInstance
from dataWarehouseProject.etlProcess import staging_process_txt
from dataWarehouseProject.etlProcess import staging_process_pdf
from dataWarehouseProject.etlProcess import dimension

con = dbInstance.getConnection(engine = True)
# get all files in a list from the directory.
rootDir = "C:/Users/manja/OneDrive/Documents/Advanced Database/clinical_trials_dump/"
#fullPath=path+'/'+f

def read_text_content(f,fileName,schema_name):
        ### Handle files that are text type .txt format
    with open(f, 'r',encoding='utf-8') as fh:
        contents = fh.read()
    df = pd.DataFrame(contents.split('Summary\n'), columns= ['content'])
    df['file_Name']=fileName
    df['run_date']=datetime.datetime.now()        
    df.to_sql('load_txt', con, index = False, if_exists='append',schema=schema_name)
    
    return df

############################# TXT PARSING ###############################
def process_txt_staging(rootDir):
    subDirectories = os.listdir(rootDir)
    subDirectories.sort()
    print(subDirectories)
    for subDir in subDirectories:
        schema_name =f'staging_{subDir.replace("-", "_")}'
        dbInstance.drop_table(f'"{schema_name}"."load_txt"')
        files= os.listdir(os.path.join(rootDir, subDir))
        for file in files:
            try:
                    print("++++++++++++++++++++++++++++",os.path.join(rootDir, subDir,file))
                    read_text_content(os.path.join(rootDir, subDir,file),file,schema_name)
                    print("Successfully ran" +file)
            except:
                    print(f'Failed to load:' + os.path.join(rootDir, subDir,file))
                    print(traceback.format_exc()) 
        staging_process_txt.run_staging_process(subDir) 
    
############################# PDF PARSING ###############################
def process_pdf_staging(rootDirPdf):
    subDirectories = os.listdir(rootDirPdf)
    subDirectories.sort()
    print(subDirectories)
    for subDir in subDirectories:
        schema_name =f'staging_{subDir.replace("-", "_")}'
        ##dbInstance.drop_table(f'"{schema_name}"."load_pdf"')
    # creating a pdf file object
        try:
            staging_process_pdf.parse_directory(rootDirPdf, subDir)
        
        except FileNotFoundError:
            print('Could not find folder', os.path.join(rootDirPdf,subDir))
            continue  
    print('############################################ STAGING COMPLETED ############################### ')
    ##dimension.run_dim_tables(subDir) 
    
process_pdf_staging(rootDir)

##process_txt_staging(rootDir)

