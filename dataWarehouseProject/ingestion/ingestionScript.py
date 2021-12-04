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
    ext =f.split('.')[1]
    ### Handle files that are text type .txt format
    if 'pdftotxt' not in f:        
        with open(f, 'r',encoding='utf-8') as fh:
            contents = fh.read()
        df = pd.DataFrame(contents.split('Summary\n'), columns= ['content'])
        df['file_Name']=fileName
        df['run_date']=datetime.datetime.now()        
        df.to_sql('load_txt', con, index = False, if_exists='append',schema=schema_name)
    else:     
        with open(f, 'r') as fh:
            contents = fh.read()
        df = pd.DataFrame(contents.split('Summary\n'), columns= ['content'])
        df['file_Name']=fileName
        df['run_date']=datetime.datetime.now()        
        df.to_sql('load_pdf', con, index = False, if_exists='append',schema=schema_name)
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
                    read_text_content(os.path.join(rootDir, subDir,file),file,schema_name)
                    print("Successfully ran" +file)
            except:
                    print(f'Failed to load:' + os.path.join(rootDir, subDir,file))
                    print(traceback.format_exc()) 
        staging_process_txt.run_staging_process(subDir) 
    
############################# PDF PARSING ###############################
def process_pdf_convert():

    ##### Extract Methods ########
    rootDirPdf = "C:/Users/manja/OneDrive/Documents/Advanced Database/clinical_trials_dump/"
    subDirectories = os.listdir(rootDirPdf)
    print(subDirectories)
    subDirectories.sort()
    print(subDirectories)
    for subDir in subDirectories:
        # creating a pdf file object
        files= os.listdir(os.path.join(rootDirPdf, subDir))
        files = [f for f in files if f.endswith('pdf')]
        ##fullPath=subDir+'/'+'trial_res-1-22092021.pdf'
        fullPathTXT =rootDirPdf+subDir+'/pdftotxt/'
        fullPathPDF =rootDirPdf+subDir+'/'
        print(fullPathPDF)
        if os.path.exists(fullPathTXT):
            shutil.rmtree(fullPathTXT)
        os.makedirs(fullPathTXT)
        for file in files:
            print("############" + fullPathPDF+file+"File loop is this")
            txtfile=file.split(".")[0]+".txt"
            print(f'''pdftotext -layout "{fullPathPDF+file}" "{fullPathTXT+txtfile}"''')
            os.system(f'''pdftotext -layout "{fullPathPDF+file}" "{fullPathTXT+txtfile}"''')
        
def process_pdf_staging():
    rootDirPdf = "C:/Users/manja/OneDrive/Documents/Advanced Database/clinical_trials_dump"
    subDirectories = os.listdir(rootDirPdf)
    subDirectories.sort()
    print(subDirectories)
    for subDir in subDirectories:
        schema_name =f'staging_{subDir.replace("-", "_")}'
        dbInstance.drop_table(f'"{schema_name}"."load_pdf"')
    # creating a pdf file object
        try:
            files= os.listdir(os.path.join(rootDirPdf,subDir,"pdftotxt"))
        except FileNotFoundError:
            print('Could not find folder', os.path.join(rootDirPdf,subDir,"pdftotxt"))
            continue  
        
        print(files)
        for file in files:
            try:
                read_text_content(os.path.join(rootDirPdf, subDir,"pdftotxt",file),file,schema_name)
                print("Successfully ran" +file)
            except:
                print(f'Failed to load:' + os.path.join(rootDir, subDir,"pdftotxt",file))
                print(traceback.format_exc()) 
        staging_process_pdf.run_staging_process(subDir) 
        
   
   

    print('############################################ STAGING COMPLETED ############################### ')
    ##dimension.run_dim_tables(subDir) 
    
process_pdf_convert()
process_pdf_staging()
process_txt_staging(rootDir)

