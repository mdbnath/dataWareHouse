import os,sys
from numpy import NaN
import pandas as pd
from database import DatabaseInterface

# get all files in a list from the directory.
rootDir = "C:/Users/manja/OneDrive/Documents/Advanced Database/clinical_trials_dump/"
#fullPath=path+'/'+f

def read_content(f,fileName):
    ext =f.split('.')[1]
    ### Handle files that are text type .txt format
    if ext  == "txt" :    
        df = pd.read_csv(f, sep="\x01", names=['content'],skiprows=11)
        df['key'] = df['content'].str.split(' ').apply(lambda x: x[0])
        # the left over line content after Key need to join as we want the whole line to again split       
        df['textValue'] = df['content'].str.split(' ').apply(lambda x:' '.join(x[1:]))
        # Get the values after : 
        df['keyName'] = df['textValue'].str.split(':').apply(lambda x: x[0])
        df['keyValue'] = df['textValue'].str.split(':').apply(lambda x: x[1] if len(x)>1 else None)
        df['fileName']=fileName
        #print(df[df['keyName'] == 'Country']) 
        df.to_sql('stage_txt', DatabaseInterface().getConnection(engine = True), index = False, if_exists='append')
        return df

def create_dims(df):
    #Prepare Sponsor Info DIM table
    sponsordf = df.loc[df['key'].str.startswith('B', na = False)]
    sponsordf.to_sql('DIM_SPONSOR_INFO', DatabaseInterface().getConnection(engine = True), index = False, if_exists='append')
    print(sponsordf)
    return

subDirectories = os.listdir(rootDir)
DatabaseInterface().drop_table('stage_txt')
for subDir in subDirectories:
    files= os.listdir(os.path.join(rootDir, subDir))
    print(files)
    for file in files:
        try:
           stage= read_content(os.path.join(rootDir, subDir,file),file)
           create_dims(stage)
        except:
            print(f'Failed to load:' + os.path.join(rootDir, subDir,file))
       
            