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
##### Extract Methods ########
rootDir = "C:/Users/manja/OneDrive/Documents/Advanced Database/clinical_trials_dump/15-09-2021"
# creating a pdf file object
fullPath=rootDir+'/'+'trial_res-1-22092021.pdf'
fullPathPDF =rootDir+'/pdftotxt/'
if os.path.exists(fullPathPDF):
    ##os.chmod(fullPathPDF, 0o777)
    os.rmdir(fullPathPDF)
os.mkdir(fullPathPDF)
os.system(f'cmd /c "pdftotext -layout {fullPath} {fullPathPDF+"trial_res-1-22092021.txt"}"')


def create_staging_endpoint(df,schema_name):
df['uid'] = df['content'].str.extract(pat='Link: https://www.clinicaltrialsregister.eu/ctr-search/trial/(.*)')
df.dropna(subset = ['uid'], inplace= True)
df['uid'] = df['uid'].apply(lambda x: x.replace('-', '')[0:-1])
df['uid'] = df['uid'].apply(lambda x: x.replace('/', '_'))
df['summary_EudraCT_Number'] = df['content'].str.extract(pat='EudraCT Number: (.*)')
df['summary_Protocol_Number'] = df['content'].str.extract(pat='Sponsor''s Protocol Code Number: (.*)')
##df['summary_NCA'] = df['content'].str.extract(pat='National Competent Authority: (.*)')
df['summary_trial_type'] = df['content'].str.extract(pat='Clinical Trial Type: (.*)')   
df['summary_trial_status'] = df['content'].str.extract(pat='Trial Status: (.*)')
df=df[['uid','summary_EudraCT_Number','summary_Protocol_Number','summary_trial_type','summary_trial_status']]
df=df.drop_duplicates()
dbInstance.drop_table(f'"{schema_name}"."SUMMARY_INFORMATION"')
df.to_sql('SUMMARY_INFORMATION', con, index = True, if_exists='append',schema=schema_name)

return(df)




 





