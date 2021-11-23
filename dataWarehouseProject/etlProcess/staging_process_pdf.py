import datetime
import os,sys
import traceback
import random
from numpy import NaN
import pandas as pd
import re
import tabula
import os
from dataWarehouseProject.database import DatabaseInterface
con = DatabaseInterface().getConnection(engine = True)
##### Extract Methods ########
rootDir = "C:/Users/manja/OneDrive/Documents/Advanced Database/clinical_trials_dump/15-09-2021"
# creating a pdf file object
fullPath=rootDir+'/'+'trial_res-1-22092021.pdf'
fullPathPDF =rootDir+'/pdftotxt/'
os.mkdir(fullPathPDF)
os.system(f'cmd /c "pdftotext -layout {fullPath} {fullPathPDF+"trial_res-1-22092021.txt"}"')



 





