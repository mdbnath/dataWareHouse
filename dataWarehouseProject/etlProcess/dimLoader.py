import datetime
import os,sys
import traceback
from numpy import NaN
import pandas as pd
from dataWarehouseProject.database import DatabaseInterface


def create_staging_sponsor(df):   
    df['sponsor_name'] = df['content'].str.extract(pat='B.1.1 Name of Sponsor: (.*)')
    df['sponsor_country'] = df['content'].str.extract(pat='B.1.3.4	Country: (.*)')
    df['sponsor_status'] = df['content'].str.extract(pat='B.3.1 and B.3.2	Status: (.*)')
    df['sponsor_organisation'] = df['content'].str.extract(pat='B.5.1 Name of organisation: (.*)')
    df['sponsor_streetName'] = df['content'].str.extract(pat='B.5.3.1 Street Address: (.*)')
    df['sponsor_city'] = df['content'].str.extract(pat='B.5.3.2 Town/ city: (.*)')
    df['sponsor_postalCode'] = df['content'].str.extract(pat='B.5.3.3 Post code: (.*)')
    df.to_sql('DIM_SPONSOR', DatabaseInterface().getConnection(engine = True), index = True, if_exists='append')
    return(df)

def create_dim_trial(df):   

    df['trial_version'] = df['content'].str.extract(pat='E.1.2 Version: (.*)')
    df['trial_investigation'] = df['content'].str.extract(pat='E.1.1 Medical condition(s) being investigated: (.*)')
    df['trial_classificationCode'] = df['content'].str.extract(pat='E.1.2 Classification code: (.*)')
    df['trial_authority_decision'] = df['content'].str.extract(pat='N. Competent Authority Decision:  (.*)')
    df['trial_controlled'] = df['content'].str.extract(pat='E.8.1 Controlled: (.*)')
    df['trail_randomised'] = df['content'].str.extract(pat='E.8.1.1 Randomised: (.*)')
    df['trial_arms'] = df['content'].str.extract(pat='BE.8.2.4 Number of treatment arms in the trial: (.*)')
    df.to_sql('DIM_TRIAL', DatabaseInterface().getConnection(engine = True), index = True, if_exists='append')
    return(df)

def create_dim_trial_ageRange(df):   
    df['trial_version'] = df['content'].str.extract(pat='E.1.2 Version: (.*)')
    df['trial_investigation'] = df['content'].str.extract(pat='E.1.1 Medical condition(s) being investigated: (.*)')
    df['trial_classificationCode'] = df['content'].str.extract(pat='E.1.2 Classification code: (.*)')
    df['trial_authority_decision'] = df['content'].str.extract(pat='N. Competent Authority Decision:  (.*)')
    df['trial_controlled'] = df['content'].str.extract(pat='E.8.1 Controlled: (.*)')
    df['trail_randomised'] = df['content'].str.extract(pat='E.8.1.1 Randomised: (.*)')
    df['trial_arms'] = df['content'].str.extract(pat='BE.8.2.4 Number of treatment arms in the trial: (.*)')
    df.to_sql('DIM_TRIAL', DatabaseInterface().getConnection(engine = True), index = True, if_exists='append')
    return(df)

def create_dim_imp(df):   
    df['imp_role'] = df['content'].str.extract(pat='D.1.2 and D.1.3 IMP Role: (.*)')
    df['imp_productCode'] = df['content'].str.extract(pat='D.3.2 Product code: (.*)')
    df['imp_concentrationNumber'] = df['content'].str.extract(pat='D.3.10.3 Concentration number: (.*)')
    df['imp_concentrationUnit'] = df['content'].str.extract(pat='D.3.10.1 Concentration unit: (.*)')
    df['imp_casnumber'] = df['content'].str.extract(pat='D.3.9.1 CAS number: (.*)')
    df.to_sql('DIM_IMP_IDENTIFICATION', DatabaseInterface().getConnection(engine = True), index = True, if_exists='append')
    return(df)

 
## Read from Ingestion Table
df = pd.read_sql_table('stage1_txt',DatabaseInterface().getConnection(engine = True))
#build all the DIM tables
create_dim_sponsor(df)
create_dim_trial(df)
create_dim_imp(df)
            