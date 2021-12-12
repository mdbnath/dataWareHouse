import datetime
import os,sys
import traceback
import random
from numpy import NaN
import pandas as pd
import re
from dataWarehouseProject import dbInstance
con = dbInstance.getConnection(engine = True)
##### Extract Methods ########
def extract_sources_sponsor(test_string):
    elements = [line for line in test_string.split("\n") if line.startswith('B.4.')]
    output = []
    for element_number in range(0, len(elements), 2):
        name = elements[element_number].split(': ')[-1]
        country = elements[element_number + 1].split(': ')[-1]
        output.append((name, country))
      
    print(output)

def extract_sources_trial(test_string):
    elements = [line for line in test_string.split("\n") if line.startswith('E.1.2')]
    output = []
    print(elements)
    for element_number in range(0, len(elements), 6):
        disease = elements[element_number].split(': ')[-1]
        version = elements[element_number + 1].split(': ')[-1]
        level = elements[element_number + 2].split(': ')[-1]
        classification_code = elements[element_number + 3].split(': ')[-1]
        term = elements[element_number + 4].split(': ')[-1]
        organ_class = elements[element_number + 5].split(': ')[-1]
        output.append((disease, version,level,classification_code,term,organ_class))
        return;
   
def extract_sources_trial_duration(test_string):
    elements = [line for line in test_string.split("\n") if line.startswith('E.8.9.1')]
    output = []
    for element_number in range(0, len(elements), 3):
        years = elements[element_number].split(': ')[-1]
        month = elements[element_number + 1].split(': ')[-1]
        days = elements[element_number + 2].split(': ')[-1]
        output.append((years, month,days))

######## Create Staging Tables #########
def create_staging_summary(df,schema_name,directory):   
    df['uid'] = df['content'].str.extract(pat='Link: https://www.clinicaltrialsregister.eu/ctr-search/trial/(.*)')
    df.dropna(subset = ['uid'], inplace= True)
    df['uid'] = df['uid'].apply(lambda x: x.replace('-', '')[0:-1])
    df['uid'] = df['uid'].apply(lambda x: x.replace('/', '_'))
    df['summary_EudraCT_Number'] = df['content'].str.extract(pat='EudraCT Number: (.*)')
    df['summary_Protocol_Number'] = df['content'].str.extract(pat='''Sponsor's Protocol Code Number: (.*)''')
    ##df['summary_NCA'] = df['content'].str.extract(pat='National Competent Authority: (.*)')
    df['summary_trial_type'] = df['content'].str.extract(pat='Clinical Trial Type: (.*)')   
    df['summary_trial_status'] = df['content'].str.extract(pat='Trial Status: (.*)')
    df['valid_from'] = pd.to_datetime(directory,errors='ignore',format='%d-%m-%Y')
    df=df[['uid','summary_EudraCT_Number','summary_Protocol_Number','summary_trial_type','summary_trial_status','valid_from','file_Name']]
    df=df.drop_duplicates()
    dbInstance.drop_table(f'"{schema_name}"."SUMMARY_INFORMATION"')
    df.columns = df.columns.str.lower()
    df.to_sql('SUMMARY_INFORMATION', con, index = True, if_exists='append',schema=schema_name)
    return(df)

def create_staging_protocol(df,schema_name,directory):   
    df['uid'] = df['content'].str.extract(pat='Link: https://www.clinicaltrialsregister.eu/ctr-search/trial/(.*)')
    df.dropna(subset = ['uid'], inplace= True)
    df['uid'] = df['uid'].apply(lambda x: x.replace('-', '')[0:-1])
    df['uid'] = df['uid'].apply(lambda x: x.replace('/', '_'))
    df['summary_EudraCT_Number'] = df['content'].str.extract(pat='EudraCT Number: (.*)')
    df['protocol_member_state'] = df['content'].str.extract(pat='A.1 Member State Concerned: (.*)')
    df['protocol_EudraCT_Number'] = df['content'].str.extract(pat='A.2 EudraCT number: (.*)')
    df['protocol_trial_fullTitle'] = df['content'].str.extract(pat='A.3 Full title of the trial: (.*)')
    df['protocol_code_number'] = df['content'].str.extract(pat='A.4.1 Sponsor''s protocol code number: (.*)')
    ##df['protocol_ISRCTN_number'] = df['content'].str.extract(pat='A.5.1 ISRCTN \(International Standard Randomised Controlled Trial\) number: (.*)')
    ##df['protocol_UTRN_number'] = df['content'].str.extract(pat='A.5.3 WHO Universal Trial Reference Number \(UTRN\): (.*)')
    ##df['protocol_Paediatric_investigation'] = df['content'].str.extract(pat='A.7 Trial is part of a Paediatric Investigation Plan: (.*)')
    ##df['protocol_EMA_decision_number'] = df['content'].str.extract(pat='A.8 EMA Decision number of Paediatric Investigation Plan: (.*)')
    df['valid_from'] = pd.to_datetime(directory,errors='ignore',format='%d-%m-%Y')
    df=df[['uid','summary_EudraCT_Number','protocol_member_state','protocol_EudraCT_Number',
    'protocol_trial_fullTitle','protocol_code_number','valid_from','file_Name']]
    df=df.drop_duplicates()
    dbInstance.drop_table(f'"{schema_name}"."PROTOCOL_INFORMATION"')
    df.columns = df.columns.str.lower()
    df.to_sql('PROTOCOL_INFORMATION', con, index = True, if_exists='append',schema=schema_name)
    return(df)

def create_staging_sponsor(df,schema_name,directory): 
    df['uid'] = df['content'].str.extract(pat='Link: https://www.clinicaltrialsregister.eu/ctr-search/trial/(.*)')
    df.dropna(subset = ['uid'], inplace= True)
    df['uid'] = df['uid'].apply(lambda x: x.replace('-', '')[0:-1])
    df['uid'] = df['uid'].apply(lambda x: x.replace('/', '_'))
    df['summary_EudraCT_Number'] = df['content'].str.extract(pat='EudraCT Number: (.*)')  
    df['sponsor_name'] = df['content'].str.extract(pat='B.1.1 Name of Sponsor: (.*)')
    df['sponsor_country'] = df['content'].str.extract(pat='B.1.3.4	Country: (.*)')
    df['sponsor_status'] = df['content'].str.extract(pat='B.3.1 and B.3.2	Status: (.*)')
    df['sponsor_source'] = df['content'].apply(extract_sources_sponsor)
    df['sponsor_organisation'] = df['content'].str.extract(pat='B.5.1 Name of organisation: (.*)')
    df['sponsor_streetName'] = df['content'].str.extract(pat='B.5.3.1 Street Address: (.*)')
    df['sponsor_city'] = df['content'].str.extract(pat='B.5.3.2 Town/ city: (.*)')
    df['sponsor_postalCode'] = df['content'].str.extract(pat='B.5.3.3 Post code: (.*)')
    df['sponsor_email'] = df['content'].str.extract(pat='B.5.6 E-mail: (.*)')    
    df['valid_from'] = pd.to_datetime(directory,errors='ignore',format='%d-%m-%Y')
    print(df.columns)
    df=df[['uid','summary_EudraCT_Number','sponsor_name','sponsor_country','sponsor_status','sponsor_source',
    'sponsor_organisation','sponsor_streetName','sponsor_city','sponsor_postalCode','sponsor_email','valid_from','file_Name']]
    df=df.drop_duplicates()
    dbInstance.drop_table(f'"{schema_name}"."SPONSOR_INFORMATION"')
    df.columns = df.columns.str.lower()
    df.to_sql('SPONSOR_INFORMATION', con, index = True, if_exists='append',schema=schema_name)
    return(df)

def create_staging_imp(df,schema_name,directory):  
    df['uid'] = df['content'].str.extract(pat='Link: https://www.clinicaltrialsregister.eu/ctr-search/trial/(.*)')
    df.dropna(subset = ['uid'], inplace= True)
    df['uid'] = df['uid'].apply(lambda x: x.replace('-', '')[0:-1])
    df['uid'] = df['uid'].apply(lambda x: x.replace('/', '_'))  
    df['summary_EudraCT_Number'] = df['content'].str.extract(pat='EudraCT Number: (.*)')
    df['imp_role'] = df['content'].str.extract(pat='D.1.2 and D.1.3 IMP Role: (.*)')
    df['imp_marketing_authorisation'] = df['content'].str.extract(pat='D.2.1 IMP to be used in the trial has a marketing authorisation: (.*)')
    df['imp_Trade_name'] = df['content'].str.extract(pat='D.2.1.1.1 Trade name: (.*)')
    df['imp_marketing_authorisation_holder'] = df['content'].str.extract(pat='D.2.1.1.2 Name of the Marketing Authorisation holder: (.*)')
    df['imp_orphan_drug'] = df['content'].str.extract(pat='D.2.5 The IMP has been designated in this indication as an orphan drug in the Community: (.*)')
    df['imp_orphan_drug_number'] = df['content'].str.extract(pat='D.2.5.1 Orphan drug designation number: (.*)')   
    df['imp_productName'] = df['content'].str.extract(pat='D.3.1 Product name: (.*)')
    df['imp_productCode'] = df['content'].str.extract(pat='D.3.2 Product code: (.*)')
    ##df['imp_pharmaceutical_form'] = df['content'].str.extract(pat='D.3.4 Pharmaceutical form: (.*)')
    ##df['imp_paediatric_formulation'] = df['content'].str.extract(pat='D.3.4.1 Specific paediatric formulation: (.*)')
    ##df['imp_admin_routes'] = df['content'].str.extract(pat='D.3.7 Routes of administration for this IMP: (.*)')
    ##df['imp_INN'] = df['content'].str.extract(pat='D.3.8 INN - Proposed INN: (.*)')
    ##df['imp_casnumber'] = df['content'].str.extract(pat='D.3.9.1 CAS number: (.*)')
    ##df['imp_substance_code'] = df['content'].str.extract(pat='D.3.9.4 EV Substance Code: (.*)')
    ##df['imp_concentrationType'] = df['content'].str.extract(pat='D.3.10.2 Concentration type: (.*)')
    ##df['imp_concentrationNumber'] = df['content'].str.extract(pat='D.3.10.3 Concentration number: (.*)')
    ##df['imp_concentrationUnit'] = df['content'].str.extract(pat='D.3.10.1 Concentration unit: (.*)')
    ##df['imp_chemical_origin'] = df['content'].str.extract(pat='D.3.11.1 Active substance of chemical origin: (.*)')
    ##df['imp_biotechnological_origin'] = df['content'].str.extract(pat='D.3.11.2 Active substance of biological/ biotechnological origin \(other than Advanced Therapy IMP \(ATIMP\):  (.*)')
    ##df['imp_ATIMP'] = df['content'].str.extract(pat='D.3.11.3 Advanced Therapy IMP \(ATIMP\): (.*)')
    ##df['imp_somatic_cell_therapy'] = df['content'].str.extract(pat='D.3.11.3.1 Somatic cell therapy medicinal product: (.*)')
    ##df['imp_gene_therapy'] = df['content'].str.extract(pat='D.3.11.3.2 Gene therapy medical product: (.*)')
    ##df['imp_tissue_engineered'] = df['content'].str.extract(pat='D.3.11.3.3 Tissue Engineered Product: (.*)')
    ##df['imp_Combination_ATIMP'] = df['content'].str.extract(pat='D.3.11.3.4 Combination ATIMP \(i.e. one involving a medical device\): (.*)')
    ##df['imp_CAT_classification'] = df['content'].str.extract(pat='D.3.11.3.5 Committee on Advanced therapies \(CAT\) has issued a classification for this product: (.*)')
    ##df['imp_advanced_Therapy'] = df['content'].str.extract(pat='D.3.11.4 Combination product that includes a device, but does not involve an Advanced Therapy: (.*)')
    ##df['imp_placebo_used'] = df['content'].str.extract(pat='D.8.1 Is a Placebo used in this Trial? (.*)')   
    df['valid_from'] = pd.to_datetime(directory,errors='ignore',format='%d-%m-%Y')
    df=df[['uid','summary_EudraCT_Number','imp_role','imp_marketing_authorisation',
    'imp_Trade_name','imp_marketing_authorisation_holder','imp_orphan_drug','imp_orphan_drug_number',
    'imp_productName','imp_productCode','valid_from','file_Name']]
    df=df.drop_duplicates()    
    dbInstance.drop_table(f'"{schema_name}"."IMP_IDENTIFICATION"')
    df.columns = df.columns.str.lower()
    df.to_sql('IMP_IDENTIFICATION', con, index = True, if_exists='append',schema=schema_name)
    return(df)

def create_staging_trial(df,schema_name,directory):   
    df['uid'] = df['content'].str.extract(pat='Link: https://www.clinicaltrialsregister.eu/ctr-search/trial/(.*)')
    df.dropna(subset = ['uid'], inplace= True)
    df['uid'] = df['uid'].apply(lambda x: x.replace('-', '')[0:-1])
    df['uid'] = df['uid'].apply(lambda x: x.replace('/', '_'))
    df['summary_EudraCT_Number'] = df['content'].str.extract(pat='EudraCT Number: (.*)')
    df['trial_investigation'] = df['content'].str.extract(pat='E.1.1 Medical condition\(s\) being investigated: (.*)')
    df['trial_language'] = df['content'].str.extract(pat='E.1.1.1 Medical condition in easily understood language: (.*)')
    df['trial_Therapeutic_area'] = df['content'].str.extract(pat='E.1.1.2 Therapeutic area: (.*)')
    df['trial_MedDRA_classification'] = df['content'].apply(extract_sources_trial)
    df['trial_rare_disease'] = df['content'].str.extract(pat='E.1.3 Condition being studied is a rare disease: (.*)')
    df['trial_main_objective'] = df['content'].str.extract(pat='E.2.1 Main objective of the trial: (.*)')
    df['trial_secondary_objective'] = df['content'].str.extract(pat='E.2.2 Secondary objectives of the trial: (.*)')
    df['trial_subStudy'] = df['content'].str.extract(pat='E.2.3 Trial contains a sub-study: (.*)')
    df['trial_inclusion_criteria'] = df['content'].str.extract(pat='E.3 Principal inclusion criteria: (.*)')
    df['trial_exclusion_criteria'] = df['content'].str.extract(pat='E.4 Principal exclusion criteria:  (.*)')
    df['trial_primary_endpoint'] = df['content'].str.extract(pat='E.5.1 Primary end point\(s\): (.*)')
    df['trial_primary_time'] = df['content'].str.extract(pat='E.5.1.1 Timepoint\(s\) of evaluation of this end point: (.*)')
    df['trial_secondary_endpoint'] = df['content'].str.extract(pat='E.5.2 Secondary end point\(s\): (.*)')
    df['trial_secondary_time'] = df['content'].str.extract(pat='E.5.2.1 Timepoint\(s\) of evaluation of this end point: (.*)')
    df['trial_phaseI'] = df['content'].str.extract(pat='E.7.1 Human pharmacology \(Phase I\): (.*)')
    df['trial_phaseII'] = df['content'].str.extract(pat='E.7.2 Therapeutic exploratory \(Phase II\): (.*)')
    df['trial_phaseIII'] = df['content'].str.extract(pat='E.7.3 Therapeutic confirmatory \(Phase III\): (.*)')
    df['trial_phaseIV'] = df['content'].str.extract(pat='E.7.4 Therapeutic use \(Phase IV\): (.*)')      
    df['trial_controlled'] = df['content'].str.extract(pat='E.8.1 Controlled: (.*)')
    df['trail_randomised'] = df['content'].str.extract(pat='E.8.1.1 Randomised: (.*)')
    df['trial_single_blind'] = df['content'].str.extract(pat='E.8.1.3 Single blind: (.*)')
    df['trail_double_blind'] = df['content'].str.extract(pat='E.8.1.4 Double blind: (.*)')
    df['trial_arms'] = df['content'].str.extract(pat='E.8.2.4 Number of treatment arms in the trial: (.*)')
    df['trial_member_state'] = df['content'].apply(extract_sources_trial_duration)
    df['trial_status'] = df['content'].str.extract(pat='P. End of Trial Status: (.*)')    
    df['valid_from'] = pd.to_datetime(directory,errors='ignore',format='%d-%m-%Y')
    df=df[['uid','summary_EudraCT_Number','trial_investigation','trial_language','trial_Therapeutic_area',
    'trial_MedDRA_classification','trial_rare_disease',
    'trial_main_objective','trial_secondary_objective','trial_subStudy','trial_inclusion_criteria',
    'trial_exclusion_criteria','trial_primary_endpoint','trial_primary_time','trial_secondary_endpoint','trial_secondary_time','trial_controlled',
	'trail_randomised','trial_single_blind','trail_double_blind','trial_arms','trial_member_state','trial_status','trial_phaseI','trial_phaseII',
    'trial_phaseIII','trial_phaseIV','valid_from','file_Name']]
    df=df.drop_duplicates()
    dbInstance.drop_table(f'"{schema_name}"."TRIAL_INFORMATION"')
    df.columns = df.columns.str.lower()
    df.to_sql('TRIAL_INFORMATION', con, index = True, if_exists='append',schema=schema_name)
    return(df)

def create_trial_review(df: pd.DataFrame,schema_name,directory):
    df['uid'] = df['content'].str.extract(pat='Link: https://www.clinicaltrialsregister.eu/ctr-search/trial/(.*)')
    df.dropna(subset = ['uid'], inplace= True)
    df['uid'] = df['uid'].apply(lambda x: x.replace('-', '')[0:-1])
    df['uid'] = df['uid'].apply(lambda x: x.replace('/', '_'))
    df['summary_EudraCT_Number'] = df['content'].str.extract(pat='EudraCT Number: (.*)')   
    df['trial_review_Authority'] = df['content'].str.extract(pat='N. Competent Authority Decision: (.*)')
    print('############################################',df['trial_review_Authority'])
    df['trial_review_ethics_Committee'] = df['content'].str.extract(pat='N. Ethics Committee Opinion of the trial application: (.*)')
    df['trial_review_ethics_committeReason'] = df['content'].str.extract(pat='N. Ethics Committee Opinion: Reason\(s\) for unfavourable opinion: (.*)')
    df=df[['uid','summary_EudraCT_Number','trial_review_Authority','trial_review_ethics_Committee','trial_review_ethics_committeReason','file_Name']]
    df=df.drop_duplicates()
    dbInstance.drop_table(f'"{schema_name}"."TRIAL_REVIEW_INFO"')
    df.columns = df.columns.str.lower()
    df.to_sql('TRIAL_REVIEW_INFO', con, index = True, if_exists='append',schema=schema_name)
    return(df)
 
def create_time_dimension(df:pd.DataFrame,schema_name,directory):
    df['uid'] = df['content'].str.extract(pat='Link: https://www.clinicaltrialsregister.eu/ctr-search/trial/(.*)')
    df.dropna(subset = ['uid'], inplace= True)
    df['uid'] = df['uid'].apply(lambda x: x.replace('-', '')[0:-1])
    df['uid'] = df['uid'].apply(lambda x: x.replace('/', '_'))
    df['summary_EudraCT_Number'] = df['content'].str.extract(pat='EudraCT Number: (.*)')  
    df.dropna(subset=['summary_EudraCT_Number'], inplace=True)
    df['summary_trial_dateEntered']= df['content'].str.extract(pat='Date on which this record was first entered in the EudraCT database: (.*)')
    df['summary_trial_dateEntered'] = pd.to_datetime(df['summary_trial_dateEntered'].apply(str),errors='ignore',format='%Y-%m-%d')
    print(df['summary_trial_dateEntered'])
    print(df.dtypes)
    df['trial_review_authority_date'] = df['content'].str.extract(pat='N. Date of Ethics Committee Opinion: (.*)')
    df['trial_review_authority_date'] = pd.to_datetime(df['trial_review_authority_date'].apply(str),errors='ignore',format='%Y-%m-%d')
    df['trail_review_ethics_date'] = df['content'].str.extract(pat='N. Date of Ethics Committee Opinion: (.*)')
    df['trail_review_ethics_date'] = pd.to_datetime(df['trail_review_ethics_date'].apply(str),errors='ignore',format='%Y-%m-%d')
    df['valid_from'] = pd.to_datetime(directory,errors='ignore',format='%d-%m-%Y')
    df=df[['uid','summary_EudraCT_Number','summary_trial_dateEntered','trial_review_authority_date','trail_review_ethics_date','valid_from','file_Name']]
    df=df.drop_duplicates()
    dbInstance.drop_table(f'"{schema_name}"."TIME_DIMENSION"')
    df.columns = df.columns.str.lower()
    df.to_sql('TIME_DIMENSION', con, index = True, if_exists='append',schema=schema_name)
    return(df)

def create_trial_subject(df,schema_name,directory): 
    df['uid'] = df['content'].str.extract(pat='Link: https://www.clinicaltrialsregister.eu/ctr-search/trial/(.*)')
    df.dropna(subset = ['uid'], inplace= True)
    df['uid'] = df['uid'].apply(lambda x: x.replace('-', '')[0:-1])
    df['uid'] = df['uid'].apply(lambda x: x.replace('/', '_'))
    df['summary_EudraCT_Number'] = df['content'].str.extract(pat='EudraCT Number: (.*)')  
    df['trial_subject_Utero'] = df['content'].str.extract(pat='F.1.1.1.1 Number of subjects for this age range: (.*)')
    df['trial_subject_Preterm'] = df['content'].str.extract(pat='F.1.1.2.1 Number of subjects for this age range: (.*)')
    df['trial_subject_Newborns'] = df['content'].str.extract(pat='F.1.1.3.1 Number of subjects for this age range: (.*)')
    df['trial_subject_toddlers'] = df['content'].str.extract(pat='F.1.1.4.1 Number of subjects for this age range: (.*)')
    df['trail_subject_Children'] = df['content'].str.extract(pat='F.1.1.5.1 Number of subjects for this age range: (.*)')
    df['trail_subject_Adolescents'] = df['content'].str.extract(pat='F.1.1.6.1 Number of subjects for this age range: (.*)')
    df['trail_subject_Adults'] = df['content'].str.extract(pat='F.1.2.1 Number of subjects for this age range:(.*)')
    df['trail_subject_Elderly'] = df['content'].str.extract(pat='F.1.3.1 Number of subjects for this age range: (.*)')
    df['trial_subject_Female'] = df['content'].str.extract(pat='F.2.1 Female: (.*)')
    df['trial_subject_male'] = df['content'].str.extract(pat='F.2.2 Male: (.*)')
    df['trial_subject_inMemberState'] = df['content'].str.extract(pat='F.4.1 In the member state: (.*)')
    df['trial_multinational'] = df['content'].str.extract(pat='F.4.2 For a multinational trial (.*)')
    df['trial_clinical'] = df['content'].str.extract(pat='F.4.2.2 In the whole clinical trial: (.*)')
    df['valid_from'] = pd.to_datetime(directory,errors='ignore',format='%d-%m-%Y')
    df=df[['uid','summary_EudraCT_Number','trial_subject_Utero','trial_subject_Preterm','trial_subject_Newborns',
    'trial_subject_toddlers','trail_subject_Children',
    'trail_subject_Adolescents','trail_subject_Adults','trail_subject_Elderly',
    'trial_subject_Female','trial_subject_male','trial_subject_inMemberState','trial_multinational','trial_clinical','valid_from','file_Name']]
    df=df.drop_duplicates()
    dbInstance.drop_table(f'"{schema_name}"."TRIAL_SUBJECT_INFO"')
    df.columns = df.columns.str.lower()
    df.to_sql('TRIAL_SUBJECT_INFO', con, index = True, if_exists='append',schema=schema_name)
    return(df)

def run_staging_process(directory):
    schema_name =f'staging_{directory.replace("-", "_")}'
    print(schema_name)
    ## Read from Ingestion Table
    df = pd.read_sql_table('load_txt',con,schema=schema_name)
    #build all the staging tables
    create_staging_sponsor(df,schema_name,directory)
    create_staging_protocol(df,schema_name,directory)
    create_staging_imp(df,schema_name,directory)
    create_staging_trial(df,schema_name,directory)
    create_trial_subject(df,schema_name,directory)
    create_trial_review(df,schema_name,directory)
    create_staging_summary(df,schema_name,directory)
    create_time_dimension(df,schema_name,directory)
