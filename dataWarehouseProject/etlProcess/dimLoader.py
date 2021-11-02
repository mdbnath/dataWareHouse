import datetime
import os,sys
import traceback
from numpy import NaN
import pandas as pd
from dataWarehouseProject.database import DatabaseInterface


def extract_sources(test_string):
    elements = [line for line in test_string.split("\n") if line.startswith('B.4.')]
    output = []
    for element_number in range(0, len(elements), 2):
        name = elements[element_number].split(': ')[-1]
        country = elements[element_number + 1].split(': ')[-1]
        output.append((name, country))
      
    print(output)

    
def create_staging_protocol(df):   
    df['protocol_member_state'] = df['content'].str.extract(pat='A.1 Member State Concerned: (.*)')
    df['protocol_EudraCT_Number'] = df['content'].str.extract(pat='A.2 EudraCT number: (.*)')
    df['protocol_trial_fullTitle'] = df['content'].str.extract(pat='A.3 Full title of the trial: (.*)')
    df['protocol_code_number'] = df['content'].str.extract(pat='A.4.1 Sponsor''s protocol code number:  (.*)')
    df['protocol_ISRCTN_number'] = df['content'].str.extract(pat='A.5.1 ISRCTN (International Standard Randomised Controlled Trial) number: (.*)')
    df['protocol_UTRN_number'] = df['content'].str.extract(pat='A.5.3 WHO Universal Trial Reference Number (UTRN): (.*)')
    df['protocol_Paediatric_investigation'] = df['content'].str.extract(pat='A.7 Trial is part of a Paediatric Investigation Plan: (.*)')
    df['protocol_EMA_decision_number'] = df['content'].str.extract(pat='A.8 EMA Decision number of Paediatric Investigation Plan: (.*)')
    df.to_sql('PROTOCOL_INFORMATION', DatabaseInterface().getConnection(engine = True), index = True, if_exists='append',schema='staging')
    return(df)


def create_staging_sponsor(df):   
    df['sponsor_name'] = df['content'].str.extract(pat='B.1.1 Name of Sponsor: (.*)')
    df['sponsor_country'] = df['content'].str.extract(pat='B.1.3.4	Country: (.*)')
    df['sponsor_status'] = df['content'].str.extract(pat='B.3.1 and B.3.2	Status: (.*)')
    df['sponsor_source'] = df['content'].apply(extract_sources)
    df['sponsor_organisation'] = df['content'].str.extract(pat='B.5.1 Name of organisation: (.*)')
    df['sponsor_streetName'] = df['content'].str.extract(pat='B.5.3.1 Street Address: (.*)')
    df['sponsor_city'] = df['content'].str.extract(pat='B.5.3.2 Town/ city: (.*)')
    df['sponsor_postalCode'] = df['content'].str.extract(pat='B.5.3.3 Post code: (.*)')
    df['sponsor_email'] = df['content'].str.extract(pat='B.5.6 E-mail: (.*)')
    df.to_sql('SPONSOR_INFORMATION', DatabaseInterface().getConnection(engine = True), index = True, if_exists='append',schema='staging')
    return(df)

def create_dim_imp(df):   
    df['imp_role'] = df['content'].str.extract(pat='D.1.2 and D.1.3 IMP Role: (.*)')
    df['imp_marketing_authorisation'] = df['content'].str.extract(pat='D.2.1 IMP to be used in the trial has a marketing authorisation:')
    df['imp_Trade_name'] = df['content'].str.extract(pat='D.2.1.1.1 Trade name: (.*)')
    df['imp_marketing_authorisation_holder'] = df['content'].str.extract(pat='D.2.1.1.2 Name of the Marketing Authorisation holder: (.*)')
    df['imp_orphan_drug'] = df['content'].str.extract(pat='D.2.5 The IMP has been designated in this indication as an orphan drug in the Community: (.*)')
    df['imp_orphan_drug_number'] = df['content'].str.extract(pat='D.2.5.1 Orphan drug designation number: (.*)')   
    df['imp_productName'] = df['content'].str.extract(pat='D.3.1 Product name: (.*)')
    df['imp_productCode'] = df['content'].str.extract(pat='D.3.2 Product code: (.*)')
    df['imp_pharmaceutical_form'] = df['content'].str.extract(pat='D.3.4 Pharmaceutical form: (.*)')
    df['imp_paediatric_formulation'] = df['content'].str.extract(pat='D.3.4.1 Specific paediatric formulation: (.*)')
    df['imp_admin_routes'] = df['content'].str.extract(pat='D.3.7 Routes of administration for this IMP: (.*)')
    df['imp_INN'] = df['content'].str.extract(pat='D.3.8 INN - Proposed INN: (.*)')
    df['imp_casnumber'] = df['content'].str.extract(pat='D.3.9.1 CAS number: (.*)')
    df['imp_substance_code'] = df['content'].str.extract(pat='D.3.9.4 EV Substance Code: (.*)')
    df['imp_concentrationType'] = df['content'].str.extract(pat='D.3.10.2 Concentration type: (.*)')
    df['imp_concentrationNumber'] = df['content'].str.extract(pat='D.3.10.3 Concentration number: (.*)')
    df['imp_concentrationUnit'] = df['content'].str.extract(pat='D.3.10.1 Concentration unit: (.*)')
    df['imp_chemical_origin'] = df['content'].str.extract(pat='D.3.11.1 Active substance of chemical origin: (.*)')
    df['imp_biotechnological_origin'] = df['content'].str.extract(pat='D.3.11.2 Active substance of biological/ biotechnological origin (other than Advanced Therapy IMP (ATIMP):  (.*)')
    df['imp_ATIMP'] = df['content'].str.extract(pat='D.3.11.3 Advanced Therapy IMP (ATIMP): (.*)')
    df['imp_somatic_cell_therapy'] = df['content'].str.extract(pat='D.3.11.3.1 Somatic cell therapy medicinal product: (.*)')
    df['imp_gene_therapy'] = df['content'].str.extract(pat='D.3.11.3.2 Gene therapy medical product: (.*)')
    df['imp_tissue_engineered'] = df['content'].str.extract(pat='D.3.11.3.3 Tissue Engineered Product: (.*)')
    df['imp_Combination_ATIMP'] = df['content'].str.extract(pat='D.3.11.3.4 Combination ATIMP (i.e. one involving a medical device): (.*)')
    df['imp_CAT_classification'] = df['content'].str.extract(pat='D.3.11.3.5 Committee on Advanced therapies (CAT) has issued a classification for this product: (.*)')
    df['imp_advanced_Therapy'] = df['content'].str.extract(pat='D.3.11.4 Combination product that includes a device, but does not involve an Advanced Therapy: (.*)')
    df['imp_Radiopharmaceutical_product'] = df['content'].str.extract(pat='D.3.11.5 Radiopharmaceutical medicinal product: (.*)')
    df['imp_Immunological_product'] = df['content'].str.extract(pat='D.3.11.6 Immunological medicinal product (such as vaccine, allergen, immune serum): (.*)')
    df['imp_plasmaDerived_product'] = df['content'].str.extract(pat='D.3.11.7 Plasma derived medicinal product: (.*)')
    df['imp_Extractive_product'] = df['content'].str.extract(pat='D.3.11.8 Extractive medicinal product: (.*)')
    df['imp_Recombinant_product'] = df['content'].str.extract(pat='D.3.11.9 Recombinant medicinal product: (.*)')
    df['imp_genetically_modified_organism'] = df['content'].str.extract(pat='D.3.11.10 Medicinal product containing genetically modified organisms: (.*)') 
    df['imp_Herbal_product'] = df['content'].str.extract(pat='D.3.11.11 Herbal medicinal product: (.*)')    
    df['imp_Homeopathic_product'] = df['content'].str.extract(pat='D.3.11.12 Homeopathic medicinal product: (.*)')    
    df['imp_other_product'] = df['content'].str.extract(pat='D.3.11.13 Another type of medicinal product: (.*)')
    df['imp_placebo_used'] = df['content'].str.extract(pat='D.8.1 Is a Placebo used in this Trial? (.*)')   
    df.to_sql('IMP_IDENTIFICATION', DatabaseInterface().getConnection(engine = True), index = True, if_exists='append')
    return(df)

def create_dim_trial(df):   

    df['trial_investigation'] = df['content'].str.extract(pat='E.1.1 Medical condition(s) being investigated: (.*)')
    df['trial_version'] = df['content'].str.extract(pat='E.1.1.1 Medical condition in easily understood language: (.*)')
    df['trial_version'] = df['content'].str.extract(pat='E.1.1.2 Therapeutic area: (.*)')
 ##call method
    df['trial_version'] = df['content'].str.extract(pat='E.1.3 Condition being studied is a rare disease: (.*)')
    df['trial_version'] = df['content'].str.extract(pat='E.2.1 Main objective of the trial: (.*)')
    df['trial_version'] = df['content'].str.extract(pat='E.2.2 Secondary objectives of the trial: (.*)')
    df['trial_version'] = df['content'].str.extract(pat='E.2.3 Trial contains a sub-study: (.*)')
    df['trial_version'] = df['content'].str.extract(pat='E.3 Principal inclusion criteria: (.*)')
    df['trial_version'] = df['content'].str.extract(pat='E.4 Principal exclusion criteria: (.*)')
    df['trial_version'] = df['content'].str.extract(pat='E.4 Principal exclusion criteria: (.*)')
    df['trial_version'] = df['content'].str.extract(pat='E.4 Principal exclusion criteria: (.*)')

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



def create_staging_protocol(df):   
    df['protocol_member_state'] = df['content'].str.extract(pat='A.1 Member State Concerned: (.*)')
    df['protocol_EudraCT_Number'] = df['content'].str.extract(pat='A.2 EudraCT number: (.*)')
    df['protocol_trial_fullTitle'] = df['content'].str.extract(pat='A.3 Full title of the trial: (.*)')
    df['protocol_code_number'] = df['content'].str.extract(pat='A.4.1 Sponsor''s protocol code number:  (.*)')
    df['protocol_ISRCTN_number'] = df['content'].str.extract(pat='A.5.1 ISRCTN (International Standard Randomised Controlled Trial) number: (.*)')
    df['protocol_UTRN_number'] = df['content'].str.extract(pat='A.5.3 WHO Universal Trial Reference Number (UTRN): (.*)')
    df['protocol_Paediatric_investigation'] = df['content'].str.extract(pat='A.7 Trial is part of a Paediatric Investigation Plan: (.*)')
    df['protocol_EMA_decision_number'] = df['content'].str.extract(pat='A.8 EMA Decision number of Paediatric Investigation Plan: (.*)')
    df.to_sql('PROTOCOL_INFORMATION', DatabaseInterface().getConnection(engine = True), index = True, if_exists='append',schema='staging')
    return(df)
 
## Read from Ingestion Table
df = pd.read_sql_table('load_txt',DatabaseInterface().getConnection(engine = True),schema='staging')
#build all the DIM tables
create_staging_sponsor(df)
#create_dim_trial(df)
#create_dim_imp(df)
            