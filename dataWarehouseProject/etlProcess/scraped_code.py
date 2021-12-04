def create_trial_subject(df,schema_name): 
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
    df['trail_subject_Adults'] = df['content'].str.extract(pat='F.1.1.5.1 Number of subjects for this age range:(.*)')
    df['trail_subject_total_Adults'] = df['content'].str.extract(pat='F.1.2.1 Number of subjects for this age range:(.*)')
    df['trail_subject_Elderly'] = df['content'].str.extract(pat='F.1.3.1 Number of subjects for this age range: (.*)')
    df['trial_subject_Female'] = df['content'].str.extract(pat='F.2.1 Female: (.*)')
    df['trial_subject_male'] = df['content'].str.extract(pat='F.2.2 Male: (.*)')
    df['trial_subject_inMemberState'] = df['content'].str.extract(pat='F.4.1 In the member state: (.*)')
    df['trial_multinational'] = df['content'].str.extract(pat='F.4.2 For a multinational trial (.*)')
    df['trial_clinical'] = df['content'].str.extract(pat='F.4.2.2 In the whole clinical trial: (.*)')
    df=df[['uid','summary_EudraCT_Number','trial_subject_Utero','trial_subject_Preterm','trial_subject_Newborns',
    'trial_subject_toddlers','trail_subject_Children',
    'trail_subject_Adolescents','trail_subject_Adults','trail_subject_total_Adults','trail_subject_Elderly',
    'trial_subject_Female','trial_subject_male','trial_subject_inMemberState','trial_multinational','trial_clinical']]
    df=df.drop_duplicates()
    dbInstance.drop_table(f'"{schema_name}"."TRIAL_SUBJECT_INFO"')
    df.to_sql('TRIAL_SUBJECT_INFO', con, index = True, if_exists='append',schema=schema_name)
    return(df)