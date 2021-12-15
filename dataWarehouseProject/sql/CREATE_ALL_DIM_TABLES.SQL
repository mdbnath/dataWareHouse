CREATE TABLE "dataWareHouse"."DIM_IMP_IDENTIFICATION" (
	id int GENERATED ALWAYS AS IDENTITY primary KEY,
    "uid" text,
	summary_EudraCT_Number text NULL,
	imp_role text NULL,	
	imp_marketing_authorisation text NULL,
	imp_Trade_name text NULL,
	imp_marketing_authorisation_holder text NULL,
	imp_orphan_drug text NULL,
	imp_orphan_drug_number text NULL,
	imp_productName text NULL,
	imp_productCode text NULL,
	--imp_pharmaceutical_form text NULL,
	--imp_paediatric_formulation text NULL,
	--imp_admin_routes text NULL,
	--"imp_INN" text NULL,
	--imp_casnumber text NULL,
	--imp_substance_code text NULL,
	--"imp_concentrationType" text NULL,
	--"imp_concentrationNumber" text NULL,
	--"imp_concentrationUnit" text NULL,
	--imp_chemical_origin text NULL,
	--imp_biotechnological_origin text NULL,
	--"imp_ATIMP" text NULL,
	--imp_somatic_cell_therapy text NULL,
	--imp_gene_therapy text NULL,
	--imp_tissue_engineered text NULL,
	--"imp_Combination_ATIMP" text NULL,
	--"imp_CAT_classification" text NULL,
	--"imp_advanced_Therapy" text NULL,
	--imp_placebo_used text NULL,
	file_Name text NULL,
	valid_from timestamp,
	valid_to timestamp default null
);

CREATE TABLE "dataWareHouse"."DIM_PROTOCOL_INFORMATION" (
	id int GENERATED ALWAYS AS IDENTITY primary KEY,
	"uid" text ,
	summary_EudraCT_Number text NULL,
	protocol_code_number text NULL,
	protocol_member_state text NULL,
	protocol_EudraCT_Number text NULL,
	protocol_trial_fulltitle text null,
    file_Name text NULL,
	valid_from timestamp ,
	valid_to timestamp default null
);

CREATE TABLE "dataWareHouse"."DIM_SPONSOR_INFORMATION" (
	id int GENERATED ALWAYS AS IDENTITY primary KEY,
	"uid" text,
	summary_EudraCT_Number text NULL,
	sponsor_name text NULL,
	sponsor_country text NULL,
	sponsor_status text NULL,
	sponsor_source text NULL,
	sponsor_organisation text NULL,
	sponsor_streetName text NULL,
	sponsor_city text NULL,
	sponsor_postalCode text NULL,
	sponsor_email text NULL,
	file_Name text NULL,
	valid_from timestamp,
	valid_to timestamp default null
);

CREATE TABLE "dataWareHouse"."DIM_SUMMARY_INFORMATION" (
    
	id int GENERATED ALWAYS AS IDENTITY primary KEY,
    "uid" text,
	summary_EudraCT_Number text,
	file_Name text NULL,
	summary_Protocol_Number text NULL,
	--"summary_NCA" text NULL,
	summary_trial_type text NULL,
	summary_trial_status text NULL,
	valid_from timestamp ,
	valid_to timestamp null
);

CREATE TABLE "dataWareHouse"."DIM_TIMEDIMENSION_INFO" (
	id int GENERATED ALWAYS AS IDENTITY primary KEY,
	"uid" text,
	summary_EudraCT_Number text NULL,
	file_Name text NULL,
	summary_trial_dateEntered timestamp NULL,
	trial_review_authority_date timestamp NULL,
	trail_review_ethics_date timestamp NULL,
	trial_end_date timestamp NULL,
	publication_date timestamp NULL,
	valid_from timestamp,
	valid_to timestamp NULL
);



CREATE TABLE "dataWareHouse"."DIM_TRIAL_ENDPOINTS" (
	id int GENERATED ALWAYS AS IDENTITY primary KEY,
    eudract_Number text,
	Trial_protocol text NULL,
	total_medicine text NULL,	
	mortality_medicine text NULL,
	mortality_placebo text NULL,
	total_placebo text NULL,
	icu_medicine text NULL,
	icu_placebo text NULL,
	ventilation_medicine text NULL,
	ventilation_placebo text NULL,
	file_Name text NULL,
	valid_from timestamp,
	valid_to timestamp default null
);


CREATE TABLE "dataWareHouse"."DIM_TRIAL_INFORMATION" (
    
	id int GENERATED ALWAYS AS IDENTITY primary KEY,
	"uid" text,
	summary_EudraCT_Number text NULL,
	trial_investigation text NULL,
	trial_language text NULL,	
	trial_MedDRA_classification text NULL,	
	trial_subStudy text NULL,
	trial_controlled text NULL,
	trail_randomised text NULL,
	trial_single_blind text NULL,
	trail_double_blind text NULL,
	trial_arms text NULL,
    trial_status TEXT NULL,
	trial_member_state text NULL,
	trial_phaseI TEXT NULL,
	trial_phaseII TEXT NULL,
	trial_phaseIII TEXT NULL,
	trial_phaseIV TEXT NULL,
	file_Name text NULL,
	valid_from timestamp,
	valid_to timestamp default null
);


CREATE TABLE "dataWareHouse"."DIM_TRIAL_REVIEW_INFO" (
    "uid" text PRIMARY KEY,
	"summary_EudraCT_Number"  text NULL,
	"trial_review_Authority"  text NULL,
	"trial_review_ethics_Committee"  text NULL,
	"trial_review_ethics_committeReason"  text NULL,	
	"file_Name" text NULL,	  
	valid_from timestamp default CURRENT_TIMESTAMP,
	valid_to timestamp default null
);


CREATE TABLE "dataWareHouse"."DIM_TRIAL_SUBJECT_INFO" (	
	id int GENERATED ALWAYS AS IDENTITY primary KEY,
	"uid" text ,
	summary_EudraCT_Number text NULL,
	trial_subject_Utero text NULL,
	trial_subject_Newborns text NULL,
	trial_subject_toddlers text NULL,
	trail_subject_Children text NULL,
	trail_subject_Adolescents text NULL,
	trail_subject_Adults text NULL,
	trail_subject_Elderly text NULL,
	trial_subject_Female text NULL,
	trial_subject_male text NULL,
	trial_subject_inMemberState text NULL,
	trial_multinational text NULL,
	trial_clinical text NULL,
	file_Name text NULL,
	valid_from timestamp,
	valid_to timestamp default null
);
