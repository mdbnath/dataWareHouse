
CREATE TABLE "dataWareHouse"."DIM_TRIAL_INFORMATION" (

    ID SERIAL PRIMARY KEY,
	"summary_EudraCT_Number" text NULL,
	trial_investigation text NULL,
	trial_language text NULL,	
	"trial_MedDRA_classification" text NULL,	
	"trial_subStudy" text NULL,
	trial_controlled text NULL,
	trail_randomised text NULL,
    trial_status TEXT NULL,
	trial_member_state text NULL,
    "file_Name" text NULL,
	valid_from timestamp default CURRENT_TIMESTAMP,
	valid_to timestamp default CURRENT_TIMESTAMP
);
