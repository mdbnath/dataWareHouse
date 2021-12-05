
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
