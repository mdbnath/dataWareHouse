
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
