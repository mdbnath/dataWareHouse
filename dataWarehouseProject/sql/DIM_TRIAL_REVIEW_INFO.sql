
CREATE TABLE "dataWareHouse"."DIM_TRIAL_REVIEW_INFO" (	

	ID SERIAL PRIMARY KEY,
	"summary_EudraCT_Number" text NULL,
	"trial_review_Authority" text NULL,
	"trial_review_ethics_Committee" text NULL,
	"trial_review_ethics_committeReason" text NULL,
	"file_Name" text NULL,
	valid_from timestamp default CURRENT_TIMESTAMP,
	valid_to timestamp default CURRENT_TIMESTAMP
);
