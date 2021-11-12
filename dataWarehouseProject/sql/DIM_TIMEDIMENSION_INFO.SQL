CREATE TABLE "dataWareHouse"."DIM_TIMEDIMENSION_INFO" (
	ID SERIAL PRIMARY KEY,
	"summary_EudraCT_Number" text null,
	"file_Name" text NULL,
	"summary_trial_dateEntered" timestamp,
	"trial_review_authority_date" timestamp,
	"trail_review_ethics_date" timestamp,	
	 valid_from timestamp default CURRENT_TIMESTAMP,
	 valid_to timestamp default CURRENT_TIMESTAMP

)
