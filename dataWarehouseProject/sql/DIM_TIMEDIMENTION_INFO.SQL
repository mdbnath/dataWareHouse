CREATE TABLE "dataWareHouse"."DIM_TIMEDIMENTION_INFO" (
	"summary_EudraCT_Number" text PRIMARY KEY,
	"file_Name" text NULL,
	"summary_trial_dateEntered" timestamp,
	"trial_review_authority_date" timestamp,
	"trail_review_ethics_date" timestamp,	
	 valid_from timestamp default CURRENT_TIMESTAMP,
	 valid_to timestamp default CURRENT_TIMESTAMP

)
