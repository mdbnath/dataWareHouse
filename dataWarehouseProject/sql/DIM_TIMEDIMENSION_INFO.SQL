CREATE TABLE "dataWareHouse"."DIM_TIMEDIMENSION_INFO" (
	uid text NOT NULL,
	"summary_EudraCT_Number" text NULL,
	"file_Name" text NULL,
	"summary_trial_dateEntered" timestamp NULL,
	trial_review_authority_date timestamp NULL,
	trail_review_ethics_date timestamp NULL,
	valid_from timestamp NULL DEFAULT CURRENT_TIMESTAMP,
	valid_to timestamp NULL
);