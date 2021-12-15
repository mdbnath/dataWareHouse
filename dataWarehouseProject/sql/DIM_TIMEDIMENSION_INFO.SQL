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