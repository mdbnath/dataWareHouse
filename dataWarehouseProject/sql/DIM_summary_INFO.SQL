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
)
