
CREATE TABLE "dataWareHouse"."DIM_TRIAL_ENDPOINTS" (
	id int GENERATED ALWAYS AS IDENTITY primary KEY,
    eudract_Number text,
	Trial_protocol text NULL,
	total_medicine NUMERIC NULL,	
	mortality_medicine NUMERIC NULL,
	mortality_placebo NUMERIC NULL,
	total_placebo NUMERIC NULL,
	icu_medicine NUMERIC NULL,
	icu_placebo NUMERIC NULL,
	ventilation_medicine NUMERIC NULL,
	ventilation_placebo NUMERIC NULL,
	file_Name text NULL,
	valid_from timestamp,
	valid_to timestamp default null
);