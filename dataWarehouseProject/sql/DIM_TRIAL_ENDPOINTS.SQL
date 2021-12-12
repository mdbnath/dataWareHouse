
CREATE TABLE "dataWareHouse"."DIM_TRIAL_ENDPOINTS" (
	id int GENERATED ALWAYS AS IDENTITY primary KEY,
    eudract_Number text,
	Trial_protocol text NULL,
	trial_end_date timestamp NULL,
	total_medicine text NULL,	
	mortality_medicine text NULL,
	mortality_placebo text NULL,
	total_placebo text NULL,
	icu_medicine text NULL,
	icu_placebo text NULL,
	ventilation_medicine text NULL,
	ventilation_placebo text NULL,
	file_Name text NULL,
	valid_from timestamp,
	valid_to timestamp default null
);