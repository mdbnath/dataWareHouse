CREATE TABLE "dataWareHouse"."FACT_TRIAL_STUDIES" (
	id int GENERATED ALWAYS AS IDENTITY primary KEY,
	Trial_EudraCT_Number INTEGER NULL,
	trial_info INTEGER NULL,
	trial_subject_details INTEGER NULL,
	dates INTEGER NULL,
	total_medicine NUMERIC NULL,	
	mortality_medicine NUMERIC NULL,
	mortality_placebo NUMERIC NULL,
	total_placebo NUMERIC NULL,
	icu_medicine NUMERIC NULL,
	icu_placebo NUMERIC NULL,
	ventilation_medicine NUMERIC NULL,
	ventilation_placebo NUMERIC NULL,
	trial_review INTEGER NULL,
	protocol_info INTEGER NULL
);

CREATE TABLE "dataWareHouse"."FACT_SPONSOR_STUDIES" (
	id int GENERATED ALWAYS AS IDENTITY primary KEY,
	Trial_EudraCT_Number INTEGER NULL,
	trial_info INTEGER NULL,
	trial_sponsor INTEGER NULL,
	protocol_info INTEGER NULL,
    dates INTEGER NULL
);

