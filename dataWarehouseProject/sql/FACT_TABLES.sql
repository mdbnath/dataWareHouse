CREATE TABLE "dataWareHouse"."FACT_TRIAL_STUDIES" (
	id int GENERATED ALWAYS AS IDENTITY primary KEY,
	Trial_EudraCT_Number INTEGER NULL,
	trial_info INTEGER NULL,
	trial_subject_details INTEGER NULL,
	dates INTEGER NULL,
	total_medicine text NULL,	
	mortality_medicine text NULL,
	mortality_placebo text NULL,
	total_placebo text NULL,
	icu_medicine text NULL,
	icu_placebo text NULL,
	ventilation_medicine text NULL,
	ventilation_placebo text NULL,
	trial_review INTEGER NULL
);

CREATE TABLE "dataWareHouse"."FACT_SPONSOR_STUDIES" (
	id int GENERATED ALWAYS AS IDENTITY primary KEY,
	Trial_EudraCT_Number INTEGER NULL,
	trial_info INTEGER NULL,
	trial_sponsor INTEGER NULL,
	protocol_info INTEGER NULL,
    dates INTEGER NULL
);

