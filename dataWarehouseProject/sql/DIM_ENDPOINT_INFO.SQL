

CREATE TABLE "dataWareHouse"."DIM_IMP_IDENTIFICATION" (
	id int GENERATED ALWAYS AS IDENTITY primary KEY,
    summary_EudraCT_Number text,
	Trial_protocol text NULL,
	total_medicine text NULL,	
	mortality_medicine text NULL,
	mortality_placebo text NULL,
	total_placebo text NULL,
	file_Name text NULL,
	valid_from timestamp,
	valid_to timestamp default null
);

  total_medicine total_placebo    mortality_medicine     mortality_placebo                  filename