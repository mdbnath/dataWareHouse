CREATE TABLE "dataWareHouse"."DIM_SPONSOR_INFORMATION" (
	ID SERIAL PRIMARY KEY,
	uid text,
	"summary_EudraCT_Number" text NULL,
	sponsor_name text NULL,
	sponsor_country text NULL,
	sponsor_status text NULL,
	sponsor_source text NULL,
	sponsor_organisation text NULL,
	"sponsor_streetName" text NULL,
	sponsor_city text NULL,
	"sponsor_postalCode" text NULL,
	sponsor_email text NULL,
	"file_Name" text NULL,
	valid_from timestamp default CURRENT_TIMESTAMP,
	valid_to timestamp default CURRENT_TIMESTAMP
);

