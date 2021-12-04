CREATE TABLE "dataWareHouse"."DIM_PROTOCOL_INFORMATION" (
	 id serial PRIMARY key,
	"uid" text ,
	"summary_EudraCT_Number" text NULL,
	"protocol_code_number" text NULL,
	"protocol_member_state" text NULL,
	"protocol_EudraCT_Number" text NULL,
	"protocol_trial_fulltitle" text null,
    "file_Name" text NULL,
	"valid_from" timestamp default CURRENT_TIMESTAMP,
	"valid_to" timestamp default null
);
