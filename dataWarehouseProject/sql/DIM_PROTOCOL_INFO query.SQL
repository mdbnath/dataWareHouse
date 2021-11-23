CREATE OR REPLACE FUNCTION public.protocol_information()
RETURNS BOOLEAN AS $$
begin
with rows_in_scope as (
select
	src."summary_EudraCT_Number",
		src."protocol_member_state",
		src."protocol_EudraCT_Number",
		src."uid",
		src."protocol_trial_fullTitle",
		src."protocol_code_number"
from
		"PROTOCOL_INFORMATION" src
left join "dataWareHouse"."DIM_PROTOCOL_INFORMATION" dim
  on
	src."uid"=dim."uid"
	and src."summary_EudraCT_Number" = dim."summary_EudraCT_Number"
	and src."protocol_member_state" = dim."protocol_member_state"
	and src."protocol_EudraCT_Number" = dim."protocol_EudraCT_Number"
	and src."uid" = dim."uid"
	and src."protocol_trial_fullTitle" = dim."protocol_trial_fulltitle"
	and src."protocol_code_number" = dim."protocol_code_number")
insert
	into
	"dataWareHouse"."DIM_PROTOCOL_INFORMATION" ("summary_EudraCT_Number",
		"protocol_member_state",
	"protocol_EudraCT_Number",
	"uid",
	"protocol_trial_fulltitle",
        "protocol_code_number",
		"valid_from",
		"valid_to") (
	select
		"summary_EudraCT_Number",
		"protocol_member_state",
		"protocol_EudraCT_Number",
		"uid",
		"protocol_trial_fullTitle",
        "protocol_code_number",
		current_timestamp,
 		null from rows_in_scope ) ON CONFLICT ("uid")
DO UPDATE set "valid_to" = current_timestamp;
return true;
exception
when others then
return false;
END;
$$ LANGUAGE plpgsql;