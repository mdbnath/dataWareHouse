CREATE OR REPLACE FUNCTION public.DIM_TRIAL_INFORMATION() RETURNS BOOLEAN AS $$
begin
with rows_in_scope as (
select
		src."uid",
	src."summary_EudraCT_Number",
	src."trial_investigation" ,
	src."trial_language",
	src."trial_MedDRA_classification" ,
	src."trial_subStudy",
	src."trial_controlled",
	src."trail_randomised" ,
    src."trial_status",
	src."trial_member_state" ,
	src."valid_from" ,
	src."valid_to"
from
	"TRIAL_INFORMATION" src
left join "dataWareHouse"."DIM_TRIAL_INFORMATION" dim
	  		on src."uid"=dim."uid"
		and	src."summary_EudraCT_Number" = dim."summary_EudraCT_Number"
		and src."trial_language" = dim."trial_language"
		and src."trial_MedDRA_classification" = dim."trial_MedDRA_classification"
		and src."trial_subStudy" = dim."trial_subStudy"
		and src."trial_controlled" = dim."trial_controlled"
		and src."trail_randomised" = dim."trail_randomised"
		and src."trial_Single_blind" = dim."trial_Single_blind"
		and src."trail_Double_blind" = dim."trail_Double_blind"
		and src."trial_arms" = dim."trial_arms"
		and src."trial_status" = dim."trial_status"
		and src."trial_member_state" = dim."trial_member_state")
insert
		into
		"dataWareHouse"."DIM_TRIAL_INFORMATION" (
		"uid",
"summary_EudraCT_Number",
"trial_investigation",
"trial_language",
"trial_MedDRA_classification",
"trial_subStudy",
"trial_controlled",
"trail_randomised",
"trial_status",
"trial_member_state",
"trial_Single_blind",
"trail_Double_blind",
"trial_arms",
"valid_from",
"valid_to" ) (
	select
	"uid",
	"summary_EudraCT_Number",
"trial_investigation",
"trial_language",
"trial_MedDRA_classification",
"trial_subStudy",
"trial_controlled",
"trail_randomised",
"trial_status",
"trial_member_state",
"trial_Single_blind",
"trail_Double_blind",
"trial_arms",

		current_timestamp,
 		null from rows_in_scope ) ON CONFLICT ("uid") DO UPDATE set "valid_to" = current_timestamp;
		 return true;
exception
when others then
return false;
END;
$$ LANGUAGE plpgsql;