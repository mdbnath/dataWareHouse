CREATE OR REPLACE FUNCTION public.DIM_TRIAL_SUBJECT_INFO()
RETURNS BOOLEAN AS $$
begin
with rows_in_scope as (
select
		src."summary_EudraCT_Number",
	src."trial_subject_Utero",
	src."trial_subject_Newborns",
	src."trial_subject_toddlers",
	src."trail_subject_Children",
	src."trail_subject_Adolescents",
	src."trail_subject_Adults",
	src."trail_subject_total_Adults",
	src."trail_subject_Elderly",
	src."trial_subject_Female",
	src."trial_subject_male",
	src."trial_subject_inMemberState",
	src."trial_multinational",
	src."trial_clinical",
	src."valid_from" ,
	src."valid_to"
from
	"TRIAL_SUBJECT_INFO" src
left join "dataWareHouse"."DIM_TRIAL_SUBJECT_INFO" dim on
		src."summary_EudraCT_Number" = dim."summary_EudraCT_Number"
		and src."trial_subject_Utero" = dim."trial_subject_Utero"
		and src."trial_subject_Newborns" = dim."trial_subject_Newborns"
		and src."trial_subject_toddlers" = dim."trial_subject_toddlers"
		and	src."trail_subject_Children" = dim."trail_subject_Children"
		and src."trail_subject_Adolescents" = dim."trail_subject_Adolescents"
		and src."trail_subject_Adults" = dim."trail_subject_Adults"
		and src."trail_subject_total_Adults" = dim."trail_subject_total_Adults"
		and src."trail_subject_Elderly" = dim."trail_subject_Elderly"
		and	src."trial_subject_Female" = dim."trial_subject_Female"
		and src."trial_subject_male" = dim."trial_subject_male"
		and src."trial_subject_inMemberState" = dim."trial_subject_inMemberState"
		and src."trial_multinational" = dim."trial_multinational"
		and src."trial_clinical" = dim."trial_clinical"
)
insert
		into
		"dataWareHouse"."DIM_TRIAL_SUBJECT_INFO" (
	"uid",
	    "summary_EudraCT_Number",
		"trial_subject_Utero",
		"trial_subject_Newborns",
		"trial_subject_toddlers",
		"trail_subject_Children",
		"trail_subject_Adolescents",
		"trail_subject_Adults",
		"trail_subject_total_Adults",
		"trail_subject_Elderly",
		"trial_subject_Female",
		"trial_subject_male",
		"trial_subject_inMemberState",
		"trial_multinational",
		"trial_clinical",
	"valid_from" ,
	"valid_to" ) (
	select
		"uid",
		    "summary_EudraCT_Number",
		"trial_subject_Utero",
		"trial_subject_Newborns",
		"trial_subject_toddlers",
		"trail_subject_Children",
		"trail_subject_Adolescents",
		"trail_subject_Adults",
		"trail_subject_total_Adults",
		"trail_subject_Elderly",
		"trial_subject_Female",
		"trial_subject_male",
		"trial_subject_inMemberState",	
				"trial_multinational",
		"trial_clinical",
		current_timestamp,
 		null from rows_in_scope ) ON CONFLICT ("uid") DO UPDATE set "valid_to" = current_timestamp;	
		 return true;
exception
when others then
return false;
END;
$$ LANGUAGE plpgsql;