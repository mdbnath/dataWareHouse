	
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
	src."trial_clinical"
from
	"staging_15_09_2021"."TRIAL_SUBJECT_INFO" src
	left join "dataWareHouse"."DIM_TRIAL_SUBJECT_INFO" dim
	  on
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
	where
		dim."summary_EudraCT_Number" is null)
	insert
		into
		"dataWareHouse"."DIM_TRIAL_SUBJECT_INFO" (
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
		"trial_clinical"
) (select
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
		"trial_clinical"		
from  rows_in_scope )