CREATE OR REPLACE FUNCTION public.DIM_TIMEDIMENSION_INFO()
RETURNS BOOLEAN AS $$
begin
with rows_in_scope as (
select
	src."summary_EudraCT_Number",
	src."summary_trial_dateEntered",
	src."trial_review_authority_date",
	src."trail_review_ethics_date",
	src."uid"
from
	"TIME_DIMENSION" src
left join "dataWareHouse"."DIM_TIMEDIMENSION_INFO" dim
	  on src."uid"=dim."uid"
		and	src."summary_EudraCT_Number" = dim."summary_EudraCT_Number"
		and src."summary_trial_dateEntered" = dim."summary_trial_dateEntered"
		and src."trial_review_authority_date" = dim."trial_review_authority_date"
		and src."trail_review_ethics_date" = dim."trail_review_ethics_date")
insert
		into
		"dataWareHouse"."DIM_TIMEDIMENSION_INFO" (
		"summary_EudraCT_Number",
		"summary_trial_dateEntered",
		"trial_review_authority_date",
		"trail_review_ethics_date",
		"valid_from",
		"valid_to") (
	select
		"summary_EudraCT_Number",
		"summary_trial_dateEntered",
		"trial_review_authority_date",
		"trail_review_ethics_date",
		current_timestamp,
 		null from rows_in_scope ) ON CONFLICT ("uid") DO UPDATE set "valid_to" = current_timestamp;	
		 return true;
exception
when others then
return false;
END;
$$ LANGUAGE plpgsql;