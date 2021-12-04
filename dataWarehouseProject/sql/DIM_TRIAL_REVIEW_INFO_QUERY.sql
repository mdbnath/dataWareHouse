CREATE OR REPLACE FUNCTION public.DIM_TRIAL_REVIEW_INFO()
RETURNS BOOLEAN AS $$
begin
with rows_in_scope as (
select
		src."uid",
		src."summary_EudraCT_Number",
		src."trial_review_Authority",	
		src."trial_review_ethics_Committee",
		src."trial_review_ethics_committeReason",
	src."valid_from" ,
	src."valid_to"
from
	"TRIAL_REVIEW_INFO" src
left join "dataWareHouse"."DIM_TRIAL_REVIEW_INFO" dim
	  		on src."uid"=dim."uid"
		and	src."summary_EudraCT_Number" = dim."summary_EudraCT_Number"
		and src."trial_review_Authority" = dim."trial_review_Authority"
		and src."trial_review_ethics_Committee" = dim."trial_review_ethics_Committee"
		and src."trial_review_ethics_committeReason" = dim."trial_review_ethics_committeReason"
)
insert
		into
		"dataWareHouse"."DIM_TRIAL_REVIEW_INFO" (
	"uid",
	"summary_EudraCT_Number",
	"trial_review_Authority",	
	"trial_review_ethics_Committee",
	"trial_review_ethics_committeReason",
	"valid_from" ,
	"valid_to" ) (
	select
		"uid",
		"summary_EudraCT_Number",
		"trial_review_Authority",	
		"trial_review_ethics_Committee",
		"trial_review_ethics_committeReason",		
		current_timestamp,
 		null from rows_in_scope ) ON CONFLICT ("uid") DO UPDATE set "valid_to" = current_timestamp;	
		 return true;
exception
when others then
return false;
END;
$$ LANGUAGE plpgsql;