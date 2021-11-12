with rows_in_scope as (
select
		src."summary_EudraCT_Number",
		src."trial_review_Authority",	
		src."trial_review_ethics_Committee",
		src."trial_review_ethics_committeReason"
from
		"staging_15_09_2021"."TRIAL_REVIEW_INFO" src
left join "dataWareHouse"."DIM_TRIAL_REVIEW_INFO" dim
	  on
		src."summary_EudraCT_Number" = dim."summary_EudraCT_Number"
	and src."trial_review_Authority" = dim."trial_review_Authority"
	and src."trial_review_ethics_Committee" = dim."trial_review_ethics_Committee"
	and src."trial_review_ethics_committeReason" = dim."trial_review_ethics_committeReason"
where
		dim."summary_EudraCT_Number" is null)
	insert
		into
		"dataWareHouse"."DIM_TRIAL_REVIEW_INFO" ("summary_EudraCT_Number",
		"trial_review_Authority",
	"trial_review_ethics_Committee",
	"trial_review_ethics_committeReason") (
	select
			"summary_EudraCT_Number",
		"trial_review_Authority",
		"trial_review_ethics_Committee",
		"trial_review_ethics_committeReason"
	from
			rows_in_scope )