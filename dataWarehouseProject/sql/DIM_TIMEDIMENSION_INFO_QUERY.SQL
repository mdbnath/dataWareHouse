with rows_in_scope as (
select
	src."summary_EudraCT_Number",
	src."summary_trial_dateEntered",
	src."trial_review_authority_date",
	src."trail_review_ethics_date"
from
	"staging_15_09_2021"."TIME_DIMENSION" src
left join "dataWareHouse"."DIM_TIMEDIMENSION_INFO" dim
	  on
		src."summary_EudraCT_Number" = dim."summary_EudraCT_Number"
	and src."summary_trial_dateEntered" = dim."summary_trial_dateEntered"
	and src."trial_review_authority_date" = dim."trial_review_authority_date"
	and src."trail_review_ethics_date" = dim."trail_review_ethics_date"
where
		dim."summary_EudraCT_Number" is null)
	insert
		into
		"dataWareHouse"."DIM_TIMEDIMENSION_INFO" (
		"summary_EudraCT_Number",
		"summary_trial_dateEntered",
		"trial_review_authority_date",
		"trail_review_ethics_date") (
	select
		"summary_EudraCT_Number",
		"summary_trial_dateEntered",
		"trial_review_authority_date",
		"trail_review_ethics_date"
	from
			rows_in_scope )