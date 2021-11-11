with rows_in_scope as (
  SELECT src."summary_EudraCT_Number",
		src."summary_Protocol_Number",
		src."summary_trial_type",
		src."uid",
		src."summary_trial_status"
		FROM "staging_15_09_2021"."SUMMARY_INFORMATION" src
  LEFT JOIN "dataWareHouse"."DIM_SUMMARY_INFORMATION" dim
  ON src."summary_EudraCT_Number"=dim."summary_EudraCT_Number"
  AND src."summary_Protocol_Number"=dim."summary_Protocol_Number"
  AND src."summary_trial_type"=dim."summary_trial_type"
  AND src."uid"=dim."uid"
  AND src."summary_trial_status"=dim."summary_trial_status"
where dim."uid" is null)
INSERT INTO "dataWareHouse"."DIM_SUMMARY_INFORMATION" (SELECT 
        "summary_EudraCT_Number",
		"summary_Protocol_Number",
		"summary_trial_type",
		"uid",
		"summary_trial_status"
 FROM rows_in_scope )