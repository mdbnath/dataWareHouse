CREATE OR REPLACE FUNCTION public.DIM_SUMMARY_INFORMATION()
RETURNS BOOLEAN AS $$
begin
	with rows_in_scope as (
  SELECT src."summary_EudraCT_Number",
		src."summary_Protocol_Number",
		src."summary_trial_type",
		src."uid",
		src."summary_trial_status"
		FROM "SUMMARY_INFORMATION" src
  LEFT JOIN "dataWareHouse"."DIM_SUMMARY_INFORMATION" dim
  ON 
  src."uid"=dim."uid"
  and src."summary_EudraCT_Number"=dim."summary_EudraCT_Number"
  AND src."summary_Protocol_Number"=dim."summary_Protocol_Number"
  AND src."summary_trial_type"=dim."summary_trial_type"
  AND src."uid"=dim."uid"
  AND src."summary_trial_status"=dim."summary_trial_status")
	INSERT INTO "dataWareHouse"."DIM_SUMMARY_INFORMATION" (
        "summary_EudraCT_Number",
		"summary_Protocol_Number",
		"summary_trial_type",
		"uid",
		"summary_trial_status",
		"valid_from",
		"valid_to") (
		select 
		"summary_EudraCT_Number",
		"summary_Protocol_Number",
		"summary_trial_type",
		"uid",
		"summary_trial_status",
		current_timestamp,
 		null from rows_in_scope ) ON CONFLICT ("uid") DO UPDATE set "valid_to" = current_timestamp;
		 		return true;
exception
when others then
return false;
END;
$$ LANGUAGE plpgsql;