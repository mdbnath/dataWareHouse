
CREATE OR REPLACE FUNCTION public.DIM_TRIAL_REVIEW_INFO()
RETURNS BOOLEAN AS $$
-- SCD Type 1 attributes
begin
	insert into "dataWareHouse"."DIM_TRIAL_REVIEW_INFO" 
		select
			src."uid",
			src."summary_EudraCT_Number",
			src."trial_review_Authority",	
			src."trial_review_ethics_Committee",
			src."trial_review_ethics_committeReason"		
from
		"TRIAL_REVIEW_INFO" src where uid = src.uid  
	 ON CONFLICT ("uid") 
	 DO update 
	 set
 		"summary_EudraCT_Number" = EXCLUDED."summary_EudraCT_Number" ,
		"trial_review_Authority" = EXCLUDED."trial_review_Authority" ,
		"trial_review_ethics_Committee" = EXCLUDED."trial_review_ethics_Committee" ,
		"trial_review_ethics_committeReason" = EXCLUDED."trial_review_ethics_committeReason";
return true;
exception
when others then
return false;
END;
$$ LANGUAGE plpgsql;