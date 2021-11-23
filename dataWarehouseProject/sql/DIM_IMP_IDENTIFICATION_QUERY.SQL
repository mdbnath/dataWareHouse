CREATE OR REPLACE FUNCTION public.DIM_IMP_IDENTIFICATION()
RETURNS BOOLEAN AS $$
begin
 with rows_in_scope_New as (
select
	src."summary_EudraCT_Number",
	src."imp_role",	
	src."imp_marketing_authorisation",
	src."imp_Trade_name",
	src."imp_marketing_authorisation_holder",
	src."imp_orphan_drug",
	src."imp_orphan_drug_number",
	src."imp_productName" ,
	src."imp_productCode" ,
	src."uid"
from
	"IMP_IDENTIFICATION" src
left join "dataWareHouse"."DIM_IMP_IDENTIFICATION" dim
  on
	src."uid"=dim."uid"
	and src."summary_EudraCT_Number" = dim."summary_EudraCT_Number"
	and src."imp_role" = dim."imp_role"
	and src."imp_marketing_authorisation" = dim."imp_marketing_authorisation"
	and src."imp_Trade_name" = dim."imp_Trade_name"
	and src."imp_marketing_authorisation_holder" = dim."imp_marketing_authorisation_holder"
	and src."imp_orphan_drug" = dim."imp_orphan_drug"
	and src."imp_orphan_drug_number" = dim."imp_orphan_drug_number"
	and src."imp_productName" = dim."imp_productName"
	and src."imp_productCode" = dim."imp_productCode"
	)
insert
	into
	"dataWareHouse"."DIM_IMP_IDENTIFICATION" ("summary_EudraCT_Number",
	"imp_role",	
	"imp_marketing_authorisation",
	"imp_Trade_name",
	"imp_marketing_authorisation_holder",
	"imp_orphan_drug",
	"imp_orphan_drug_number",
	"imp_productName",
	"imp_productCode",
	"uid",
	"valid_from",
	"valid_to" ) (
	select
		"summary_EudraCT_Number",
		"imp_role",
		"imp_marketing_authorisation",
		"imp_Trade_name",
		"imp_marketing_authorisation_holder",
		"imp_orphan_drug",
		"imp_orphan_drug_number",
		"imp_productName",
		"imp_productCode",
		"uid",
		current_timestamp,
 		null from rows_in_scope_New ) ON CONFLICT ("uid") DO UPDATE set "valid_to" = current_timestamp;	
		 return true;
exception
when others then
return false;
END;
$$ LANGUAGE plpgsql;