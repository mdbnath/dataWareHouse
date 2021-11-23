CREATE OR REPLACE FUNCTION public.DIM_SPONSOR_INFORMATION()
RETURNS BOOLEAN AS $$
begin
with rows_in_scope as (
select
	src."summary_EudraCT_Number",
		src."sponsor_name",
		src."sponsor_country",
		src."sponsor_status",
		src."sponsor_source",
		src."sponsor_organisation",
		src."sponsor_streetName",
		src."sponsor_city",
		src."sponsor_postalCode",
		src."sponsor_email",
		src."uid"
from
	"SPONSOR_INFORMATION" src
left join "dataWareHouse"."DIM_SPONSOR_INFORMATION" dim
  on
	src."uid"=dim."uid"
	and src."summary_EudraCT_Number" = dim."summary_EudraCT_Number"
	and src."sponsor_name" = dim."sponsor_name"
	and src."sponsor_country" = dim."sponsor_country"
	and src."sponsor_status" = dim."sponsor_status"
	and src."sponsor_source" = dim."sponsor_source"
	and src."sponsor_organisation" = dim."sponsor_organisation"
	and src."sponsor_streetName" = dim."sponsor_streetName"
	and src."sponsor_city" = dim."sponsor_city"
	and src."sponsor_postalCode" = dim."sponsor_postalCode"
	and src."sponsor_email" = dim."sponsor_email")
insert
	into
	"dataWareHouse"."DIM_SPONSOR_INFORMATION" ("summary_EudraCT_Number",
	"sponsor_name",
	"sponsor_country",
	"sponsor_status",
	"sponsor_source",
	"sponsor_organisation",
	"sponsor_streetName",
	"sponsor_city",
	"sponsor_postalCode",
	"sponsor_email",
	"uid",
	"valid_from",
	"valid_to") (
	select
		"summary_EudraCT_Number",
		"sponsor_name",
		"sponsor_country",
		"sponsor_status",
		"sponsor_source",
		"sponsor_organisation",
		"sponsor_streetName",
		"sponsor_city",
		"sponsor_postalCode",
		"sponsor_email",
		"uid",
		current_timestamp,
 		null from rows_in_scope ) ON CONFLICT ("uid") DO UPDATE set "valid_to" = current_timestamp;	
		return true;
exception
when others then
return false;
END;
$$ LANGUAGE plpgsql;