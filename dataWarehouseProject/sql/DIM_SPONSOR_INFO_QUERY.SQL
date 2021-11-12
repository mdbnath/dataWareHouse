
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
	"staging_15_09_2021"."SPONSOR_INFORMATION" src
left join "dataWareHouse"."DIM_SPONSOR_INFORMATION" dim
  on
	src."summary_EudraCT_Number" = dim."summary_EudraCT_Number"
	and src."sponsor_name" = dim."sponsor_name"
	and src."sponsor_country" = dim."sponsor_country"
	and src."sponsor_status" = dim."sponsor_status"
	and src."sponsor_source" = dim."sponsor_source"
	and src."sponsor_organisation" = dim."sponsor_organisation"
	and src."sponsor_streetName" = dim."sponsor_streetName"
	and src."sponsor_city" = dim."sponsor_city"
	and src."sponsor_postalCode" = dim."sponsor_postalCode"
	and src."sponsor_email" = dim."sponsor_email"
where
	dim."uid" is null)
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
	"uid") (
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
		"uid"
	from
		rows_in_scope )