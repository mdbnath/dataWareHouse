CREATE OR REPLACE FUNCTION public.DIM_SPONSOR_INFORMATION(text) RETURNS BOOLEAN AS $$	
declare 
		o record;
		in_clause ALIAS FOR $1;
	begin		
		raise notice 'inside function,%',in_clause;
		--set search_path to in_clause;
	    PERFORM public.set_search_path(in_clause||', public');
		for o in
		(
			--Get all versions
			select 
					"uid",
					summary_EudraCT_Number,
					sponsor_name,
					sponsor_country,
					sponsor_status,
					sponsor_source,
					sponsor_organisation,
					sponsor_streetName,
					sponsor_city,
					sponsor_postalCode,
					sponsor_email,
					valid_from 
					from "SPONSOR_INFORMATION"
					order by "uid" asc, valid_from asc
					-- Just up to here...
		)
		loop
			-- If there is no such version, insert a new one
			-- Important: for the between to work, need to cast timestamp into date!
			if not exists (
					select "uid"
					from "dataWareHouse"."DIM_SPONSOR_INFORMATION" dim
					where
					o.valid_from::date between dim.valid_from and dim.valid_to)
			then
				insert into "dataWareHouse"."DIM_SPONSOR_INFORMATION"
				(	
					"uid",
					summary_EudraCT_Number,
					sponsor_name,
					sponsor_country,
					sponsor_status,
					sponsor_source,
					sponsor_organisation,
					sponsor_streetName,
					sponsor_city,
					sponsor_postalCode,
					sponsor_email,
					valid_from,
					valid_to
					) values(
							o."uid",
							o.summary_EudraCT_Number,
							o.sponsor_name,
							o.sponsor_country,
							o.sponsor_status,
							o.sponsor_source,
							o.sponsor_organisation,
							o.sponsor_streetName,
							o.sponsor_city,
							o.sponsor_postalCode,
							o.sponsor_email,
							o.valid_from ,					
							null );
end if;
		end loop;

with
-- Comparison of two consecutive versions
comparison as (
	select
		uid,
	    summary_EudraCT_Number,
		sponsor_name,
		lag(sponsor_name) over w as prev_sponsor_name,
		sponsor_country,
		lag(sponsor_country) over w as prev_sponsor_country,
		sponsor_status,
		lag(sponsor_status) over w as prev_sponsor_status,	
		sponsor_source,
		lag(sponsor_source) over w as prev_sponsor_source,
		sponsor_organisation,
		lag(sponsor_organisation) over w as prev_sponsor_organisation,
		sponsor_streetName,
		lag(sponsor_streetName) over w as prev_sponsor_streetName,
		sponsor_city,
		lag(sponsor_city) over w as prev_sponsor_city,
		sponsor_postalCode,
		lag(sponsor_postalCode) over w as prev_sponsor_postalCode,
		sponsor_email,
		lag(sponsor_email) over w as prev_sponsor_email,
		valid_from,
		lag(valid_from) over w as prev_valid_from
	from "dataWareHouse"."DIM_SPONSOR_INFORMATION"
	window w as (partition by "uid" order by valid_from)
),
-- Mark them as removable if they are the same on the SCD type II attributes
versions_to_remove as (
	select "uid", valid_from
	from comparison
	where
		sponsor_name is not distinct from prev_sponsor_name and
		sponsor_country is not distinct from prev_sponsor_country AND
		sponsor_status is not distinct from prev_sponsor_status AND		
		sponsor_source is not distinct from prev_sponsor_source and
		sponsor_organisation is not distinct from prev_sponsor_organisation AND
		sponsor_streetName is not distinct from prev_sponsor_streetName AND
		sponsor_city is not distinct from prev_sponsor_city and
		sponsor_postalCode is not distinct from prev_sponsor_postalCode AND
		sponsor_email is not distinct from prev_sponsor_email 			
)

--select count(*) from versions_to_remove
-- Effectively delete them
delete from "dataWareHouse"."DIM_SPONSOR_INFORMATION"
where ("uid",valid_from) in
(select * from versions_to_remove);

--Set version_ends
--Set version_ends
update "dataWareHouse"."DIM_SPONSOR_INFORMATION" dd
set valid_to = d.valid_to
 from
	(select
		uid,
		valid_from,
		lead(valid_from) over natural_key_partition as valid_to
	 from "dataWareHouse"."DIM_SPONSOR_INFORMATION"
	 window natural_key_partition as
		(partition by "uid" order by valid_from)
	 order by "uid", valid_from asc) d
 where d."uid" = dd."uid" and d.valid_from = dd.valid_from;
return true;
end;
$$ language plpgsql;