CREATE OR REPLACE FUNCTION public.DIM_SUMMARY_INFORMATION(text) RETURNS BOOLEAN AS $$	
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
					summary_Protocol_Number,
					summary_trial_type,
					summary_trial_status,
					valid_from 
					from "SUMMARY_INFORMATION"
					order by "uid" asc, valid_from asc
					-- Just up to here...
		)
		loop
			-- If there is no such version, insert a new one
			-- Important: for the between to work, need to cast timestamp into date!
			if not exists (
					select "uid"
					from "dataWareHouse"."DIM_SUMMARY_INFORMATION" dim
					where
					o.valid_from::date between dim.valid_from and dim.valid_to)
			then
				insert into "dataWareHouse"."DIM_SUMMARY_INFORMATION"
				(	
					"uid",
					summary_EudraCT_Number,
					summary_Protocol_Number,
					summary_trial_type,
					summary_trial_status,
					valid_from,
					valid_to
					) values(
							o.uid,
							o.summary_EudraCT_Number,
							o.summary_Protocol_Number,
							o.summary_trial_type,
							o.summary_trial_status,
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
		summary_Protocol_Number,
		lag(summary_Protocol_Number) over w as prev_summary_Protocol_Number,
		summary_trial_type,
		lag(summary_trial_type) over w as prev_summary_trial_type,
		summary_trial_status,
		lag(summary_trial_status) over w as prev_summary_trial_status,		
		valid_from,
		lag(valid_from) over w as prev_valid_from
	from "dataWareHouse"."DIM_SUMMARY_INFORMATION"
	window w as (partition by "uid" order by valid_from)
),
-- Mark them as removable if they are the same on the SCD type II attributes
versions_to_remove as (
	select "uid", valid_from
	from comparison
	where
		summary_Protocol_Number is not distinct from prev_summary_Protocol_Number and
		summary_trial_type is not distinct from prev_summary_trial_type AND
		summary_trial_status is not distinct from prev_summary_trial_status 		
)

--select count(*) from versions_to_remove
-- Effectively delete them
delete from "dataWareHouse"."DIM_SUMMARY_INFORMATION"
where ("uid",valid_from) in
(select * from versions_to_remove);

--Set version_ends
--Set version_ends
update "dataWareHouse"."DIM_SUMMARY_INFORMATION" dd
set valid_to = d.valid_to
 from
	(select
		uid,
		valid_from,
		lead(valid_from) over natural_key_partition as valid_to
	 from "dataWareHouse"."DIM_SUMMARY_INFORMATION"
	 window natural_key_partition as
		(partition by "uid" order by valid_from)
	 order by "uid", valid_from asc) d
 where d."uid" = dd."uid" and d.valid_from = dd.valid_from;
return true;
end;
$$ language plpgsql;