CREATE OR REPLACE FUNCTION public.dim_protocol_information(text) RETURNS BOOLEAN AS $$

declare 
	 o record;
	 in_clause ALIAS FOR $1;
	
		begin	
		raise notice 'inside function,%',in_clause;
		--set search_path to in_clause;
	    PERFORM public.set_search_path(in_clause||', public');
		for o in
		(
			select 
					uid,
					summary_EudraCT_Number,
					protocol_member_state,
					protocol_EudraCT_Number,
					protocol_trial_fullTitle,
					protocol_code_number,
					valid_from 
					from "PROTOCOL_INFORMATION"
					order by uid asc, valid_from asc
					-- Just up to here...
		)
		loop
			-- If there is no such version, insert a new one
			-- Important: for the between to work, need to cast timestamp into date!
			if not exists (
					select uid
					from "dataWareHouse"."DIM_PROTOCOL_INFORMATION" dim
					where
					o.valid_from::date between dim.valid_from and dim.valid_to)
			then
				insert into "dataWareHouse"."DIM_PROTOCOL_INFORMATION"
				(	
					uid,
					summary_EudraCT_Number,
					protocol_member_state,
					protocol_EudraCT_Number,
					protocol_trial_fullTitle,
					protocol_code_number,
					valid_from,
					valid_to
					) values(
							o.uid,
							o.summary_EudraCT_Number,
							o.protocol_member_state,
							o.protocol_EudraCT_Number,
							o.protocol_trial_fullTitle,
							o.protocol_code_number,
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
		protocol_member_state,
		lag(protocol_member_state) over w as prev_protocol_member_state,
		protocol_EudraCT_Number,
		lag(protocol_EudraCT_Number) over w as prev_protocol_EudraCT_Number,
		protocol_trial_fullTitle,
		lag(protocol_trial_fullTitle) over w as prev_protocol_trial_fullTitle,	
		protocol_code_number,
		lag(protocol_code_number) over w as prev_protocol_code_number,
		valid_from,
		lag(valid_from) over w as prev_valid_from
	from "dataWareHouse"."DIM_PROTOCOL_INFORMATION"
	window w as (partition by uid order by valid_from)
),
-- Mark them as removable if they are the same on the SCD type II attributes
versions_to_remove as (
	select uid, valid_from
	from comparison
	where
		protocol_member_state is not distinct from prev_protocol_member_state and
		protocol_EudraCT_Number is not distinct from prev_protocol_EudraCT_Number AND
		protocol_trial_fullTitle is not distinct from prev_protocol_trial_fullTitle AND		
		protocol_code_number is not distinct from prev_protocol_code_number 
		)

--select count(*) from versions_to_remove
-- Effectively delete them
delete from "dataWareHouse"."DIM_PROTOCOL_INFORMATION"
where (uid,valid_from) in
(select * from versions_to_remove);

--Set version_ends
--Set version_ends
update "dataWareHouse"."DIM_PROTOCOL_INFORMATION" dd
set valid_to = d.valid_to
 from
	(select
		uid,
		valid_from,
		lead(valid_from) over natural_key_partition as valid_to
	 from "dataWareHouse"."DIM_PROTOCOL_INFORMATION"
	 window natural_key_partition as
		(partition by uid order by valid_from)
	 order by uid, valid_from asc) d
 where d.uid = dd.uid and d.valid_from = dd.valid_from;
return true;
end;
$$ language plpgsql;