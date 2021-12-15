CREATE OR REPLACE FUNCTION public.DIM_TIMEDIMENSION_INFO(text) RETURNS BOOLEAN AS $$	
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
					tt."uid",
					tt.summary_EudraCT_Number,
					tt.summary_trial_dateEntered,
					tt.trial_review_authority_date,
					tt.trail_review_ethics_date,
					tt.valid_from,
					ee.trial_end_date,
					ee.publication_date
					from "TIME_DIMENSION" tt 
					left join "TRIAL_ENDPOINTS" ee on TRIM(tt.summary_eudract_number) = TRIM(ee.eudract_Number)
					order by "uid" asc, valid_from asc
				
		)
		loop
			-- If there is no such version, insert a new one
			-- Important: for the between to work, need to cast timestamp into date!
			if not exists (
					select "uid"
					from "dataWareHouse"."DIM_TIMEDIMENSION_INFO" dim
					where
					o.valid_from::date between dim.valid_from and dim.valid_to)
			then
				insert into "dataWareHouse"."DIM_TIMEDIMENSION_INFO"
				(	
					"uid",
					summary_EudraCT_Number,
					summary_trial_dateEntered,	
					trial_review_authority_date,
					trail_review_ethics_date,
					trial_end_date,
					publication_date,
					valid_from,
					valid_to
					) values(
							o.uid,
							o.summary_EudraCT_Number,
							o.summary_trial_dateEntered,	
							o.trial_review_authority_date,
							o.trail_review_ethics_date,
							o.trial_end_date,
							o.publication_date,
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
		summary_trial_dateEntered,
		lag(summary_trial_dateEntered) over w as prev_summary_trial_dateEntered,
		trial_review_authority_date,
		lag(trial_review_authority_date) over w as prev_trial_review_authority_date,
		trail_review_ethics_date,
		lag(trail_review_ethics_date) over w as prev_trail_review_ethics_date,	
		publication_date,
		lag(publication_date) over w as prev_publication_date,
		trial_end_date,
		lag(trial_end_date) over w as prev_trial_end_date,	
		valid_from,
		lag(valid_from) over w as prev_valid_from
	from "dataWareHouse"."DIM_TIMEDIMENSION_INFO"
	window w as (partition by "uid" order by valid_from)
),
-- Mark them as removable if they are the same on the SCD type II attributes
versions_to_remove as (
	select "uid", valid_from
	from comparison
	where
		summary_trial_dateEntered is not distinct from prev_summary_trial_dateEntered and
		trial_review_authority_date is not distinct from prev_trial_review_authority_date AND
		trail_review_ethics_date is not distinct from prev_trail_review_ethics_date AND
		trial_end_date is not distinct from prev_trial_end_date AND
		publication_date is not distinct from prev_publication_date 
		)

--select count(*) from versions_to_remove
-- Effectively delete them
delete from "dataWareHouse"."DIM_TIMEDIMENSION_INFO"
where ("uid",valid_from) in
(select * from versions_to_remove);

--Set version_ends
--Set version_ends
update "dataWareHouse"."DIM_TIMEDIMENSION_INFO" dd
set valid_to = d.valid_to
 from
	(select
		uid,
		valid_from,
		lead(valid_from) over natural_key_partition as valid_to
	 from "dataWareHouse"."DIM_TIMEDIMENSION_INFO"
	 window natural_key_partition as
		(partition by "uid" order by valid_from)
	 order by "uid", valid_from asc) d
 where d."uid" = dd."uid" and d.valid_from = dd.valid_from;
return true;
end;
$$ language plpgsql;