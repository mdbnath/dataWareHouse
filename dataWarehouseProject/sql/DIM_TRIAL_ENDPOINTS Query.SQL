CREATE OR REPLACE FUNCTION public.DIM_TRIAL_ENDPOINTS(text) RETURNS BOOLEAN AS $$	
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
					
					eudract_number,
					trial_protocol,	
					trial_end_date,
					publication_date,
					total_medicine,
					total_placebo,
					mortality_medicine,
					mortality_placebo ,
					icu_medicine,
					icu_placebo,
					ventilation_medicine,
					ventilation_placebo,
					valid_from 
					from "TRAIL_ENDPOINTS"
					order by eudract_number asc, valid_from asc
					-- Just up to here...
		)
		loop
			-- If there is no such version, insert a new one
			-- Important: for the between to work, need to cast timestamp into date!
			if not exists (
					select eudract_number
					from "dataWareHouse"."DIM_TRAIL_ENDPOINTS" dim
					where
					o.valid_from::date between dim.valid_from and dim.valid_to)
			then
				insert into "dataWareHouse"."DIM_TRAIL_ENDPOINTS"
				(	
					eudract_number,
					trial_protocol,	
					trial_end_date,
					publication_date,
					total_medicine,
					total_placebo,
					mortality_medicine,
					mortality_placebo ,
					icu_medicine,
					icu_placebo,
					ventilation_medicine,
					ventilation_placebo,
					valid_from,
					valid_to
					) values(
							o.eudract_number,
							o.trial_protocol,	
							o.trial_end_date,
							o.publication_date,
							o.total_medicine,
							o.total_placebo,
							o.mortality_medicine,
							o.mortality_placebo ,
							o.icu_medicine,
							o.icu_placebo,
							o.ventilation_medicine,
							o.ventilation_placebo,
							o.valid_from ,					
							null );
			end if;
		end loop;

with
-- Comparison of two consecutive versions
comparison as (
	select
		
	    eudract_number,
		trial_protocol,
		lag(trial_protocol) over w as prev_trial_protocol,
		trial_end_date,
		lag(trial_end_date) over w as prev_trial_end_date,
		publication_date,
		lag(publication_date) over w as prev_publication_date,
		total_medicine,
		lag(total_medicine) over w as prev_total_medicine,	
		total_placebo,
		lag(total_placebo) over w as prev_total_placebo,
		mortality_medicine,
		lag(mortality_medicine) over w as prev_mortality_medicine,	
		mortality_placebo,
		lag(mortality_placebo) over w as prev_mortality_placebo,
		icu_medicine,
		lag(icu_medicine) over w as prev_icu_medicine,
		icu_placebo,
		lag(icu_placebo) over w as prev_icu_placebo,
		ventilation_medicine,
		lag(ventilation_medicine) over w as prev_ventilation_medicine,	
		ventilation_placebo,
		lag(ventilation_placebo) over w as prev_ventilation_placebo,			
		valid_from,
		lag(valid_from) over w as prev_valid_from
	from "dataWareHouse"."DIM_TRAIL_ENDPOINTS"
	window w as (partition by eudract_number order by valid_from)
),
-- Mark them as removable if they are the same on the SCD type II attributes
versions_to_remove as (
	select eudract_number, valid_from
	from comparison
	where
		trial_protocol is not distinct from prev_trial_protocol and
		trial_end_date is not distinct from prev_trial_end_date AND
		publication_date is not distinct from prev_publication_date AND		
		total_medicine is not distinct from prev_total_medicine and
		total_placebo is not distinct from prev_total_placebo and
		mortality_medicine is not distinct from prev_mortality_medicine AND
		mortality_placebo is not distinct from prev_mortality_placebo AND		
		icu_medicine is not distinct from prev_icu_medicine AND
		icu_placebo is not distinct from prev_icu_placebo AND
		ventilation_medicine is not distinct from prev_ventilation_medicine AND
		ventilation_placebo is not distinct from prev_ventilation_placebo 
		)

--select count(*) from versions_to_remove
-- Effectively delete them
delete from "dataWareHouse"."DIM_TRAIL_ENDPOINTS"
where (eudract_number,valid_from) in
(select * from versions_to_remove);

--Set version_ends
--Set version_ends
update "dataWareHouse"."DIM_TRAIL_ENDPOINTS" dd
set valid_to = d.valid_to
 from
	(select
		eudract_number,
		valid_from,
		lead(valid_from) over natural_key_partition as valid_to
	 from "dataWareHouse"."DIM_TRAIL_ENDPOINTS"
	 window natural_key_partition as
		(partition by eudract_number order by valid_from)
	 order by eudract_number, valid_from asc) d
 where d.eudract_number = dd.eudract_number and d.valid_from = dd.valid_from;
return true;
end;
$$ language plpgsql;