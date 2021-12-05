CREATE OR REPLACE FUNCTION public.DIM_TRIAL_INFORMATION() RETURNS BOOLEAN AS $$	
declare 
		o record;
	begin		
		-- For each 
		for o in
		(
			--Get all versions
			select 
					"uid",
					summary_EudraCT_Number,
					trial_investigation ,
					trial_language,
					trial_MedDRA_classification ,
					trial_subStudy,
					trial_controlled,
					trail_randomised ,
					trial_status,
					trial_member_state,
					trial_single_blind,
					trail_double_blind,
					trial_arms,
					trial_phaseI,
					trial_phaseII,
					trial_phaseIII,
					trial_phaseIV,
					valid_from
					from "TRIAL_INFORMATION" 
					order by "uid" asc, valid_from asc
					-- Just up to here...
		)
		loop
			-- If there is no such version, insert a new one
			-- Important: for the between to work, need to cast timestamp into date!
			if not exists (
					select "uid"
					from "dataWareHouse"."DIM_TRIAL_INFORMATION" dim
					where
					o.valid_from::date between dim.valid_from and dim.valid_to)
			then
				insert into "dataWareHouse"."DIM_TRIAL_INFORMATION"
				(	
					"uid",
					summary_EudraCT_Number,
					trial_investigation,
					trial_language,
					trial_MedDRA_classification,
					trial_subStudy,
					trial_controlled,
					trail_randomised,
					trial_status,
					trial_member_state,
					trial_single_blind,
					trail_double_blind,
					trial_arms,
					trial_phaseI,
					trial_phaseII,
					trial_phaseIII,
					trial_phaseIV,
					valid_from,
					valid_to
					) values(
										o.uid,
										o.summary_EudraCT_Number,
										o.trial_investigation,
										o.trial_language,
										o.trial_MedDRA_classification,
										o.trial_subStudy,
										o.trial_controlled,
										o.trail_randomised,
										o.trial_status,
										o.trial_member_state,
										o.trial_Single_blind,
										o.trail_Double_blind,
										o.trial_arms,
										o.trial_phaseI,
										o.trial_phaseII,
										o.trial_phaseIII,
										o.trial_phaseIV,
										o.valid_from,					
										null );
			end if;
		end loop;

with
-- Comparison of two consecutive versions
comparison as (
	select
		uid,
	    summary_EudraCT_Number,
		trial_investigation,
		lag(trial_investigation) over w as prev_trial_investigation,
		trial_language,
		lag(trial_language) over w as prev_trial_language,
		trial_MedDRA_classification,
		lag(trial_MedDRA_classification) over w as prev_trial_MedDRA_classification,
		trial_subStudy,
		lag(trial_subStudy) over w as prev_trial_subStudy,
		trial_controlled,
		lag(trial_controlled) over w as prev_trial_controlled,
		trail_randomised,
		lag(trail_randomised) over w as prev_trail_randomised,
		trial_status,
		lag(trial_status) over w as prev_trial_status,
		trial_member_state,
		lag(trial_member_state) over w as prev_trial_member_state,
		trial_single_blind,
		lag(trial_single_blind) over w as prev_trial_single_blind,
		trail_double_blind,
		lag(trail_double_blind) over w as prev_trail_double_blind,
		trial_arms,
		lag(trial_arms) over w as prev_trial_arms,
		trial_phaseI,
		lag(trial_phaseI) over w as prev_trial_phaseI,
		trial_phaseII,
		lag(trial_phaseII) over w as prev_trial_phaseII,
		trial_phaseIII,
		lag(trial_phaseIII) over w as prev_trial_phaseIII,
		trial_phaseIV,
		lag(trial_phaseIV) over w as prev_trial_phaseIV,
		valid_from,
		lag(valid_from) over w as prev_valid_from
	from "dataWareHouse"."DIM_TRIAL_INFORMATION"
	window w as (partition by "uid" order by valid_from)
),
-- Mark them as removable if they are the same on the SCD type II attributes
versions_to_remove as (
	select "uid", valid_from
	from comparison
	where
		trial_investigation is not distinct from prev_trial_investigation and
		trial_language is not distinct from prev_trial_language AND
		trial_MedDRA_classification is not distinct from prev_trial_MedDRA_classification AND
		trial_subStudy is not distinct from prev_trial_subStudy AND
		trial_controlled is not distinct from prev_trial_controlled AND
		trail_randomised is not distinct from prev_trail_randomised AND
		trial_status is not distinct from prev_trial_status AND
		trial_member_state is not distinct from prev_trial_member_state AND
		trial_single_blind is not distinct from prev_trial_single_blind AND
		trail_double_blind is not distinct from prev_trail_double_blind AND
		trial_arms is not distinct from prev_trial_arms AND
		trial_phaseI is not distinct from prev_trial_phaseI AND
		trial_phaseII is not distinct from prev_trial_phaseII AND
		trial_phaseIII is not distinct from prev_trial_phaseIII AND
		trial_phaseIV is not distinct from prev_trial_phaseIV 
	
)

--select count(*) from versions_to_remove
-- Effectively delete them
delete from "dataWareHouse"."DIM_TRIAL_INFORMATION"
where ("uid",valid_from) in
(select * from versions_to_remove);

--Set version_ends
--Set version_ends
update "dataWareHouse"."DIM_TRIAL_INFORMATION" dd
set valid_to = d.valid_to
 from
	(select
		uid,
		valid_from,
		lead(valid_from) over natural_key_partition as valid_to
	 from "dataWareHouse"."DIM_TRIAL_INFORMATION"
	 window natural_key_partition as
		(partition by "uid" order by valid_from)
	 order by "uid", valid_from asc) d
 where d."uid" = dd."uid" and d.valid_from = dd.valid_from;
return true;
end;
$$ language plpgsql;