CREATE OR REPLACE FUNCTION public.DIM_TRIAL_SUBJECT_INFO(text) RETURNS BOOLEAN AS $$	
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
					trial_subject_Utero,
					trial_subject_Newborns,
					trial_subject_toddlers,
					trail_subject_Children,
					trail_subject_Adolescents,
					trail_subject_Adults,					
					trail_subject_Elderly,
					trial_subject_Female,
					trial_subject_male,
					trial_subject_inMemberState,
					trial_multinational,
					trial_clinical,
					valid_from 
					from "TRIAL_SUBJECT_INFO"
					order by "uid" asc, valid_from asc
					-- Just up to here...
		)
		loop
			-- If there is no such version, insert a new one
			-- Important: for the between to work, need to cast timestamp into date!
			if not exists (
					select "uid"
					from "dataWareHouse"."DIM_TRIAL_SUBJECT_INFO" dim
					where
					o.valid_from::date between dim.valid_from and dim.valid_to)
			then
				insert into "dataWareHouse"."DIM_TRIAL_SUBJECT_INFO"
				(	
					"uid",
					summary_EudraCT_Number,
					trial_subject_Utero,
					trial_subject_Newborns,
					trial_subject_toddlers,
					trail_subject_Children,
					trail_subject_Adolescents,
					trail_subject_Adults,
					trail_subject_Elderly,
					trial_subject_Female,
					trial_subject_male,
					trial_subject_inMemberState,
					trial_multinational,
					trial_clinical,
					valid_from,
					valid_to
					) values(
										o.uid,
										o.summary_EudraCT_Number,
										o.trial_subject_Utero,
										o.trial_subject_Newborns,
										o.trial_subject_toddlers,
										o.trail_subject_Children,
										o.trail_subject_Adolescents,
										o.trail_subject_Adults,
										o.trail_subject_Elderly,
										o.trial_subject_Female,
										o.trial_subject_male,
										o.trial_subject_inMemberState,
										o.trial_multinational,
										o.trial_clinical,
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
		trial_subject_Utero,
		lag(trial_subject_Utero) over w as prev_trial_subject_Utero,
		trial_subject_Newborns,
		lag(trial_subject_Newborns) over w as prev_trial_subject_Newborns,
		trial_subject_toddlers,
		lag(trial_subject_toddlers) over w as prev_trial_subject_toddlers,
		trail_subject_Children,
		lag(trail_subject_Children) over w as prev_trail_subject_Children,
		trail_subject_Adolescents,
		lag(trail_subject_Adolescents) over w as prev_trail_subject_Adolescents,
		trail_subject_Adults,
		lag(trail_subject_Adults) over w as prev_trail_subject_Adults,
		trail_subject_Elderly,
		lag(trail_subject_Elderly) over w as prev_trail_subject_Elderly,
		trial_subject_Female,
		lag(trial_subject_Female) over w as prev_trial_subject_Female,
		trial_subject_male,
		lag(trial_subject_male) over w as prev_trial_subject_male,
		trial_subject_inMemberState,
		lag(trial_subject_inMemberState) over w as prev_trial_subject_inMemberState,
		trial_multinational,
		lag(trial_multinational) over w as prev_trial_multinational,
		trial_clinical,
		lag(trial_clinical) over w as prev_trial_clinical,

		valid_from,
		lag(valid_from) over w as prev_valid_from
	from "dataWareHouse"."DIM_TRIAL_SUBJECT_INFO"
	window w as (partition by "uid" order by valid_from)
),
-- Mark them as removable if they are the same on the SCD type II attributes
versions_to_remove as (
	select "uid", valid_from
	from comparison
	where
		trial_subject_Utero is not distinct from prev_trial_subject_Utero and
		trial_subject_Newborns is not distinct from prev_trial_subject_Newborns AND
		trial_subject_toddlers is not distinct from prev_trial_subject_toddlers AND
		trail_subject_Children is not distinct from prev_trail_subject_Children AND
		trail_subject_Adolescents is not distinct from prev_trail_subject_Adolescents AND
		trail_subject_Adults is not distinct from prev_trail_subject_Adults AND
		trail_subject_Elderly is not distinct from prev_trail_subject_Elderly AND
		trial_subject_Female is not distinct from prev_trial_subject_Female AND
		trial_subject_male is not distinct from prev_trial_subject_male AND
		trial_subject_inMemberState is not distinct from prev_trial_subject_inMemberState AND
		trial_multinational is not distinct from prev_trial_multinational AND
		trial_clinical is not distinct from prev_trial_clinical	
)

--select count(*) from versions_to_remove
-- Effectively delete them
delete from "dataWareHouse"."DIM_TRIAL_SUBJECT_INFO"
where ("uid",valid_from) in
(select * from versions_to_remove);

--Set version_ends
--Set version_ends
update "dataWareHouse"."DIM_TRIAL_SUBJECT_INFO" dd
set valid_to = d.valid_to
 from
	(select
		uid,
		valid_from,
		lead(valid_from) over natural_key_partition as valid_to
	 from "dataWareHouse"."DIM_TRIAL_SUBJECT_INFO"
	 window natural_key_partition as
		(partition by "uid" order by valid_from)
	 order by "uid", valid_from asc) d
 where d."uid" = dd."uid" and d.valid_from = dd.valid_from;
return true;
end;
$$ language plpgsql;