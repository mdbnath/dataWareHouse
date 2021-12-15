CREATE OR REPLACE FUNCTION public.FACT_TRIAL_STUDIES() RETURNS BOOLEAN AS $$	
 declare 
		o record;
	begin		
		for o in(
			select
			a.id as summary_id,
			b.id as trial_id,
			c.id as subject_id,
			d.id as date_id,
			e.total_medicine,	
			e.mortality_medicine ,
			e.mortality_placebo,
			e.total_placebo,
			e.icu_medicine,
			e.icu_placebo ,
			e.ventilation_medicine,
			e.ventilation_placebo  from "dataWareHouse"."DIM_SUMMARY_INFORMATION" a 
			left join "dataWareHouse"."DIM_TRIAL_INFORMATION" b on a.uid=b.uid
			left join "dataWareHouse"."DIM_TRIAL_SUBJECT_INFO" c on a.uid=c.uid
			left join "dataWareHouse"."DIM_TIMEDIMENSION_INFO" d on  a.uid=d.uid
			left join "dataWareHouse"."DIM_TRIAL_ENDPOINTS" e on  TRIM(a.summary_eudract_number)=TRIM(e.eudract_number)
			where b.valid_to is null and
			c.valid_to is null and
			d.valid_to is null)
		loop
		insert into "dataWareHouse"."FACT_TRIAL_STUDIES"
				(	
					Trial_EudraCT_Number,
					trial_info,
					trial_subject_details,
					dates,
					total_medicine,	
					mortality_medicine ,
					mortality_placebo,
					total_placebo,
					icu_medicine,
					icu_placebo ,
					ventilation_medicine,
					ventilation_placebo
									
					) values(
						o.summary_id,
						o.trial_id,
						o.subject_id,
						o.date_id,
						o.total_medicine,	
						o.mortality_medicine ,
						o.mortality_placebo,
						o.total_placebo,
						o.icu_medicine,
						o.icu_placebo ,
						o.ventilation_medicine,
						o.ventilation_placebo );
		end loop;
return true;
end;
$$ language plpgsql;

