CREATE OR REPLACE FUNCTION public.FACT_SPONSOR_STUDIES() RETURNS BOOLEAN AS $$	
 declare 
		o record;
	begin		
		for o in(
			select
			a.id as summary_id,
			b.id as trial_id,
			c.id as sponsor_id,
			d.id as date_id,
			e.id as protocol_id
		    from "dataWareHouse"."DIM_SUMMARY_INFORMATION" a 
			left join "dataWareHouse"."DIM_TRIAL_INFORMATION" b on a.uid=b.uid
			left join "dataWareHouse"."DIM_SPONSOR_INFORMATION" c on a.uid=c.uid
			left join "dataWareHouse"."DIM_TIMEDIMENSION_INFO" d on  a.uid=d.uid
			left join "dataWareHouse"."DIM_PROTOCOL_INFORMATION" e on  a.uid=e.uid
			where b.valid_to is null and
			c.valid_to is null and
			d.valid_to is null and 
			e.valid_to is null)
		loop
		insert into "dataWareHouse"."FACT_SPONSOR_STUDIES"
				(	
					Trial_EudraCT_Number,
					trial_info,
					trial_sponsor,
					protocol_info,
					dates				
					) values(
						o.summary_id,
						o.trial_id,	
						o.sponsor_id,
						o.protocol_id,
						o.date_id );
		end loop;
return true;
end;
$$ language plpgsql;