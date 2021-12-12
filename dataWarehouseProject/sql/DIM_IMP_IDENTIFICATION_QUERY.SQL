CREATE OR REPLACE FUNCTION public.DIM_IMP_IDENTIFICATION(text) RETURNS BOOLEAN AS $$	
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
					imp_role,	
					imp_marketing_authorisation,
					imp_Trade_name,
					imp_marketing_authorisation_holder,
					imp_orphan_drug,
					imp_orphan_drug_number,
					imp_productName ,
					imp_productCode,
					valid_from 
					from "IMP_IDENTIFICATION"
					order by "uid" asc, valid_from asc
					-- Just up to here...
		)
		loop
			-- If there is no such version, insert a new one
			-- Important: for the between to work, need to cast timestamp into date!
			if not exists (
					select "uid"
					from "dataWareHouse"."DIM_IMP_IDENTIFICATION" dim
					where
					o.valid_from::date between dim.valid_from and dim.valid_to)
			then
				insert into "dataWareHouse"."DIM_IMP_IDENTIFICATION"
				(	
					"uid",
					summary_EudraCT_Number,
					imp_role,	
					imp_marketing_authorisation,
					imp_Trade_name,
					imp_marketing_authorisation_holder,
					imp_orphan_drug,
					imp_orphan_drug_number,
					imp_productName ,
					imp_productCode,
					valid_from,
					valid_to
					) values(
							o.uid,
							o.summary_EudraCT_Number,
							o.imp_role,	
							o.imp_marketing_authorisation,
							o.imp_Trade_name,
							o.imp_marketing_authorisation_holder,
							o.imp_orphan_drug,
							o.imp_orphan_drug_number,
							o.imp_productName ,
							o.imp_productCode,
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
		imp_role,
		lag(imp_role) over w as prev_imp_role,
		imp_marketing_authorisation,
		lag(imp_marketing_authorisation) over w as prev_imp_marketing_authorisation,
		imp_Trade_name,
		lag(imp_Trade_name) over w as prev_imp_Trade_name,	
		imp_marketing_authorisation_holder,
		lag(imp_marketing_authorisation_holder) over w as prev_imp_marketing_authorisation_holder,
		imp_orphan_drug,
		lag(imp_orphan_drug) over w as prev_imp_orphan_drug,	
		imp_orphan_drug_number,
		lag(imp_orphan_drug_number) over w as prev_imp_orphan_drug_number,
		imp_productName,
		lag(imp_productName) over w as prev_imp_productName,
		imp_productCode,
		lag(imp_productCode) over w as prev_imp_productCode,		
		valid_from,
		lag(valid_from) over w as prev_valid_from
	from "dataWareHouse"."DIM_IMP_IDENTIFICATION"
	window w as (partition by "uid" order by valid_from)
),
-- Mark them as removable if they are the same on the SCD type II attributes
versions_to_remove as (
	select "uid", valid_from
	from comparison
	where
		imp_role is not distinct from prev_imp_role and
		imp_marketing_authorisation is not distinct from prev_imp_marketing_authorisation AND
		imp_Trade_name is not distinct from prev_imp_Trade_name AND		
		imp_marketing_authorisation_holder is not distinct from prev_imp_marketing_authorisation_holder and
		imp_orphan_drug is not distinct from prev_imp_orphan_drug and
		imp_orphan_drug_number is not distinct from prev_imp_orphan_drug_number AND
		imp_productName is not distinct from prev_imp_productName AND		
		imp_productCode is not distinct from prev_imp_productCode 
		)

--select count(*) from versions_to_remove
-- Effectively delete them
delete from "dataWareHouse"."DIM_IMP_IDENTIFICATION"
where ("uid",valid_from) in
(select * from versions_to_remove);

--Set version_ends
--Set version_ends
update "dataWareHouse"."DIM_IMP_IDENTIFICATION" dd
set valid_to = d.valid_to
 from
	(select
		uid,
		valid_from,
		lead(valid_from) over natural_key_partition as valid_to
	 from "dataWareHouse"."DIM_IMP_IDENTIFICATION"
	 window natural_key_partition as
		(partition by "uid" order by valid_from)
	 order by "uid", valid_from asc) d
 where d."uid" = dd."uid" and d.valid_from = dd.valid_from;
return true;
end;
$$ language plpgsql;