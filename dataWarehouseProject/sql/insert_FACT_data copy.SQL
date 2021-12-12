create or replace procedure public.insert_fact_data()
language plpgsql
as $$
begin
    PERFORM public.FACT_TRIAL_STUDIES();
	PERFORM public.FACT_SPONSOR_STUDIES();
end; $$