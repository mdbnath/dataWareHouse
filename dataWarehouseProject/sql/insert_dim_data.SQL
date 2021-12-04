create or replace procedure insert_dim_data(text)
language plpgsql
as $$
declare
 in_clause ALIAS FOR $1;
begin
--for x in 1..3 loop 
	set search_path to in_clause;
    PERFORM public.protocol_information();
	PERFORM public.DIM_IMP_IDENTIFICATION();
	PERFORM public.DIM_SPONSOR_INFORMATION();
	PERFORM public.DIM_SUMMARY_INFORMATION();
	PERFORM public.dim_timedimension_info();
    PERFORM public.DIM_TRIAL_REVIEW_INFO();
    PERFORM public.DIM_TRIAL_SUBJECT_INFO();
    PERFORM public.DIM_TRIAL_INFORMATION();
--end loop;
end; $$