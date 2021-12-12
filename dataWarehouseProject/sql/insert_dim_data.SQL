create or replace procedure public.insert_dim_data(text)
language plpgsql
as $$
declare
 in_clause ALIAS FOR $1;
begin
--for x in 1..3 loop 
	set search_path to in_clause;
    PERFORM public.dim_protocol_information(in_clause);
	PERFORM public.DIM_IMP_IDENTIFICATION(in_clause);
	PERFORM public.DIM_SPONSOR_INFORMATION(in_clause);
	PERFORM public.DIM_SUMMARY_INFORMATION(in_clause);
	PERFORM public.dim_timedimension_info(in_clause);
    PERFORM public.DIM_TRIAL_REVIEW_INFO(in_clause);
    PERFORM public.DIM_TRIAL_SUBJECT_INFO(in_clause);
    PERFORM public.DIM_TRIAL_INFORMATION(in_clause);
--end loop;
end; $$