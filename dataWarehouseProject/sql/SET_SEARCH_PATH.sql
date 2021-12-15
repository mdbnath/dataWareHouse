CREATE OR REPLACE FUNCTION public.set_search_path(path TEXT, is_local BOOLEAN DEFAULT false) RETURNS TEXT AS $$
    SELECT set_config('search_path', regexp_replace(path, '[^\w ,]', '', 'g'), is_local);
$$ LANGUAGE sql;
