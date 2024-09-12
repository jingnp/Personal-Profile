DO $$ 
declare
	r RECORD;
	column_name TEXT;
	query TEXT; 
BEGIN 
	FOR r IN 
		SELECT cc.column_name 
		FROM information_schema.columns cc
		WHERE table_name = 'cl_clexpf' and table_schema = 'L_TH_DATACACHE'
	LOOP 
		column_name := r.column_name; 
		query := 'SELECT COUNT(*) FROM "L_TH_DATACACHE"."cl_clexpf" WHERE ' || 
				quote_ident(column_name) || '::text  ~ ''[^a-zA-Z0-9]'''; 
		EXECUTE query INTO r; RAISE NOTICE 'Column: %: % :rows contain special characters', 
column_name, r.count; 
	END LOOP; 
END $$;