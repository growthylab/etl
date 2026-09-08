drop event trigger if exists supabase_etl_00_all_tables_identity_guard;

drop function if exists etl.enforce_all_tables_publication_identity();
drop function if exists etl.assert_all_tables_publications_use_one_dimensional_arrays();
drop function if exists etl.assert_all_tables_publications_disable_row_security();
drop function if exists etl.assert_all_tables_publications_have_usable_identity();
drop function if exists etl.assert_all_tables_publications_publish_all_changes();
