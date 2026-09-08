-- Fail closed when a FOR ALL TABLES publication contains a relation whose
-- row identity cannot deterministically address one destination row.
--
-- The guard deliberately rejects REPLICA IDENTITY FULL even though Postgres
-- can publish it. A full old tuple is not a key when duplicate rows exist, so
-- applying UPDATE or DELETE downstream could affect more than one row.

-- The replicator is configured for a complete row-change stream. PostgreSQL
-- permits publication owners to disable individual operations after creation;
-- doing so would make source and destination diverge without an apply error.
create function etl.assert_all_tables_publications_publish_all_changes()
returns void
language plpgsql
security definer
set search_path = pg_catalog
as
$fnc$
declare
    r record;
begin
    for r in
        select
            p.pubname,
            p.pubinsert,
            p.pubupdate,
            p.pubdelete,
            p.pubtruncate
        from pg_catalog.pg_publication p
        where p.puballtables
          and not (
              p.pubinsert
              and p.pubupdate
              and p.pubdelete
              and p.pubtruncate
          )
        order by p.pubname
    loop
        raise exception using
            errcode = '55000',
            message = pg_catalog.format(
                'FOR ALL TABLES publication %I must publish INSERT, UPDATE, DELETE, and TRUNCATE',
                r.pubname
            ),
            detail = pg_catalog.format(
                'Publication flags are pubinsert=%s, pubupdate=%s, pubdelete=%s, pubtruncate=%s. Disabling any operation would silently diverge the destination.',
                r.pubinsert,
                r.pubupdate,
                r.pubdelete,
                r.pubtruncate
            ),
            hint = pg_catalog.format(
                'Run ALTER PUBLICATION %I SET (publish = ''insert, update, delete, truncate'').',
                r.pubname
            );
    end loop;
end;
$fnc$;

revoke all on function etl.assert_all_tables_publications_publish_all_changes() from public;

comment on function etl.assert_all_tables_publications_publish_all_changes() is
$$Raises SQLSTATE 55000 when any FOR ALL TABLES publication does not publish
INSERT, UPDATE, DELETE, and TRUNCATE. A partial operation set would allow the
source and destination to diverge silently.$$;

create function etl.assert_all_tables_publications_have_usable_identity()
returns void
language plpgsql
security definer
set search_path = pg_catalog
as
$fnc$
declare
    r record;
begin
    for r in
        with effective_tables as (
            select distinct
                p.pubname,
                published.relid
            from pg_catalog.pg_publication p
            cross join lateral pg_catalog.pg_get_publication_tables(p.pubname) published
            where p.puballtables
        ),
        table_identity as (
            select
                e.pubname,
                c.oid,
                n.nspname,
                c.relname,
                c.relreplident,
                case c.relreplident
                    when 'd' then exists (
                        select 1
                        from pg_catalog.pg_index i
                        where i.indrelid = c.oid
                          and i.indisprimary
                          and i.indisunique
                          and i.indisvalid
                          and i.indisready
                          and i.indislive
                          and i.indpred is null
                          and i.indexprs is null
                          and i.indnkeyatts > 0
                          and not exists (
                              select 1
                              from unnest(i.indkey::pg_catalog.int2[]) with ordinality
                                  as key_column(attnum, position)
                              left join pg_catalog.pg_attribute a
                                on a.attrelid = c.oid
                               and a.attnum = key_column.attnum
                              where key_column.position <= i.indnkeyatts
                                and (
                                    key_column.attnum <= 0
                                    or a.attnum is null
                                    or not a.attnotnull
                                    or a.attgenerated <> ''
                                )
                          )
                    )
                    when 'i' then exists (
                        select 1
                        from pg_catalog.pg_index i
                        where i.indrelid = c.oid
                          and i.indisreplident
                          and i.indisunique
                          and i.indisvalid
                          and i.indisready
                          and i.indislive
                          and i.indpred is null
                          and i.indexprs is null
                          and i.indnkeyatts > 0
                          and not exists (
                              select 1
                              from unnest(i.indkey::pg_catalog.int2[]) with ordinality
                                  as key_column(attnum, position)
                              left join pg_catalog.pg_attribute a
                                on a.attrelid = c.oid
                               and a.attnum = key_column.attnum
                              where key_column.position <= i.indnkeyatts
                                and (
                                    key_column.attnum <= 0
                                    or a.attnum is null
                                    or not a.attnotnull
                                    or a.attgenerated <> ''
                                )
                          )
                    )
                    else false
                end as usable
            from effective_tables e
            join pg_catalog.pg_class c
              on c.oid = e.relid
            join pg_catalog.pg_namespace n
              on n.oid = c.relnamespace
            where c.relkind in ('r', 'p')
              and c.relpersistence = 'p'
        )
        select
            t.pubname,
            t.oid,
            t.nspname,
            t.relname,
            t.relreplident
        from table_identity t
        where not t.usable
        order by t.pubname, t.nspname, t.relname, t.oid
    loop
        raise exception using
            errcode = '55000',
            message = pg_catalog.format(
                'FOR ALL TABLES publication %I cannot include table %I.%I without a deterministic replica identity',
                r.pubname,
                r.nspname,
                r.relname
            ),
            detail = pg_catalog.format(
                'Relation OID %s has relreplident=%s. Modes NONE and FULL are rejected; DEFAULT requires a valid primary-key index, and INDEX requires a valid unique, non-partial replica-identity index whose key columns are NOT NULL and non-generated.',
                r.oid,
                r.relreplident
            ),
            hint = 'Create new published tables with an inline PRIMARY KEY. Repair existing tables before creating the FOR ALL TABLES publication.';
    end loop;
end;
$fnc$;

revoke all on function etl.assert_all_tables_publications_have_usable_identity() from public;

comment on function etl.assert_all_tables_publications_have_usable_identity() is
$$Raises SQLSTATE 55000 when the effective table set of any FOR ALL TABLES
publication contains a permanent base or partitioned table without a
deterministic key-based replica identity. Uses pg_get_publication_tables so
partition roots and leaves follow each publication's publish_via_partition_root
setting exactly.$$;

-- Initial COPY runs ordinary SELECT statements as the replicator reader. Row
-- security could therefore produce a successful but incomplete snapshot,
-- while later logical changes follow different visibility rules. Refuse that
-- ambiguous source contract instead of silently copying only policy-visible
-- rows.
create function etl.assert_all_tables_publications_disable_row_security()
returns void
language plpgsql
security definer
set search_path = pg_catalog
as
$fnc$
declare
    r record;
begin
    for r in
        select distinct
            p.pubname,
            c.oid,
            n.nspname,
            c.relname,
            c.relrowsecurity,
            c.relforcerowsecurity
        from pg_catalog.pg_publication p
        cross join lateral pg_catalog.pg_get_publication_tables(p.pubname) published
        join pg_catalog.pg_class c
          on c.oid = published.relid
        join pg_catalog.pg_namespace n
          on n.oid = c.relnamespace
        where p.puballtables
          and c.relkind in ('r', 'p')
          and c.relpersistence = 'p'
          and (c.relrowsecurity or c.relforcerowsecurity)
        order by p.pubname, n.nspname, c.relname, c.oid
    loop
        raise exception using
            errcode = '55000',
            message = pg_catalog.format(
                'FOR ALL TABLES publication %I cannot include row-security table %I.%I',
                r.pubname,
                r.nspname,
                r.relname
            ),
            detail = pg_catalog.format(
                'Relation OID %s has relrowsecurity=%s and relforcerowsecurity=%s. Initial COPY uses ordinary SELECT and could otherwise produce a policy-filtered snapshot.',
                r.oid,
                r.relrowsecurity,
                r.relforcerowsecurity
            ),
            hint = 'Disable and unforce row-level security before including the table in a FOR ALL TABLES publication.';
    end loop;
end;
$fnc$;

revoke all on function etl.assert_all_tables_publications_disable_row_security() from public;

comment on function etl.assert_all_tables_publications_disable_row_security() is
$$Raises SQLSTATE 55000 when the effective table set of any FOR ALL TABLES
publication contains a permanent base or partitioned table with row-level
security enabled or forced. This prevents a policy-filtered initial snapshot
from diverging silently from logical replication.$$;

-- The ETL value model supports only one-dimensional arrays. PostgreSQL does
-- not enforce declared dimensions at write time, so this structural guard is
-- paired with a rollout-time scan of existing array_ndims(column) values.
create function etl.assert_all_tables_publications_use_one_dimensional_arrays()
returns void
language plpgsql
security definer
set search_path = pg_catalog
as
$fnc$
declare
    r record;
begin
    for r in
        select distinct
            p.pubname,
            c.oid,
            n.nspname,
            c.relname,
            a.attname,
            a.attndims,
            pg_catalog.format_type(a.atttypid, a.atttypmod) as formatted_type
        from pg_catalog.pg_publication p
        cross join lateral pg_catalog.pg_get_publication_tables(p.pubname) published
        join pg_catalog.pg_class c
          on c.oid = published.relid
        join pg_catalog.pg_namespace n
          on n.oid = c.relnamespace
        join pg_catalog.pg_attribute a
          on a.attrelid = c.oid
         and a.attnum > 0
         and not a.attisdropped
        where p.puballtables
          and c.relkind in ('r', 'p')
          and c.relpersistence = 'p'
          and a.attndims > 1
        order by p.pubname, n.nspname, c.relname, a.attname, c.oid
    loop
        raise exception using
            errcode = '55000',
            message = pg_catalog.format(
                'FOR ALL TABLES publication %I cannot include declared multidimensional array %I.%I.%I',
                r.pubname,
                r.nspname,
                r.relname,
                r.attname
            ),
            detail = pg_catalog.format(
                'Relation OID %s column type %s has attndims=%s. Multidimensional arrays are not replicated by this ETL contract.',
                r.oid,
                r.formatted_type,
                r.attndims
            ),
            hint = 'Remove the multidimensional declaration before including the table. Readiness must also scan existing array_ndims(column) values because PostgreSQL does not enforce declared dimensions.';
    end loop;
end;
$fnc$;

revoke all on function etl.assert_all_tables_publications_use_one_dimensional_arrays() from public;

comment on function etl.assert_all_tables_publications_use_one_dimensional_arrays() is
$$Raises SQLSTATE 55000 when an effective table in any FOR ALL TABLES
publication declares a multidimensional array. Existing array values still
require the separate rollout-time array_ndims scan because PostgreSQL array
declarations do not constrain stored dimensions.$$;

-- Validate publications that already exist when this migration is installed.
select etl.assert_all_tables_publications_publish_all_changes();
select etl.assert_all_tables_publications_have_usable_identity();
select etl.assert_all_tables_publications_disable_row_security();
select etl.assert_all_tables_publications_use_one_dimensional_arrays();

create function etl.enforce_all_tables_publication_identity()
returns pg_catalog.event_trigger
language plpgsql
security definer
set search_path = pg_catalog
as
$fnc$
begin
    perform etl.assert_all_tables_publications_publish_all_changes();
    perform etl.assert_all_tables_publications_have_usable_identity();
    perform etl.assert_all_tables_publications_disable_row_security();
    perform etl.assert_all_tables_publications_use_one_dimensional_arrays();
end;
$fnc$;

revoke all on function etl.enforce_all_tables_publication_identity() from public;

comment on function etl.enforce_all_tables_publication_identity() is
$$DDL guard for complete row-change capture, deterministic downstream UPDATE
and DELETE application, and complete initial-copy visibility.

The check runs after every statement that can create a published table, change
its identity, row-security mode, or declared array shape, install an extension
that owns ordinary tables, change a partition hierarchy, or create/change a
FOR ALL TABLES publication. Because
ddl_command_end is statement-scoped, a future published table must declare its
PRIMARY KEY inline in CREATE TABLE; creating the table and adding its key in a
later statement, even in one transaction, is rejected.$$;

-- Event triggers with the same event are executed in name order. The `00`
-- prefix makes the invariant fail before the logical DDL message trigger does
-- its more expensive snapshot work.
create event trigger supabase_etl_00_all_tables_identity_guard
    on ddl_command_end
    when tag in (
        'ALTER INDEX',
        'ALTER PUBLICATION',
        'ALTER TABLE',
        'CREATE EXTENSION',
        'CREATE PUBLICATION',
        'CREATE SCHEMA',
        'CREATE TABLE',
        'CREATE TABLE AS',
        'DROP INDEX',
        'SELECT INTO'
    )
    execute function etl.enforce_all_tables_publication_identity();
