CREATE OR REPLACE FUNCTION create_timescaledb_catalog_publication(
        publication_name text,
        replication_user text
)
    RETURNS bool
    LANGUAGE plpgsql
    SECURITY DEFINER
AS
$$
DECLARE
    found bool;
    owner oid;
BEGIN
    SELECT true, pubowner
    FROM pg_catalog.pg_publication
    WHERE pubname = publication_name
    INTO found, owner;

    IF found THEN
        SELECT true
        FROM pg_catalog.pg_publication_tables
        WHERE pubname = publication_name
          AND schemaname = '_timescaledb_catalog'
          AND tablename = 'hypertable'
        INTO found;

        IF NOT found THEN
            RAISE EXCEPTION
                'Publication % already exists but is missing _timescaledb_catalog.hypertable', publication_name;
        END IF;

        SELECT true
        FROM pg_catalog.pg_publication_tables
        WHERE pubname = publication_name
          AND schemaname = '_timescaledb_catalog'
          AND tablename = 'chunk'
        INTO found;

        IF NOT found THEN
            RAISE EXCEPTION
                'Publication % already exists but is missing _timescaledb_catalog.chunk', publication_name;
        END IF;

        SELECT true
        FROM (SELECT session_user as uid) s
        WHERE s.uid = owner
        INTO found;

        IF NOT found THEN
            RAISE EXCEPTION
                'Publication % already exists but is not owned by the session user', publication_name;
        END IF;

        RETURN true;
    END IF;

    EXECUTE
        format(
            'CREATE PUBLICATION %I FOR TABLE _timescaledb_catalog.chunk, _timescaledb_catalog.hypertable',
            publication_name
        );
    EXECUTE format('ALTER PUBLICATION %I OWNER TO %s', publication_name, replication_user);
    RETURN true;
END;
$$;
