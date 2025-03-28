-- Upewnij się, że masz włączony PostGIS w Twojej bazie:
-- CREATE EXTENSION IF NOT EXISTS postgis;

-- Na wszelki wypadek stwórz schemat rdlp, jeśli go nie ma
CREATE SCHEMA IF NOT EXISTS rdlp;

DO $$
DECLARE
    rdlp_names TEXT[] := ARRAY[
        'bialystok', 'katowice', 'krakow', 'krosno', 'lublin',
        'lodz', 'olsztyn', 'pila', 'poznan', 'szczecin',
        'szczecinek', 'torun', 'wroclaw', 'zielona_gora',
        'gdansk', 'radom', 'warszawa'
    ];
    rdlp_name TEXT;
BEGIN
    FOREACH rdlp_name IN ARRAY rdlp_names LOOP
        -- 1. Usuń tabelę-rodzica (jeśli istnieje)
        EXECUTE format(
            'DROP TABLE IF EXISTS rdlp.%I_wydzielenia CASCADE;',
            rdlp_name
        );

        -- 2. Tworzenie tabeli rodzica z partycjonowaniem + geometry(MultiPolygon,4326)
        EXECUTE format($sql$
            CREATE TABLE rdlp.%I_wydzielenia (
                id                BIGINT  NOT NULL,
                area_type         VARCHAR(50),
                a_i_num           BIGINT,
                silvicult         VARCHAR(10),
                stand_stru        VARCHAR(10),
                sub_area          NUMERIC(10, 2),
                species_cd        VARCHAR(10),
                spec_age          INT,
                nazwa             VARCHAR(255),
                adr_for           VARCHAR(255) NOT NULL,
                site_type         VARCHAR(50),
                forest_fun        VARCHAR(50),
                rotat_age         INT,
                prot_categ        VARCHAR(50),
                part_cd           VARCHAR(10),
                a_year            INT,

                -- kluczowa deklaracja z SRID=4326
                geometry          geometry(MultiPolygon,4326),

                load_time         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                forest_range_name VARCHAR(255) NOT NULL,
                rdlp_name         VARCHAR(255) NOT NULL,

                CONSTRAINT %I_wydzielenia_adr_for_key UNIQUE (adr_for, rdlp_name), -- tego nie zmieniamy
                CONSTRAINT %I_wydzielenia_pkey        PRIMARY KEY (id, rdlp_name) -- tego nie zmieniamy
            )
            PARTITION BY LIST (rdlp_name); -- partycjonowanie po nazwie rdlp nie dotykac
        $sql$
        , rdlp_name        -- %I_wydzielenia
        , rdlp_name        -- %I_wydzielenia_adr_for_key
        , rdlp_name        -- %I_wydzielenia_pkey
        );

        -- 3. Partycja
        EXECUTE format($sql$
            CREATE TABLE rdlp.%I_wydzielenia_partition
            PARTITION OF rdlp.%I_wydzielenia
            FOR VALUES IN (%L);
        $sql$
        , rdlp_name
        , rdlp_name
        , rdlp_name
        );

        -- 4. Indeks GIST na geometrii w partycji
        EXECUTE format($sql$
            CREATE INDEX ON rdlp.%I_wydzielenia_partition USING GIST (geometry);
        $sql$,
            rdlp_name
        );

        -- 5. Indeks na kolumnie adr_for
        EXECUTE format($sql$
            CREATE INDEX ON rdlp.%I_wydzielenia_partition (adr_for);
        $sql$,
            rdlp_name
        );

        -- 6. Populacja metadanych PostGIS (rodzic)
        EXECUTE format($sql$
            SELECT Populate_Geometry_Columns('rdlp.%I_wydzielenia'::regclass)
        $sql$,
            rdlp_name
        );

        -- 7. Populacja metadanych PostGIS (partycja)
        EXECUTE format($sql$
            SELECT Populate_Geometry_Columns('rdlp.%I_wydzielenia_partition'::regclass)
        $sql$,
            rdlp_name
        );

    END LOOP;
END $$;