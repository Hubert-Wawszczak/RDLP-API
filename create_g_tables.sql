-- Upewnij się, że masz włączony PostGIS w Twojej bazie:
-- CREATE EXTENSION IF NOT EXISTS postgis;

-- Na wszelki wypadek stwórz schemat rdlp, jeśli go nie ma
CREATE SCHEMA IF NOT EXISTS rdlp;

-- Uwaga: Tabele są zarządzane przez Alembic migracje
-- Ten plik służy jako dokumentacja struktury tabel
-- NIE UŻYWAJ DROP TABLE - Alembic zarządza tabelami

-- Struktura tabeli G_INSPECTORATE (granice nadleśnictw)
-- CREATE TABLE IF NOT EXISTS rdlp.G_INSPECTORATE (
--     id              BIGSERIAL PRIMARY KEY,
--     a_i_num         BIGINT,
--     adr_for         VARCHAR(255),
--     i_name          VARCHAR(255),
--     a_year          INT,
--     geometry        geometry(MultiPolygon,4326),
--     load_time       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
-- );

-- Struktura tabeli G_FOREST_RANGE (granice leśnictw)
-- CREATE TABLE IF NOT EXISTS rdlp.G_FOREST_RANGE (
--     id              BIGSERIAL PRIMARY KEY,
--     a_i_num         BIGINT,
--     adr_for         VARCHAR(255),
--     f_r_name        VARCHAR(255),
--     a_year          INT,
--     geometry        geometry(MultiPolygon,4326),
--     load_time       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
-- );

-- Struktura tabeli G_SUBAREA (wydzielenia z pełnymi danymi)
-- CREATE TABLE IF NOT EXISTS rdlp.G_SUBAREA (
--     id              BIGSERIAL PRIMARY KEY,
--     a_i_num         BIGINT,
--     adr_for         VARCHAR(255),
--     area_type       VARCHAR(50),
--     site_type       VARCHAR(50),
--     silvicult       VARCHAR(10),
--     forest_fun      VARCHAR(50),
--     stand_stru      VARCHAR(10),
--     rotat_age       INT,
--     sub_area        NUMERIC(10, 2),
--     prot_categ      VARCHAR(50),
--     species_cd      VARCHAR(10),
--     part_cd         VARCHAR(10),
--     spec_age        INT,
--     a_year          INT,
--     geometry        geometry(MultiPolygon,4326),
--     load_time       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
-- );
