"""
Database initialization module.
Creates tables and partitions for RDLP wydzielenia if they don't exist.
"""
import asyncpg
from pathlib import Path
from utils.logger.logger import AsyncLogger
from config.config import Settings

logger = AsyncLogger()

RDLP_NAMES = [
    'bialystok', 'katowice', 'krakow', 'krosno', 'lublin',
    'lodz', 'olsztyn', 'pila', 'poznan', 'szczecin',
    'szczecinek', 'torun', 'wroclaw', 'zielona_gora',
    'gdansk', 'radom', 'warszawa'
]


async def init_database(connection: asyncpg.Connection):
    """
    Initialize database schema and tables for RDLP wydzielenia.
    Creates schema, tables, partitions, and indexes if they don't exist.
    
    Args:
        connection: asyncpg connection to the database
    """
    try:
        # Create schema if not exists
        await connection.execute("CREATE SCHEMA IF NOT EXISTS rdlp;")
        logger.log("INFO", "Schema 'rdlp' created or already exists.")
        
        # Enable PostGIS extension if not exists
        await connection.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
        logger.log("INFO", "PostGIS extension enabled or already exists.")
        
        # Create tables and partitions for each RDLP
        for rdlp_name in RDLP_NAMES:
            await _create_rdlp_table(connection, rdlp_name)
        
        logger.log("INFO", "Database initialization completed successfully.")
    except Exception as e:
        logger.log("ERROR", f"Failed to initialize database: {e}")
        raise


async def _create_rdlp_table(connection: asyncpg.Connection, rdlp_name: str):
    """
    Create table and partition for a specific RDLP.
    
    Args:
        connection: asyncpg connection to the database
        rdlp_name: Name of the RDLP (e.g., 'olsztyn', 'warszawa')
    """
    try:
        # Check if table already exists
        table_exists = await connection.fetchval(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'rdlp' 
                AND table_name = $1
            );
            """,
            f"{rdlp_name}_wydzielenia"
        )
        
        if table_exists:
            logger.log("INFO", f"Table rdlp.{rdlp_name}_wydzielenia already exists, skipping creation.")
            return
        
        # Create parent table
        await connection.execute(f"""
            CREATE TABLE rdlp.{rdlp_name}_wydzielenia (
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
                geometry          geometry(MultiPolygon,4326),
                load_time         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                forest_range_name VARCHAR(255) NOT NULL,
                rdlp_name         VARCHAR(255) NOT NULL,
                CONSTRAINT {rdlp_name}_wydzielenia_adr_for_key UNIQUE (adr_for, rdlp_name),
                CONSTRAINT {rdlp_name}_wydzielenia_pkey PRIMARY KEY (id, rdlp_name)
            )
            PARTITION BY LIST (rdlp_name);
        """)
        logger.log("INFO", f"Created parent table rdlp.{rdlp_name}_wydzielenia")
        
        # Create partition
        await connection.execute(f"""
            CREATE TABLE rdlp.{rdlp_name}_wydzielenia_partition
            PARTITION OF rdlp.{rdlp_name}_wydzielenia
            FOR VALUES IN ('{rdlp_name}');
        """)
        logger.log("INFO", f"Created partition rdlp.{rdlp_name}_wydzielenia_partition")
        
        # Create GIST index on geometry
        await connection.execute(f"""
            CREATE INDEX ON rdlp.{rdlp_name}_wydzielenia_partition USING GIST (geometry);
        """)
        logger.log("INFO", f"Created GIST index on geometry for rdlp.{rdlp_name}_wydzielenia_partition")
        
        # Create index on adr_for
        await connection.execute(f"""
            CREATE INDEX ON rdlp.{rdlp_name}_wydzielenia_partition (adr_for);
        """)
        logger.log("INFO", f"Created index on adr_for for rdlp.{rdlp_name}_wydzielenia_partition")
        
        # Populate PostGIS metadata for parent table
        await connection.execute(f"""
            SELECT Populate_Geometry_Columns('rdlp.{rdlp_name}_wydzielenia'::regclass);
        """)
        
        # Populate PostGIS metadata for partition
        await connection.execute(f"""
            SELECT Populate_Geometry_Columns('rdlp.{rdlp_name}_wydzielenia_partition'::regclass);
        """)
        
    except Exception as e:
        logger.log("ERROR", f"Failed to create table for {rdlp_name}: {e}")
        raise


async def ensure_database_initialized(connection: asyncpg.Connection):
    """
    Ensure database is initialized. Creates tables if they don't exist.
    This is a safe wrapper that can be called multiple times.
    
    Args:
        connection: asyncpg connection to the database
    """
    await init_database(connection)

