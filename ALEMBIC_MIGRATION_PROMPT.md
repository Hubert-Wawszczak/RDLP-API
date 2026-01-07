# Prompty dla Backendu i RDLP-API - Migracja Alembic dla tabel RDLP wydzielenia

---

# PROMPT 1: Dla Backendu (Alembic Migrations)

## Kontekst
Backend powinien inicjalizować tabele dla RDLP wydzielenia używając Alembic. Aplikacja RDLP-API będzie wstawiać dane do tych tabel, ale nie powinna ich tworzyć - to jest odpowiedzialność backendu.

## Wymagania do implementacji

### 1. Schemat bazy danych
- **Schemat**: `rdlp`
- **Rozszerzenie**: PostGIS musi być włączone (`CREATE EXTENSION IF NOT EXISTS postgis;`)

### 2. Struktura tabeli dla każdego RDLP

Dla każdego RDLP należy utworzyć partycjonowaną tabelę z następującą strukturą:

#### Lista wszystkich RDLP:
```python
RDLP_NAMES = [
    'bialystok', 'katowice', 'krakow', 'krosno', 'lublin',
    'lodz', 'olsztyn', 'pila', 'poznan', 'szczecin',
    'szczecinek', 'torun', 'wroclaw', 'zielona_gora',
    'gdansk', 'radom', 'warszawa'
]
```

#### Struktura tabeli głównej (partycjonowanej):
```sql
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
    geometry          geometry(MultiPolygon,4326),  -- PostGIS geometry
    load_time         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    forest_range_name VARCHAR(255) NOT NULL,
    rdlp_name         VARCHAR(255) NOT NULL,
    
    CONSTRAINT {rdlp_name}_wydzielenia_adr_for_key UNIQUE (adr_for, rdlp_name),
    CONSTRAINT {rdlp_name}_wydzielenia_pkey PRIMARY KEY (id, rdlp_name)
)
PARTITION BY LIST (rdlp_name);
```

#### Partycja dla każdego RDLP:
```sql
CREATE TABLE rdlp.{rdlp_name}_wydzielenia_partition
PARTITION OF rdlp.{rdlp_name}_wydzielenia
FOR VALUES IN ('{rdlp_name}');
```

#### Indeksy:
```sql
-- Indeks przestrzenny GIST na geometrii
CREATE INDEX ON rdlp.{rdlp_name}_wydzielenia_partition USING GIST (geometry);

-- Indeks B-tree na adr_for
CREATE INDEX ON rdlp.{rdlp_name}_wydzielenia_partition (adr_for);
```

#### PostGIS metadata:
```sql
-- Populacja metadanych PostGIS dla tabeli głównej
SELECT Populate_Geometry_Columns('rdlp.{rdlp_name}_wydzielenia'::regclass);

-- Populacja metadanych PostGIS dla partycji
SELECT Populate_Geometry_Columns('rdlp.{rdlp_name}_wydzielenia_partition'::regclass);
```

### 3. Mapowanie na modele SQLAlchemy

#### Model dla wydzielenia:
```python
from sqlalchemy import Column, BigInteger, String, Numeric, Integer, TIMESTAMP, UniqueConstraint, PrimaryKeyConstraint
from sqlalchemy.dialects.postgresql import TIMESTAMP
from geoalchemy2 import Geometry
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func

Base = declarative_base()

class Wydzielenie(Base):
    __tablename__ = '{rdlp_name}_wydzielenia'
    __table_args__ = (
        UniqueConstraint('adr_for', 'rdlp_name', name=f'{rdlp_name}_wydzielenia_adr_for_key'),
        PrimaryKeyConstraint('id', 'rdlp_name', name=f'{rdlp_name}_wydzielenia_pkey'),
        {'schema': 'rdlp'},
        {'postgresql_partition_by': 'LIST (rdlp_name)'}
    )
    
    id = Column(BigInteger, primary_key=True)
    area_type = Column(String(50), nullable=True)
    a_i_num = Column(BigInteger, nullable=True)
    silvicult = Column(String(10), nullable=True)
    stand_stru = Column(String(10), nullable=True)
    sub_area = Column(Numeric(10, 2), nullable=True)
    species_cd = Column(String(10), nullable=True)
    spec_age = Column(Integer, nullable=True)
    nazwa = Column(String(255), nullable=True)
    adr_for = Column(String(255), nullable=False)
    site_type = Column(String(50), nullable=True)
    forest_fun = Column(String(50), nullable=True)
    rotat_age = Column(Integer, nullable=True)
    prot_categ = Column(String(50), nullable=True)
    part_cd = Column(String(10), nullable=True)
    a_year = Column(Integer, nullable=True)
    geometry = Column(Geometry('MULTIPOLYGON', srid=4326), nullable=True)
    load_time = Column(TIMESTAMP, server_default=func.current_timestamp())
    forest_range_name = Column(String(255), nullable=False)
    rdlp_name = Column(String(255), nullable=False)
```

### 4. Migracja Alembic - Przykładowa struktura

#### Krok 1: Utworzenie schematu i rozszerzenia
```python
from alembic import op
import sqlalchemy as sa

def upgrade():
    # Utworzenie schematu
    op.execute("CREATE SCHEMA IF NOT EXISTS rdlp;")
    
    # Włączenie PostGIS
    op.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
```

#### Krok 2: Utworzenie tabel dla wszystkich RDLP
```python
RDLP_NAMES = [
    'bialystok', 'katowice', 'krakow', 'krosno', 'lublin',
    'lodz', 'olsztyn', 'pila', 'poznan', 'szczecin',
    'szczecinek', 'torun', 'wroclaw', 'zielona_gora',
    'gdansk', 'radom', 'warszawa'
]

def upgrade():
    # ... schemat i PostGIS ...
    
    for rdlp_name in RDLP_NAMES:
        # Utworzenie tabeli głównej (partycjonowanej)
        op.execute(f"""
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
        
        # Utworzenie partycji
        op.execute(f"""
            CREATE TABLE rdlp.{rdlp_name}_wydzielenia_partition
            PARTITION OF rdlp.{rdlp_name}_wydzielenia
            FOR VALUES IN ('{rdlp_name}');
        """)
        
        # Indeksy
        op.execute(f"""
            CREATE INDEX ON rdlp.{rdlp_name}_wydzielenia_partition USING GIST (geometry);
        """)
        op.execute(f"""
            CREATE INDEX ON rdlp.{rdlp_name}_wydzielenia_partition (adr_for);
        """)
        
        # PostGIS metadata
        op.execute(f"""
            SELECT Populate_Geometry_Columns('rdlp.{rdlp_name}_wydzielenia'::regclass);
        """)
        op.execute(f"""
            SELECT Populate_Geometry_Columns('rdlp.{rdlp_name}_wydzielenia_partition'::regclass);
        """)
```

### 5. Ważne uwagi

1. **Partycjonowanie**: Tabele są partycjonowane po kolumnie `rdlp_name` używając `PARTITION BY LIST`
2. **PostGIS**: Wymagane rozszerzenie PostGIS z geometrią `MultiPolygon` w SRID 4326 (WGS84)
3. **Klucze**: 
   - PRIMARY KEY: `(id, rdlp_name)`
   - UNIQUE: `(adr_for, rdlp_name)`
4. **Indeksy**: 
   - GIST na `geometry` (dla zapytań przestrzennych)
   - B-tree na `adr_for` (dla szybkiego wyszukiwania)
5. **Idempotentność**: Migracja powinna być bezpieczna do wielokrotnego uruchomienia (użyj `IF NOT EXISTS` gdzie możliwe)

### 6. Rollback (downgrade)

```python
def downgrade():
    RDLP_NAMES = [
        'bialystok', 'katowice', 'krakow', 'krosno', 'lublin',
        'lodz', 'olsztyn', 'pila', 'poznan', 'szczecin',
        'szczecinek', 'torun', 'wroclaw', 'zielona_gora',
        'gdansk', 'radom', 'warszawa'
    ]
    
    for rdlp_name in RDLP_NAMES:
        op.execute(f"DROP TABLE IF EXISTS rdlp.{rdlp_name}_wydzielenia CASCADE;")
    
    # Opcjonalnie: usunięcie schematu (jeśli nie używany przez inne tabele)
    # op.execute("DROP SCHEMA IF EXISTS rdlp CASCADE;")
```

### 7. Zgodność z backendem

Tabele muszą być zgodne z modelem `WydzielenieResponse` w backendzie. Wszystkie pola są opcjonalne oprócz:
- `id` (wymagane)
- `adr_for` (wymagane)
- `forest_range_name` (wymagane)
- `rdlp_name` (wymagane)

### 8. Testowanie

Po utworzeniu migracji, sprawdź:
1. Czy wszystkie tabele zostały utworzone
2. Czy partycje działają poprawnie
3. Czy indeksy są utworzone
4. Czy PostGIS metadata jest poprawnie wypełniona
5. Czy można wstawiać dane do tabel (INSERT testowy)

#### Przykładowe zapytanie testowe:
```sql
-- Sprawdzenie czy tabele istnieją
SELECT table_name 
FROM information_schema.tables 
WHERE table_schema = 'rdlp' 
AND table_name LIKE '%_wydzielenia%';

-- Sprawdzenie partycji
SELECT * FROM pg_partitions 
WHERE schemaname = 'rdlp';

-- Test INSERT
INSERT INTO rdlp.olsztyn_wydzielenia (
    id, adr_for, forest_range_name, rdlp_name, geometry
) VALUES (
    1, '01-01-001-001', 'Test Range', 'olsztyn',
    ST_GeomFromText('MULTIPOLYGON(((0 0, 1 0, 1 1, 0 1, 0 0)))', 4326)
);
```

---

# PROMPT 2: Dla RDLP-API (Przygotowanie do pracy z tabelami)

## Kontekst
Aplikacja RDLP-API wstawia dane do tabel utworzonych przez backend przez Alembic. RDLP-API **NIE** powinno tworzyć tabel - zakłada, że tabele już istnieją.

## Wymagania i założenia

### 1. Zależności

RDLP-API zakłada, że:
- ✅ Backend uruchomił migracje Alembic przed startem RDLP-API
- ✅ Wszystkie tabele w schemacie `rdlp` są już utworzone
- ✅ PostGIS extension jest włączone
- ✅ Wszystkie indeksy są utworzone

### 2. Struktura danych wstawianych przez RDLP-API

RDLP-API wstawia dane zgodnie z modelem `RDLPData` (Pydantic):

```python
class RDLPData(BaseModel):
    id: int                                    # Wymagane
    area_type: Optional[str] = None
    a_i_num: Optional[int] = None
    silvicult: Optional[str] = None
    stand_stru: Optional[str] = None
    sub_area: Optional[float] = None
    species_cd: Optional[str] = None
    spec_age: Optional[int] = None
    nazwa: Optional[str] = None
    adr_for: str                               # Wymagane
    site_type: Optional[str] = None
    forest_fun: Optional[str] = None
    rotat_age: Optional[int] = None
    prot_categ: Optional[str] = None
    part_cd: Optional[str] = None
    a_year: Optional[int] = None
    geometry: Optional[Any] = None             # GeoJSON MultiPolygon
    forest_range_name: str                     # Wymagane
    rdlp_name: str                             # Wymagane
```

### 3. SQL INSERT używany przez RDLP-API

RDLP-API wstawia dane do **tabeli głównej** (nie do partycji bezpośrednio):

```sql
INSERT INTO rdlp.{rdlp_name}_wydzielenia (
    id, area_type, a_i_num, silvicult, stand_stru, sub_area,
    species_cd, spec_age, nazwa, adr_for, site_type, forest_fun,
    rotat_age, prot_categ, part_cd, a_year, geometry,
    forest_range_name, rdlp_name
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
    ST_GeomFromText($17, 4326), $18, $19
)
ON CONFLICT (adr_for, rdlp_name) DO NOTHING;
```

**Ważne**: PostgreSQL automatycznie przekieruje INSERT do odpowiedniej partycji na podstawie wartości `rdlp_name`.

**Uwaga**: RDLP-API używa `executemany()` dla batch insertów, co jest wydajniejsze niż pojedyncze INSERTy.

### 4. Konwersja geometrii

RDLP-API konwertuje geometrię z GeoJSON na WKT przed wstawieniem:

```python
# GeoJSON → WKT
if item.get('geometry') is not None:
    if isinstance(item['geometry'], dict):
        try:
            geom = shapely.geometry.shape(item['geometry'])
            item['geometry'] = shapely.wkt.dumps(geom)  # WKT format
        except Exception as e:
            logger.log("ERROR", f"Failed to convert geometry to WKT: {e}")
            item['geometry'] = None  # Ustawia None jeśli konwersja się nie powiedzie
    elif isinstance(item['geometry'], str):
        # Already in WKT format, keep as is
        pass
    else:
        # Unknown format, set to None
        item['geometry'] = None
```

Następnie używa `ST_GeomFromText()` w SQL:
```sql
ST_GeomFromText($17, 4326)  -- WKT string, SRID 4326
```

**Obsługa błędów konwersji:**
- Jeśli konwersja GeoJSON → WKT się nie powiedzie, geometria jest ustawiana na `None`
- Błąd jest logowany, ale nie przerywa przetwarzania pozostałych rekordów
- Rekordy bez geometrii są nadal wstawiane (geometria jest opcjonalna)

### 5. Mapowanie RDLP names

RDLP-API mapuje nazwy RDLP z danych źródłowych na dwa sposoby:

#### A. Mapowanie z nazw plików API (stary format):
```python
__ENDPOINTS_DICT = {
    'RDLP_Bialystok_wydzielenia': 'bialystok',
    'RDLP_Katowice_wydzielenia': 'katowice',
    'RDLP_Krakow_wydzielenia': 'krakow',
    'RDLP_Krosno_wydzielenia': 'krosno',
    'RDLP_Lublin_wydzielenia': 'lublin',
    'RDLP_Lodz_wydzielenia': 'lodz',
    'RDLP_Olsztyn_wydzielenia': 'olsztyn',
    'RDLP_Pila_wydzielenia': 'pila',
    'RDLP_Poznan_wydzielenia': 'poznan',
    'RDLP_Szczecin_wydzielenia': 'szczecin',
    'RDLP_Szczecinek_wydzielenia': 'szczecinek',
    'RDLP_Torun_wydzielenia': 'torun',
    'RDLP_Wroclaw_wydzielenia': 'wroclaw',
    'RDLP_Zielona_Gora_wydzielenia': 'zielona_gora',
    'RDLP_Gdansk_wydzielenia': 'gdansk',
    'RDLP_Radom_wydzielenia': 'radom',
    'RDLP_Warszawa_wydzielenia': 'warszawa'
}
```

#### B. Mapowanie z kodów BDL (nowy format - pliki ZIP):
RDLP-API wyciąga kod regionu z nazwy pliku ZIP (wzorzec `BDL_XX_XX`) i mapuje na nazwę RDLP:

```python
BDL_REGION_TO_RDLP = {
    '01': 'bialystok',
    '02': 'katowice',
    '03': 'krakow',
    '04': 'krosno',
    '05': 'lublin',
    '06': 'lodz',
    '07': 'olsztyn',
    '08': 'pila',
    '09': 'poznan',
    '10': 'szczecin',
    '11': 'szczecinek',
    '12': 'torun',
    '13': 'wroclaw',
    '14': 'zielona_gora',
    '15': 'gdansk',
    '16': 'radom',
    '17': 'warszawa'
}
```

**Przykłady mapowania:**
- `BDL_01_01_AUGUSTOW_2025.zip` → `bialystok`
- `BDL_07_04_BOLEWICE_2025.zip` → `olsztyn`
- `BDL_17_01_WARSZAWA_2025.zip` → `warszawa`

**Logika ekstrakcji:**
1. Jeśli plik jest w katalogu `extracted/`, szuka wzorca `BDL_(\d+)_` w ścieżce
2. Wyciąga kod regionu (np. `01`, `07`, `17`)
3. Mapuje kod na nazwę RDLP używając słownika powyżej
4. Jeśli nie znajdzie kodu BDL, próbuje wyciągnąć `rdlp_name` z properties danych

**Fallback:**
Jeśli nie można wyciągnąć nazwy RDLP z nazwy pliku, RDLP-API próbuje użyć wartości z `properties.rdlp_name` w danych GeoJSON. Jeśli to też nie zadziała, rekord jest pomijany (logowany jako błąd).

### 6. Filtrowanie plików

RDLP-API przetwarza **tylko pliki G_COMPARTMENT** (wydzielenia). Pomija:
- `G_SUBAREA` - podpowierzchnie (subareas)
- `G_FOREST_RANGE` - lesnictwa (forest ranges)
- `G_INSPECTORATE` - inspektoraty (inspectorates)

**Logika filtrowania:**
```python
def __should_process_file(self, file_path: Path) -> bool:
    filename = file_path.name.upper()
    
    # Process G_COMPARTMENT files (wydzielenia/compartments)
    if 'G_COMPARTMENT' in filename:
        return True
    
    # Process old API format files (RDLP_*_wydzielenia)
    if '_wydzielenia' in filename:
        return True
    
    # Skip all other G_ prefixed files
    if filename.startswith('G_'):
        return False
    
    # Skip SUBAREA files
    if 'SUBAREA' in filename:
        return False
    
    # For old API JSON files, process them
    if filename.endswith('.JSON') or filename.endswith('.GEOJSON'):
        return True
    
    return False
```

### 7. Obsługa błędów

RDLP-API powinno obsługiwać następujące błędy:

#### Tabela nie istnieje:
```python
asyncpg.exceptions.UndefinedTableError: relation "rdlp.{rdlp_name}_wydzielenia" does not exist
```
**Rozwiązanie**: Upewnij się, że backend uruchomił migracje Alembic przed startem RDLP-API.

#### PostGIS nie jest włączone:
```python
asyncpg.exceptions.UndefinedFunctionError: function st_geomfromtext(unknown, integer) does not exist
```
**Rozwiązanie**: Upewnij się, że PostGIS extension jest włączone w bazie danych.

#### Nieprawidłowa geometria:
```python
asyncpg.exceptions.DataError: invalid geometry
```
**Rozwiązanie**: Sprawdź format geometrii (musi być poprawny WKT MultiPolygon).

#### Brak wymaganego pola:
```python
pydantic.ValidationError: field required (id, adr_for, forest_range_name, rdlp_name)
```
**Rozwiązanie**: Sprawdź dane źródłowe - wszystkie wymagane pola muszą być obecne.

### 8. Weryfikacja przed startem

Przed uruchomieniem RDLP-API, sprawdź:

```sql
-- 1. Czy schemat istnieje
SELECT EXISTS (
    SELECT FROM information_schema.schemata 
    WHERE schema_name = 'rdlp'
);

-- 2. Czy PostGIS jest włączone
SELECT EXISTS (
    SELECT FROM pg_extension 
    WHERE extname = 'postgis'
);

-- 3. Czy tabele istnieją
SELECT table_name 
FROM information_schema.tables 
WHERE table_schema = 'rdlp' 
AND table_name LIKE '%_wydzielenia'
ORDER BY table_name;

-- Powinno zwrócić 17 tabel (dla każdego RDLP)
```

### 9. Przykładowy kod wstawiania danych

```python
# services/loader.py - insert_data()

async with self.pool.acquire() as conn:
    for table_name, items in table_groups.items():
        # table_name = "{rdlp_name}_wydzielenia"
        
        # Filter out load_time if present (not in backend schema)
        columns = [col for col in items[0].keys() if col != 'load_time']
        
        # Build column names (geometry is special)
        col_names = ', '.join([f'"{col}"' if col != 'geometry' else '"geometry"' 
                              for col in columns])
        
        # Build placeholders (geometry uses ST_GeomFromText)
        placeholders = ', '.join([
            f'${i+1}' if col != 'geometry' else f'ST_GeomFromText(${i+1}, 4326)' 
            for i, col in enumerate(columns)
        ])
        
        # Build SQL with ON CONFLICT
        sql = (f'INSERT INTO rdlp.{table_name} ({col_names}) VALUES ({placeholders})'
               f'ON CONFLICT ("adr_for", "rdlp_name") DO NOTHING')
        
        # Prepare rows as tuples
        rows = [tuple(item.get(col) for col in columns) for item in items]
        
        # Execute batch insert
        await conn.executemany(sql, rows)
        logger.log("INFO", f"Inserted {len(rows)} records into rdlp.{table_name}.")
```

**Kluczowe szczegóły:**
- `load_time` jest pomijane (nie jest w schemacie backendu)
- Geometria jest konwertowana na WKT przed wstawieniem
- Używa `executemany()` dla wydajności
- `ON CONFLICT DO NOTHING` zapobiega duplikatom

### 10. Konfiguracja Docker Compose

Upewnij się, że w `docker-compose.prod.yml` kolejność startu jest poprawna:

```yaml
services:
  db:
    image: postgis/postgis:16-3.4
    # PostgreSQL z PostGIS
    
  backend:
    depends_on:
      db:
        condition: service_healthy
    # Backend uruchamia Alembic migrations przy starcie
    
  rdlp_api:
    depends_on:
      db:
        condition: service_healthy
      # UWAGA: Jeśli backend nie jest w tym samym docker-compose,
      # upewnij się, że backend uruchomił migracje przed startem rdlp_api
    # RDLP-API zakłada, że tabele już istnieją
```

**Ważne**: Jeśli backend i RDLP-API są w różnych docker-compose, upewnij się, że:
1. Backend uruchamia się pierwszy
2. Migracje Alembic są wykonane przed startem RDLP-API
3. Oba serwisy używają tej samej bazy danych

### 11. Checklist przed wdrożeniem

- [ ] Backend ma migrację Alembic dla tabel RDLP wydzielenia
- [ ] Migracja tworzy wszystkie 17 tabel (dla każdego RDLP)
- [ ] Migracja tworzy partycje dla każdej tabeli
- [ ] Migracja tworzy indeksy (GIST na geometry, B-tree na adr_for)
- [ ] PostGIS extension jest włączone
- [ ] Schemat `rdlp` istnieje
- [ ] Backend uruchamia migracje przed startem RDLP-API
- [ ] RDLP-API nie próbuje tworzyć tabel (usunięto `db/init_db.py` z workflow)
- [ ] Test INSERT działa poprawnie
- [ ] Konwersja GeoJSON → WKT działa poprawnie
- [ ] Filtrowanie plików działa (tylko G_COMPARTMENT)

### 12. Troubleshooting

#### Problem: "relation does not exist"
**Przyczyna**: Backend nie uruchomił migracji lub migracja nie została wykonana poprawnie.
**Rozwiązanie**: 
1. Sprawdź logi backendu - czy migracje zostały uruchomione
2. Sprawdź bazę danych - czy tabele istnieją: `SELECT table_name FROM information_schema.tables WHERE table_schema = 'rdlp'`
3. Uruchom migracje ręcznie: `alembic upgrade head`

#### Problem: "ON CONFLICT does not support unique constraint"
**Przyczyna**: Constraint `(adr_for, rdlp_name)` nie istnieje w tabeli.
**Rozwiązanie**: Upewnij się, że migracja tworzy constraint `{rdlp_name}_wydzielenia_adr_for_key`.

#### Problem: "invalid geometry"
**Przyczyna**: Geometria nie jest poprawnym MultiPolygon lub ma zły SRID.
**Rozwiązanie**: 
1. Sprawdź konwersję GeoJSON → WKT w `services/loader.py`
2. Sprawdź czy geometria jest typu `MultiPolygon`
3. Sprawdź czy SRID jest 4326

#### Problem: "Failed to convert geometry to WKT"
**Przyczyna**: GeoJSON geometry jest nieprawidłowa lub nie jest MultiPolygon.
**Rozwiązanie**: 
1. Sprawdź format geometrii w danych źródłowych
2. Sprawdź logi - błąd jest logowany, ale przetwarzanie kontynuowane
3. Rekordy z nieprawidłową geometrią są wstawiane z `geometry = NULL`

#### Problem: "Could not extract RDLP name"
**Przyczyna**: Nie można wyciągnąć nazwy RDLP z nazwy pliku lub properties.
**Rozwiązanie**: 
1. Sprawdź nazwę pliku - czy zawiera wzorzec `BDL_XX_XX` lub `RDLP_*_wydzielenia`
2. Sprawdź czy `properties.rdlp_name` jest ustawione w danych
3. Sprawdź mapowanie BDL → RDLP (kod regionu 01-17)

---

## Podsumowanie

### Backend (Alembic):
- ✅ Tworzy schemat `rdlp`
- ✅ Włącza PostGIS extension
- ✅ Tworzy 17 tabel (dla każdego RDLP)
- ✅ Tworzy partycje i indeksy
- ✅ Uruchamia się przed RDLP-API

### RDLP-API:
- ✅ Zakłada, że tabele już istnieją
- ✅ Wstawia dane do tabel głównych (PostgreSQL automatycznie kieruje do partycji)
- ✅ Konwertuje GeoJSON → WKT przed wstawieniem
- ✅ Używa `ON CONFLICT DO NOTHING` dla duplikatów
- ✅ Nie tworzy tabel (to odpowiedzialność backendu)
- ✅ Filtruje pliki (tylko G_COMPARTMENT)
- ✅ Obsługuje błędy konwersji geometrii gracefully

