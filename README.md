# RDLP-API

API do ładowania danych RDLP (Regionalne Dyrekcje Lasów Państwowych) z ogcapi.bdl.lasy.gov.pl do bazy danych PostgreSQL z PostGIS.

## Funkcjonalności

- Pobieranie danych z API OGC
- Walidacja danych zgodnie ze schematem backendu
- Ładowanie danych do bazy PostgreSQL z PostGIS
- Obsługa partycjonowanych tabel dla każdego RDLP

## Wymagania

- Python 3.13+
- PostgreSQL 16+ z PostGIS
- Docker i Docker Compose (opcjonalnie)

## Instalacja

### Lokalnie

1. Sklonuj repozytorium:
```bash
git clone <repository-url>
cd RDLP-API
```

2. Zainstaluj zależności:
```bash
pip install -r requirements.txt
```

3. Skonfiguruj zmienne środowiskowe:
```bash
cp .env.prod.example .env.prod
# Edytuj .env.prod z właściwymi wartościami
```

4. Uruchom migracje bazy danych:
```bash
psql -U postgres -d forest_db -f create_rdlp.sql
```

5. Uruchom aplikację:
```bash
python main.py
```

### Docker

1. Skonfiguruj zmienne środowiskowe:
```bash
cp .env.prod.example .env.prod
# Edytuj .env.prod
```

2. Uruchom z Docker Compose:
```bash
docker compose -f docker-compose.prod.yml up -d
```

## CI/CD

Projekt używa GitHub Actions do automatycznego testowania, budowania i wdrażania.

### Workflow

1. **Test** - Uruchamia testy jednostkowe z bazą danych PostgreSQL
2. **Build** - Buduje obraz Docker i publikuje do GitHub Container Registry
3. **Deploy** - Automatycznie wdraża na serwer produkcyjny (tylko dla brancha `main`)

### Konfiguracja Secrets

W GitHub Settings → Secrets and variables → Actions dodaj:

- `SERVER_IP` - IP serwera produkcyjnego
- `SERVER_USER` - Użytkownik SSH (np. `root`)
- `SSH_KEY` - Prywatny klucz SSH do serwera

### Ręczne wdrożenie

```bash
# Na serwerze produkcyjnym
cd /srv/app
docker compose -f docker-compose.prod.yml pull rdlp_api
docker compose -f docker-compose.prod.yml up -d rdlp_api
```

## Struktura projektu

```
RDLP-API/
├── config/           # Konfiguracja aplikacji
├── db/              # Połączenie z bazą danych
├── services/        # Logika biznesowa (API client, loader)
├── tests/           # Testy jednostkowe
├── utils/           # Narzędzia pomocnicze
├── main.py          # Punkt wejścia aplikacji
├── Dockerfile       # Obraz Docker
└── docker-compose.prod.yml  # Konfiguracja Docker Compose
```

## Konfiguracja

Aplikacja używa zmiennych środowiskowych do konfiguracji. Główne parametry:

- `DB_HOST` - Host bazy danych
- `DB_PORT` - Port bazy danych
- `DB_NAME` - Nazwa bazy danych
- `DB_USERNAME` - Użytkownik bazy danych
- `DB_PASSWORD` - Hasło bazy danych

## Testy

Uruchom testy:
```bash
RUN_TESTS=1 python -m unittest discover -s tests -p "test_*.py" -v
```

## Licencja

[Twoja licencja]