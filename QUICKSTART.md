# Szybki start - RDLP-API

## 🚀 Sposoby uruchomienia

### 1. Lokalnie (bez Dockera)

#### Wymagania:
- Python 3.13+
- PostgreSQL 16+ z PostGIS
- Dostęp do bazy danych

#### Kroki:

1. **Zainstaluj zależności:**
```bash
pip install -r requirements.txt
```

2. **Skonfiguruj zmienne środowiskowe:**
```bash
# Ustaw zmienne środowiskowe lub utwórz plik .env
export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=forest_db
export DB_USERNAME=postgres
export DB_PASSWORD=twoje_haslo
```

3. **Uruchom aplikację:**
```bash
python main.py
```

---

### 2. Docker Compose (produkcja)

#### Wymagania:
- Docker i Docker Compose
- Plik `.env.prod` z konfiguracją

#### Kroki:

1. **Utwórz plik `.env.prod`:**
```bash
# Przykładowa zawartość .env.prod
DB_HOST=db
DB_PORT=5432
DB_NAME=forest_db
DB_USERNAME=app
DB_PASSWORD=twoje_haslo
POSTGRES_DB=forest_db
POSTGRES_USER=app
POSTGRES_PASSWORD=twoje_haslo
```

2. **Uruchom wszystkie serwisy:**
```bash
docker compose -f docker-compose.prod.yml up -d
```

3. **Sprawdź logi:**
```bash
docker compose -f docker-compose.prod.yml logs -f rdlp_api
```

4. **Zatrzymaj serwisy:**
```bash
docker compose -f docker-compose.prod.yml down
```

---

### 3. Tylko RDLP-API (gdy baza już działa)

Jeśli masz już uruchomioną bazę danych PostgreSQL z PostGIS:

1. **Ustaw zmienne środowiskowe:**
```bash
export DB_HOST=localhost  # lub IP serwera z bazą
export DB_PORT=5432
export DB_NAME=forest_db
export DB_USERNAME=postgres
export DB_PASSWORD=twoje_haslo
```

2. **Uruchom aplikację:**
```bash
python main.py
```

---

### 4. Testy

**Uruchom wszystkie testy:**
```bash
python run_tests.py
```

**Lub przez unittest:**
```bash
RUN_TESTS=1 python main.py
```

---

## 📋 Co robi aplikacja?

1. **Pobiera dane** z API OGC (ogcapi.bdl.lasy.gov.pl)
2. **Waliduje dane** zgodnie ze schematem backendu
3. **Ładuje dane** do bazy PostgreSQL z PostGIS
4. **Tworzy partycjonowane tabele** dla każdego RDLP

---

## 🔍 Sprawdzanie statusu

### Docker Compose:
```bash
# Status wszystkich kontenerów
docker compose -f docker-compose.prod.yml ps

# Logi RDLP-API
docker compose -f docker-compose.prod.yml logs rdlp_api

# Logi bazy danych
docker compose -f docker-compose.prod.yml logs db
```

### Lokalnie:
- Sprawdź logi w katalogu `logs/` (jeśli skonfigurowane)
- Sprawdź połączenie z bazą danych

---

## ⚙️ Konfiguracja

### Zmienne środowiskowe:

| Zmienna | Opis | Przykład |
|---------|------|----------|
| `DB_HOST` | Host bazy danych | `localhost` lub `db` (w Docker) |
| `DB_PORT` | Port bazy danych | `5432` |
| `DB_NAME` | Nazwa bazy danych | `forest_db` |
| `DB_USERNAME` | Użytkownik bazy | `postgres` |
| `DB_PASSWORD` | Hasło bazy | `twoje_haslo` |

### Plik konfiguracyjny:
Aplikacja może również używać pliku `config.dev.env` w katalogu głównym.

---

## 🐛 Rozwiązywanie problemów

### Problem: "Connection refused"
- Sprawdź czy baza danych jest uruchomiona
- Sprawdź `DB_HOST` i `DB_PORT`
- Sprawdź firewall

### Problem: "Authentication failed"
- Sprawdź `DB_USERNAME` i `DB_PASSWORD`
- Sprawdź uprawnienia użytkownika w bazie

### Problem: "Module not found"
- Uruchom: `pip install -r requirements.txt`
- Sprawdź czy jesteś w odpowiednim środowisku wirtualnym

---

## 📚 Więcej informacji

Zobacz [README.md](README.md) dla szczegółowej dokumentacji.

