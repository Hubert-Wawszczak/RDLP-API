# Porównanie danych: API OGC vs Pliki ZIP

## Struktura danych z API OGC (poprzednia wersja)

Dane z API OGC miały format GeoJSON FeatureCollection:
```json
{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "id": 123,
      "properties": {
        "area_type": "...",
        "a_i_num": 123,
        "adr_for": "...",
        ...
      },
      "geometry": {
        "type": "MultiPolygon",
        "coordinates": [...]
      }
    }
  ]
}
```

## Struktura danych z ZIP (Shapefile)

Pliki ZIP zawierają Shapefile (.shp, .dbf, .shx, .prj), które po konwersji na GeoJSON mają identyczną strukturę.

## Pola w RDLPData (zgodne z WydzielenieResponse)

Wszystkie pola są zgodne:
- `id` - identyfikator wydzielenia
- `area_type` - typ powierzchni
- `a_i_num` - numer
- `silvicult` - hodowla
- `stand_stru` - struktura drzewostanu
- `sub_area` - powierzchnia
- `species_cd` - kod gatunku
- `spec_age` - wiek gatunku
- `nazwa` - nazwa
- `adr_for` - adres for (wymagane)
- `site_type` - typ siedliska
- `forest_fun` - funkcja lasu
- `rotat_age` - wiek rębności
- `prot_categ` - kategoria ochrony
- `part_cd` - kod części
- `a_year` - rok
- `geometry` - geometria (MultiPolygon)
- `forest_range_name` - nazwa nadleśnictwa (wymagane)
- `rdlp_name` - nazwa RDLP (wymagane)

## Pokrycie danych

✅ **WSZYSTKIE pola są pokryte** - dane z ZIP zawierają te same pola co dane z API OGC.

Różnice:
- **Źródło**: API OGC vs pliki ZIP
- **Format**: JSON bezpośrednio vs Shapefile (konwertowany na GeoJSON)
- **Struktura**: Identyczna po konwersji

## Mapowanie RDLP

Kody regionów BDL (BDL_XX_XX) są mapowane na nazwy RDLP:
- 01 → bialystok
- 02 → katowice
- 03 → krakow
- 04 → krosno
- 05 → lublin
- 06 → lodz
- 07 → olsztyn
- 08 → pila
- 09 → poznan
- 10 → szczecin
- 11 → szczecinek
- 12 → torun
- 13 → wroclaw
- 14 → zielona_gora
- 15 → gdansk
- 16 → radom
- 17 → warszawa

