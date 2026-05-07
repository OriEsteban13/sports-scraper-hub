# Sports Scraper Hub

Aplicación web full-stack para scraping diario de medios de sports business, con extracción automática de **Propiedad / Marca / Agencia / Resumen** vía Gemini AI y composición de correos ejecutivos.

## Características

- **Scraping multi-portal** con auto-escalación `Fetcher → DynamicFetcher → StealthyFetcher` (Scrapling)
- **Scheduler en background** que respeta la cadencia configurada por sitio (diario, semanal, mensual…)
- **Enriquecimiento AI** con Gemini 2.5 Flash Lite que extrae entidades + resumen ejecutivo de cada artículo
- **Importador masivo** que resuelve URLs automáticamente desde una lista de nombres de medios
- **Búsqueda full-text** en titular, body completo, resumen, propiedad, marca y agencia
- **Composición de correos** HTML con tabla Propiedad/Marca/Agencia + envío vía mailto
- **CRUD completo de sitios** con modal de edición, modo stealth, patrón de URL configurable

## Stack

- **FastAPI + Starlette 1.0** (servidor)
- **SQLite** (persistencia)
- **Jinja2 + Tailwind CDN + Alpine.js** (frontend ligero, sin build)
- **Scrapling 0.4.7** (scraping con anti-bot integrado)
- **Google Gemini 2.5 Flash Lite** (extracción de entidades, gratis hasta 1.000 req/día)

## Setup

Requiere **Python 3.10+** (recomendado 3.13).

```bash
# 1) crea entorno virtual
python3.13 -m venv .venv
source .venv/bin/activate

# 2) dependencias
pip install "scrapling[all]>=0.4.7" fastapi uvicorn jinja2 python-multipart google-genai anthropic

# 3) instala los browsers de Scrapling (una vez)
scrapling install --force

# 4) configura tu API key de Gemini (gratis: https://aistudio.google.com/apikey)
export GEMINI_API_KEY=AIza...

# 5) arranca
.venv/bin/uvicorn webapp:app --port 8000 --reload
```

Abre http://localhost:8000

## Estructura

```
webapp.py              Backend FastAPI (rutas, scraping, AI, scheduler)
templates/
  base.html            Layout con sidebar + navegación
  dashboard.html       Stats + buscador
  sites.html           CRUD sitios + import masivo
  news.html            Tabla artículos + enrich + búsqueda
  aggregated.html      Histórico paginado con filtros
  email.html           Composer de correos HTML
  settings.html        Frecuencia de scraping y auto-enrich por medio
```

## Scheduler

Hilo en segundo plano dentro del proceso de uvicorn. Cada 5 minutos consulta:

```sql
SELECT * FROM sites
WHERE active=1 AND scrape_frequency_days > 0
  AND (last_scraped IS NULL OR
       julianday('now') - julianday(last_scraped) >= scrape_frequency_days)
```

Si un sitio tiene `auto_enrich=1`, también enriquece con Gemini los artículos nuevos sin body.

Para producción 24/7 considera correrlo bajo `launchd` (macOS) o `systemd`.

## Datos

La base de datos `scraper_hub.db` se crea automáticamente al arrancar y **no se versiona** — se regenera scrapeando los sitios configurados.
