# Road Safety — Emergency Response Decision Engine

Road Safety is a lightweight decision-support CLI for road-accident analysis.
It helps explore accident data, compute insights, and generate visual outputs
(map, heatmap, commune risk map), with a dynamic long-term pulse report.

## Features

- Interactive CLI for analysis (`chat`, `insights`, `dashboard`)
- Data exploration and operational metrics from PostgreSQL
- Interactive geographic outputs:
  - point map (`map`)
  - heatmap (`heatmap`)
  - commune risk bubble map (`commune-map`)
- Dynamic long-term monitoring with `pulse`:
  - monthly trend
  - simple forecast
  - rising communes
  - persisted JSONL history for run-to-run comparison

## Tech stack

- Python `>=3.14,<3.15`
- Poetry
- PostgreSQL
- Folium (map rendering)
- Pytest (test suite)

## Quick start (recommended)

```bash
# 1) Install dependencies
poetry install

# 2) Run the CLI
poetry run road-safety <command>
```

Alternative without Poetry script:

```bash
PYTHONPATH=src python -m road_safety <command>
```

## Configuration (.env)

Create a `.env` at project root:

```env
DB_HOST=localhost
DB_PORT=5888
POSTGRES_USER=user
POSTGRES_PASSWORD=password
POSTGRES_DB=accidents_db
DB_SCHEMA=raw
```

Notes:
- In Devcontainer mode, host/port are usually `road-safety-postgres:5432`.
- If your container was initialized with different credentials, update `.env`
  to match the running PostgreSQL instance.

## Commands

```bash
# Interactive chat/queries
poetry run road-safety chat

# Key indicators
poetry run road-safety insights

# Dashboard
poetry run road-safety dashboard

# Maps
poetry run road-safety map [output.html] [limit]
poetry run road-safety heatmap [output.html] [limit]
poetry run road-safety commune-map [output.html]

# Dynamic pulse (trend + forecast + history)
poetry run road-safety pulse [history.jsonl] [months] [top]
```

Default `pulse` output history path:
`src/road_safety/data/pulse_history.jsonl`

## Geocoding prerequisite for map commands

`map`, `heatmap` and `commune-map` require `latitude` / `longitude` columns.
If missing, run:

```bash
poetry run python src/road_safety/bootstrap/geocode_communes.py
```

This script:
- creates GPS columns if absent
- geocodes communes with Nominatim (rate-limited)
- updates rows in `raw.accidents`

## Tests & coverage

Run all tests:

```bash
poetry run pytest
```

Run coverage:

```bash
poetry run pytest --cov=road_safety --cov-report=term-missing
```

Current status (2026-03-24): **100% coverage** on `src/road_safety`.

## Project structure

- `.devcontainer/` — Devcontainer and PostgreSQL compose setup
- `scripts/` — helper scripts
- `src/road_safety/` — application source code
- `tests/` — automated tests
- `pyproject.toml` — project metadata and dependencies

## Troubleshooting

- `password authentication failed`: credentials in `.env` do not match DB.
- `Missing GPS columns in raw.accidents`: run geocoding script above.
- `Database connection failed`: verify `DB_HOST`, `DB_PORT`, user/password/db.

## License

URCA — see `pyproject.toml`.

## Contact

Hervé ADJAKOSSA — adjakossah@gmail.com
