# Hospital Matcher

Hospital Matcher is a full-stack healthcare provider routing prototype that
turns unstructured healthcare data and unstructured patient requests into
structured hospital recommendations. The data pipeline converts raw provider
records into normalized JSON with diagnosis categories, coordinates, trust
scores, consistency labels, and Google review signals. A user then describes
symptoms, a diagnosis, or a care need in a chat interface, optionally with a
location. The backend combines that symptom description with the structured
provider dataset and Google Places API data to match the user to nearby
hospitals or clinics.

The project is built for hackathon demos and local experimentation with
healthcare search, geocoding, provider data quality, and LLM-assisted
reranking. It is not a medical diagnosis system; it routes the user's stated
need to the closest supported provider category and surfaces candidate
facilities from the available dataset.

The implementation combines:

- a polished chat-style frontend in `frontend/`
- a lightweight Python HTTP server in `app/server.py`
- diagnosis classification, geocoding, matching, and OpenAI pipeline logic in
  `backend/core/`
- dataset generation and scoring utilities in `data_pipeline/`, `scripts/`,
  and `trust_scoring/`

The current `full_pipeline` branch is configured for the Gaya/Bihar demo
dataset at `data/demo_2_eye.json`.

## How It Works

1. Raw provider data is converted into structured JSON records with provider
   names, locations, diagnosis categories, trust scores, consistency metadata,
   and optional Google Places ratings.
2. A user enters a free-text symptom or care request such as "blurry vision and
   eye pain near Civil Lines, Gaya".
3. In full pipeline mode, the backend asks OpenAI to extract a supported
   diagnosis category, location text, coordinates, and a compact care need.
4. Google Places API data is used for geocoding and, when available, provider
   review/rating signals.
5. The matcher searches the active structured dataset for hospitals or clinics
   that fit the extracted diagnosis and location, then ranks them by distance
   plus configured trust-score weighting.
6. OpenAI reranks the short list using provider context, and the frontend
   displays the best pick plus nearby alternatives, trust scores, consistency
   labels, and Google review signals when present.

## Coverage Analytics

The fetched remote `origin/main` change adds a Streamlit medical-island
visualization under `medical_island_visualization/medical_deserts.py`. That work
fits this project as a companion analytics layer rather than as part of the chat
request path. It consumes the same scored provider workbook to visualize:

- specialty coverage by distance to the nearest matching facility
- regional trust score based on nearby provider trust scores

Those maps help inspect access gaps and provider-data quality before or after
running the matcher. They do not change the `/api/recommend` flow, which still
uses the structured JSON dataset, symptom extraction, Google Places geocoding,
and reranking pipeline described above.

## Features

- Extracts a care need from natural language prompts in full pipeline mode.
- Matches providers by diagnosis category and distance.
- Uses trust score, consistency, and Google review fields when they are present in the dataset.
- Supports an offline test mode for exact diagnosis-category matching without API keys.
- Includes CLI utilities for local matching, dataset generation, spreadsheet sampling, and dataset overview reports.

## Quick Start

Use Python 3.11+.

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements.txt
cp .env.example .env
```

Fill `.env` only if you want the full LLM/geocoding pipeline:

```bash
OPENAI_API_KEY=your-openai-api-key
GOOGLE_PLACES_API_KEY=your-google-places-api-key
```

Start the app:

```bash
python3 app/server.py
```

Then open `http://127.0.0.1:8000`.

## Runtime Modes

The app mode is controlled by `configs/config.yaml`.

```yaml
app:
  test_mode: False
```

- `test_mode: False` is the full pipeline. It expects `OPENAI_API_KEY` for extraction and reranking. `GOOGLE_PLACES_API_KEY` is optional and improves geocoding.
- `test_mode: True` is an offline demo mode. The UI asks for an exact supported diagnosis category plus latitude/longitude and does not call OpenAI.

## Useful Commands

Run a local matcher query without starting the web server:

```bash
python3 app/find_hospitals.py \
  --diagnosis "Dentistry" \
  --latitude 24.786 \
  --longitude 85.006 \
  --exact-category \
  --limit 5
```

List supported diagnosis categories:

```bash
python3 app/find_hospitals.py --list-categories
```

Generate a JSON dataset from the raw CSV:

```bash
python3 data_pipeline/create_dataset.py --name dataset
```

Generate a smaller dataset near a reference point:

```bash
python3 data_pipeline/create_dataset.py \
  --name demo_gaya \
  --latitude 24.786 \
  --longitude 85.006 \
  --top 100
```

Run the smoke tests:

```bash
python3 -m unittest discover -s tests
```

## Configuration

`configs/config.yaml` contains the active data and model settings:

```yaml
paths:
  data_path: data/demo_2_eye.json
  template_json: data/template/template.json
  raw_source_csv: data/data_source/data_full.csv

app:
  test_mode: False
  openai:
    api_key_env: OPENAI_API_KEY
    extraction_model: gpt-5.4
    rerank_model: gpt-5.4-mini

matching:
  trust_score_km_equivalent: 10.0
```

Change `paths.data_path` when you want the app to search a different JSON dataset.

## Project Layout

```text
app/                 Web server and CLI matcher
backend/core/        Diagnosis, geocoding, matching, and OpenAI pipeline logic
backend/             Google Places review utility
configs/             Runtime configuration
data/                JSON datasets, template, and raw CSV source
data_pipeline/       CSV-to-JSON dataset generation
example_prompts/     Prompt bank for demos and manual testing
frontend/            Static chat UI
scripts/             Spreadsheet sampling and dataset overview tools
tests/               Offline smoke tests
trust_scoring/       Trust score and LLM consistency utilities
```

## Data Notes

Each JSON dataset record follows the shape in `data/template/template.json`:

```json
{
  "name": "Aruna Dental Clinic",
  "longitude": 84.99243927,
  "latitude": 24.80192757,
  "type": "clinic",
  "diagnosis": "Dentistry",
  "trustworthy_score": 7.32,
  "description": "Dental services including root canal treatment and laser dentistry",
  "consistency": "Suspicious",
  "consistency_flags": "No equipment listed for extensive dental procedures",
  "google_rating": 4.9,
  "google_rating_count": 172
}
```

`data/data_source/data_full.csv` is the raw CSV source used by the pipeline.
The scored source workbook is kept at
`data/data_source/VF_Hackathon_Dataset_India_Large_scored.xlsx`, and sample
workbooks live under `data/samples/`.

Some CSV rows contain NUL bytes; the pipeline strips those before parsing.

## Helper Scripts

Create a random spreadsheet sample:

```bash
python3 scripts/excel_random_sampler.py \
  --input data/data_source/VF_Hackathon_Dataset_India_Large_scored.xlsx \
  --output data/samples/Small_Dataset_N=50.xlsx \
  --samples 50 \
  --seed 42
```

Build a static data overview report:

```bash
python3 scripts/dataset_overview.py \
  --input data/data_source/VF_Hackathon_Dataset_India_Large_scored.xlsx \
  --output scripts/dataset_overview.html
```

Run the trust score helper:

```bash
python3 trust_scoring/TrustScore.py \
  --input data/data_source/VF_Hackathon_Dataset_India_Large_scored.xlsx \
  --output data/data_source/VF_Hackathon_Dataset_India_Large_trust_scored.xlsx
```

Run the LLM consistency helper:

```bash
python3 trust_scoring/inconsistency_check.py \
  --input data/samples/Small_Dataset_N=50.xlsx \
  --output data/samples/Small_Dataset_N=50_checked.xlsx
```

The consistency helper requires `OPENAI_API_KEY`.
