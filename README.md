# speedcode
5th Hacknation

## Doctor Types (Diagnosis Categories)

This project contains healthcare facility data with the following finite set of doctor/specialist categories found in the dataset:

### 1. Primary Care
- Primary Care / General Practice
- Internal Medicine
- Pediatrics
- Gynecology

### 2. Specialists
- Dermatology
- Cardiology
- Orthopedics
- Neurology
- Psychiatry / Psychotherapy
- ENT
- Ophthalmology
- Urology
- Gastroenterology
- Endocrinology
- Rheumatology
- Pulmonology
- Oncology

### 3. Dentistry
- Dentistry
- Orthodontics
- Oral Surgery

### 4. Acute and Special Care
- Emergency Medicine
- Surgery
- Radiology
- Anesthesiology
- Intensive Care
- Pathology / Laboratory Medicine

### 5. Therapy-related Health Professions
- Physiotherapy
- Occupational Therapy
- Nutrition Counseling
- Midwifery

### 6. Other
- Alternative Medicine
- Pharmacy
- Veterinary Medicine

## Data Pipeline

Use the data pipeline script to convert the raw CSV data to the standardized JSON format:

```bash
python3 data_pipeline/create_dataset.py
```

This script:
- Reads `data/data_source/data_full.csv`
- Maps 560+ unique specialties to the finite set of English diagnosis categories above
- Handles typos and variations in specialty names
- Filters records with valid coordinates
- Generates `data/dataset.json` according to the template format

### Template Structure

Each record in the dataset follows this structure (see `data/template/template.json`):

```json
{
  "name": "Hospital Name",
  "longitude": 8.682127,
  "latitude": 50.110924,
  "type": "clinic|hospital|dentist|doctor|pharmacy",
  "diagnosis": "English diagnosis category",
  "trustworthy_score": 0.8,
  "description": "Description of the facility"
}
```

### Data Statistics

- Total CSV records: 10,053
- Total unique specialties in CSV: 560
- Records with valid coordinates: 10,000
- Mapped to finite diagnosis categories: 33 canonical English categories

## App

A static chat UI lives in `frontend/`, and `app/server.py` serves both the UI and a local diagnosis-to-hospital API.

## API Keys

Create a local secrets file from the example:

```bash
cp configs/api_keys.env.example configs/api_keys.env
```

Then edit `configs/api_keys.env` and fill in:

```env
OPENAI_API_KEY=your-openai-api-key
OPENAI_MODEL=gpt-5.4-nano
OPENAI_HOSPITAL_SELECTION_MODEL=gpt-4.5-mini
GOOGLE_PLACES_API_KEY=your-google-api-key
```

`app/server.py` loads `configs/api_keys.env` automatically. The file is gitignored so real keys stay local.
The Google key is used for address geocoding and Google Places review enrichment. Those review signals are added to the 5 closest hospitals before the second LLM chooses the best option.

Start the full app with:

```bash
python3 app/server.py
```

Then visit `http://127.0.0.1:8000`.

The pipeline is:

1. The frontend sends a free-text symptom/care request that should include a location as coordinates or an address.
2. If configured, OpenAI returns structured JSON with whether the input is medical, the needed doctor category, urgency, reason, and extracted location.
3. If the input is not medical or location is missing, the app asks for the missing information instead of forcing a provider match.
4. If the location is an address, the backend geocodes it with Google Geocoding before matching.
5. The matcher returns the 5 closest hospitals or clinics in `data/dataset.json` that cover the selected category.
6. The backend formats the final chat response locally.

### LLM Configuration

Copy `.env.example` to `.env` and fill in:

```bash
OPENAI_API_KEY=your-openai-api-key-here
OPENAI_MODEL=gpt-5.4-nano
OPENAI_BASE_URL=https://api.openai.com/v1
```

`OPENAI_API_KEY` is a normal OpenAI platform API key. The key is read only by the Python backend and is not sent to the browser.

For address input, set `GOOGLE_PLACES_API_KEY` in `.env` to a Google Maps Platform key with the Geocoding API enabled.
