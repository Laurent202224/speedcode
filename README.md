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

Start the full app with:

```bash
python3 app/server.py
```

Then visit `http://127.0.0.1:8000`.

The pipeline is:

1. The frontend sends a free-text diagnosis or symptom description plus latitude and longitude.
2. The backend classifies the input into the closest supported diagnosis category from the list above.
3. The matcher finds the nearest hospital or clinic in `data/dataset.json` that covers that category.
4. The frontend shows the recommended hospital name and details.
