# speedcode
5th Hacknation

## Doctor Types (Diagnosis Categories)

This project contains healthcare facility data with the following finite set of doctor/specialist categories found in the dataset:

### 1. Grundversorgung (Primary Care)
- Hausarzt / Allgemeinmedizin (6,577 records)
- Innere Medizin (3,556 records)
- Kinderarzt / Pädiatrie (640 records)
- Gynäkologie / Frauenarzt (826 records)

### 2. Fachärzte (Specialists)
- Dermatologie / Hautarzt (655 records)
- Kardiologie / Herzarzt (441 records)
- Orthopädie (508 records)
- Neurologie (230 records)
- Psychiatrie / Psychotherapie (196 records)
- HNO (361 records)
- Augenarzt / Ophthalmologie (495 records)
- Urologie (261 records)
- Gastroenterologie (328 records)
- Endokrinologie (432 records)
- Rheumatologie (91 records)
- Pneumologie (226 records)
- Onkologie (142 records)

### 3. Zahnmedizin (Dentistry)
- Zahnarzt (2,133 records)
- Kieferorthopädie (624 records)
- Oralchirurgie (465 records)

### 4. Akut- und Spezialversorgung (Acute and Special Care)
- Notfallmedizin (211 records)
- Chirurgie (335 records)
- Radiologie (456 records)
- Anästhesiologie (68 records)
- Intensivmedizin (194 records)
- Pathologie / Labor (536 records)

### 5. Therapie-nahe Gesundheitsberufe (Therapy-related Health Professions)
- Physiotherapie (381 records)
- Ergotherapie (3 records)
- Ernährungsberatung (2 records)
- Hebamme (153 records)

### 6. Sonstige (Other)
- Alternativmedizin (Homeopathy, Ayurveda, Naturopathy)
- Apotheke (Pharmacy)
- Tierarzt (Veterinary)

## Data Pipeline

Use the data pipeline script to convert the raw CSV data to the standardized JSON format:

```bash
python3 data_pipeline/create_dataset.py
```

This script:
- Reads `data/data_source/data_full.csv`
- Maps 560+ unique specialties to the finite set of German diagnosis categories above
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
  "diagnosis": "German diagnosis category",
  "trustworthy_score": 0.8,
  "description": "Description of the facility"
}
```

### Data Statistics

- Total CSV records: 10,053
- Total unique specialties in CSV: 560
- Records with valid coordinates: ~8,500
- Mapped to finite diagnosis categories: 17 main categories

