#!/usr/bin/env python3
"""
Data pipeline script to convert data_full.csv to dataset.json.

Specialties from the raw CSV are normalized into one canonical, English
diagnosis category so the frontend, matcher, and recommendation pipeline all
share the same vocabulary.
"""

import argparse
import csv
import json
import math
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from tqdm import tqdm
from openai import OpenAI

# Load environment variables from .env file
PROJECT_ROOT = Path(__file__).resolve().parents[1]
dotenv_path = PROJECT_ROOT / ".env"
if dotenv_path.exists():
    for raw_line in dotenv_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'\"")
        if key and key not in os.environ:
            os.environ[key] = value

# Google Places API configuration
PLACES_TEXT_SEARCH_URL = "https://places.googleapis.com/v1/places:searchText"


def build_address_from_row(row: Dict[str, Any]) -> str:
    """Build address string from CSV row for Google Places search."""
    address_fields = [
        "address_line1",
        "address_line2",
        "address_line3",
        "address_city",
        "address_stateOrRegion",
        "address_zipOrPostcode",
        "address_country",
    ]
    ignored_values = {"", "null", "none", "nan"}
    parts = []
    for field in address_fields:
        value = str(row.get(field, "")).strip()
        if value.lower() not in ignored_values:
            parts.append(value)
    return ", ".join(parts)


def fetch_google_place_rating(row: Dict[str, Any], api_key: str) -> Dict[str, Any]:
    """Fetch Google Places rating and review count for a hospital."""
    name = row.get("name", "").strip()
    address = build_address_from_row(row)
    
    if not name:
        return {"google_rating": None, "google_rating_count": None}
    
    # Build search query
    query_parts = [name]
    if address:
        query_parts.append(address)
    text_query = ", ".join(query_parts)
    
    body: Dict[str, Any] = {
        "textQuery": text_query,
        "pageSize": 1,
    }
    
    # Add location bias if coordinates available
    latitude = row.get("latitude")
    longitude = row.get("longitude")
    if latitude is not None and longitude is not None:
        try:
            body["locationBias"] = {
                "circle": {
                    "center": {
                        "latitude": float(latitude),
                        "longitude": float(longitude),
                    },
                    "radius": 1000.0,
                }
            }
        except (ValueError, TypeError):
            pass
    
    # Make API request
    try:
        payload = json.dumps(body).encode("utf-8")
        headers = {
            "Content-Type": "application/json",
            "X-Goog-Api-Key": api_key,
            "X-Goog-FieldMask": "places.id,places.displayName,places.rating,places.userRatingCount",
        }
        
        request = Request(PLACES_TEXT_SEARCH_URL, data=payload, headers=headers, method="POST")
        
        with urlopen(request, timeout=30) as response:
            data = json.loads(response.read().decode("utf-8"))
        
        places = data.get("places", [])
        if places:
            place = places[0]
            rating = place.get("rating")
            rating_count = place.get("userRatingCount")
            # Only return if we actually got values
            if rating is not None or rating_count is not None:
                return {
                    "google_rating": rating,
                    "google_rating_count": rating_count,
                }
    except (HTTPError, URLError, json.JSONDecodeError, KeyError, Exception):
        # Silently fail and return None values
        pass
    
    return {"google_rating": None, "google_rating_count": None}


# Specialty to German diagnosis mapping
SPECIALTY_TO_DIAGNOSIS = {
    # Grundversorgung (Primary Care)
    "familyMedicine": "Hausarzt / Allgemeinmedizin",
    "internalMedicine": "Innere Medizin",
    "pediatrics": "Kinderarzt / Pädiatrie",
    "gynecologyAndObstetrics": "Gynäkologie / Frauenarzt",
    "gynecology": "Gynäkologie / Frauenarzt",
    "obstetricsAndMaternityCare": "Gynäkologie / Frauenarzt",
    "obstetricsAndGynaecology": "Gynäkologie / Frauenarzt",
    "obstetricsAndGynecology": "Gynäkologie / Frauenarzt",
    "cosmeticGynaecology": "Gynäkologie / Frauenarzt",
    "cosmeticGynecology": "Gynäkologie / Frauenarzt",
    
    # Fachärzte (Specialists)
    "dermatology": "Dermatologie / Hautarzt",
    "cosmeticDermatology": "Dermatologie / Hautarzt",
    "dermatopathology": "Dermatologie / Hautarzt",
    "immunodermatologyAndComplexMedicalDermatology": "Dermatologie / Hautarzt",
    "skinOfColorDermatology": "Dermatologie / Hautarzt",
    "pediatricDermatology": "Dermatologie / Hautarzt",
    "paediatricDermatology": "Dermatologie / Hautarzt",
    "geriatricDermatology": "Dermatologie / Hautarzt",
    "proceduralDermatology": "Dermatologie / Hautarzt",
    "dermatosurgery": "Dermatologie / Hautarzt",
    
    "cardiology": "Kardiologie / Herzarzt",
    "pediatricCardiology": "Kardiologie / Herzarzt",
    "interventionalCardiology": "Kardiologie / Herzarzt",
    "interventionalCardiologist": "Kardiologie / Herzarzt",
    "cardiacElectrophysiology": "Kardiologie / Herzarzt",
    
    "orthopedicSurgery": "Orthopädie",
    "orthopedics": "Orthopädie",
    "orthopaedics": "Orthopädie",
    "pediatricOrthopedicSurgery": "Orthopädie",
    "paediatricOrthopedicSurgery": "Orthopädie",
    "pediatricOrthopedics": "Orthopädie",
    "paediatricOrthopaedic": "Orthopädie",
    "orthopedicSpineSurgery": "Orthopädie",
    "orthopedicSportsMedicine": "Orthopädie",
    "orthopedicTraumaAndFractureSurgery": "Orthopädie",
    "orthopedicOncology": "Orthopädie",
    "orthopaedicOncology": "Orthopädie",
    "footAndAnkleOrthopedicSurgery": "Orthopädie",
    "shoulderAndElbowOrthopedicSurgery": "Orthopädie",
    "orth orthopedicSurgery": "Orthopädie",
    
    "neurology": "Neurologie",
    "childNeurology": "Neurologie",
    "pediatricNeurology": "Neurologie",
    "vascularNeurology": "Neurologie",
    "clinicalNeurophysiology": "Neurologie",
    "epilepsyNeurology": "Neurologie",
    "sleepMedicineNeurology": "Neurologie",
    "movementDisorders": "Neurologie",
    "cognitiveAndBehavioralNeurology": "Neurologie",
    "brainInjuryNeurology": "Neurologie",
    "neuroimmunology": "Neurologie",
    "interventionalNeurology": "Neurologie",
    "neuroOtolaryngology": "Neurologie",
    
    "psychiatry": "Psychiatrie / Psychotherapie",
    "addictionPsychiatry": "Psychiatrie / Psychotherapie",
    "childAndAdolescentPsychiatry": "Psychiatrie / Psychotherapie",
    "neuropsychiatry": "Psychiatrie / Psychotherapie",
    "perinatalAndReproductivePsychiatry": "Psychiatrie / Psychotherapie",
    "geriatricPsychiatry": "Psychiatrie / Psychotherapie",
    "forensicPsychiatry": "Psychiatrie / Psychotherapie",
    "emergencyPsychiatry": "Psychiatrie / Psychotherapie",
    "anxietyPsychiatry": "Psychiatrie / Psychotherapie",
    "clinicalPsychology": "Psychiatrie / Psychotherapie",
    "rehabilitationPsychology": "Psychiatrie / Psychotherapie",
    
    "otolaryngology": "HNO",
    "headAndNeckSurgeryOtolaryngology": "HNO",
    "pediatricOtolaryngology": "HNO",
    "sleepMedicineOtolaryngology": "HNO",
    "rhinology": "HNO",
    "laryngology": "HNO",
    "neurotologyAndOtology": "HNO",
    "otology": "HNO",
    "facialPlasticsAndReconstructiveOtolaryngology": "HNO",
    "ent": "HNO",
    "ENT/ Otorhinolaryngologist": "HNO",
    
    "ophthalmology": "Augenarzt / Ophthalmologie",
    "cataractAndAnteriorSegmentSurgery": "Augenarzt / Ophthalmologie",
    "retinaAndVitreoretinalOphthalmology": "Augenarzt / Ophthalmologie",
    "refractiveSurgeryOphthalmology": "Augenarzt / Ophthalmologie",
    "corneaOphthalmology": "Augenarzt / Ophthalmologie",
    "glaucomaOphthalmology": "Augenarzt / Ophthalmologie",
    "pediatricsAndStrabismusOphthalmology": "Augenarzt / Ophthalmologie",
    "oculoplasticsAndReconstructiveOrbitalSurgery": "Augenarzt / Ophthalmologie",
    "occuloplasticsAndReconstructiveOrbitalSurgery": "Augenarzt / Ophthalmologie",
    "eyeTraumaAndEmergencyEyeCare": "Augenarzt / Ophthalmologie",
    "neuroOphthalmology": "Augenarzt / Ophthalmologie",
    "uveitisOphthalmology": "Augenarzt / Ophthalmologie",
    "pediatricOphthalmology": "Augenarzt / Ophthalmologie",
    "paediatricOphthalmology": "Augenarzt / Ophthalmologie",
    "ocularOncology": "Augenarzt / Ophthalmologie",
    "eyeOphthalmology": "Augenarzt / Ophthalmologie",
    "keratoconus": "Augenarzt / Ophthalmologie",
    "amblyopia": "Augenarzt / Ophthalmologie",
    "squintEyeSurgery": "Augenarzt / Ophthalmologie",
    "squintSurgery": "Augenarzt / Ophthalmologie",
    "Squint": "Augenarzt / Ophthalmologie",
    "CVI": "Augenarzt / Ophthalmologie",
    "prostheticContactLenses": "Augenarzt / Ophthalmologie",
    "Vitreo-Retinal Surgery": "Augenarzt / Ophthalmologie",
    
    "urology": "Urologie",
    "pediatricUrology": "Urologie",
    "urologicOncology": "Urologie",
    "renalTransplantationUrology": "Urologie",
    "genitourinaryReconstructiveSurgery": "Urologie",
    "minimallyInvasiveSurgeryAndEndourology": "Urologie",
    "femaleUrology": "Urologie",
    "Female Urological Disease": "Urologie",
    "neuroUrology": "Urologie",
    "endourology": "Urologie",
    "Andrology (Male Infertility)": "Urologie",
    
    "gastroenterology": "Gastroenterologie",
    "pediatricGastroenterology": "Gastroenterologie",
    "hepatology": "Gastroenterologie",
    "gastrointestinalSurgery": "Gastroenterologie",
    "Gastroenterologist": "Gastroenterologie",
    
    "endocrinologyAndDiabetesAndMetabolism": "Endokrinologie",
    "endocrinology": "Endokrinologie",
    "pediatricEndocrinology": "Endokrinologie",
    "paediatricEndocrinology": "Endokrinologie",
    "metabolicDisorders": "Endokrinologie",
    "pituitaryDisorders": "Endokrinologie",
    "thyroid": "Endokrinologie",
    "diabetology": "Endokrinologie",
    
    "rheumatology": "Rheumatologie",
    "pediatricRheumatology": "Rheumatologie",
    
    "pulmonology": "Pneumologie",
    "pediatricPulmonology": "Pneumologie",
    "interventionalPulmonology": "Pneumologie",
    "criticalCarePulmonaryMedicine": "Pneumologie",
    
    "medicalOncology": "Onkologie",
    "oncology": "Onkologie",
    "surgicalOncology": "Onkologie",
    "breastSurgicalOncology": "Onkologie",
    "pediatricOncology": "Onkologie",
    "radiationOncology": "Onkologie",
    "radiationAndClinicalOncology": "Onkologie",
    "neuroOncology": "Onkologie",
    "neuroOncologyNeurosurgery": "Onkologie",
    "gynecologicalOncology": "Onkologie",
    "pediatricHematologyOncology": "Onkologie",
    "oncologyAndReconstructiveOralAndMaxillofacialSurgery": "Onkologie",
    "Oncofertility": "Onkologie",
    
    # Zahnmedizin (Dentistry)
    "dentistry": "Zahnarzt",
    "generalDentistry": "Zahnarzt",
    "aestheticDentistry": "Zahnarzt",
    "cosmeticDentistry": "Zahnarzt",
    "cosmetics dentistry": "Zahnarzt",
    "estheticDentistry": "Zahnarzt",
    "dentalImplants": "Zahnarzt",
    "dental implants": "Zahnarzt",
    "implantDentistry": "Zahnarzt",
    "implantology": "Zahnarzt",
    "dentalImplantology": "Zahnarzt",
    "implant dentistry": "Zahnarzt",
    "dentalImplantsAndPeriodontics": "Zahnarzt",
    "digitalDentistry": "Zahnarzt",
    "digitalSmileDesigning": "Zahnarzt",
    "smiledesign": "Zahnarzt",
    "preventiveDentistry": "Zahnarzt",
    "restorativeDentistry": "Zahnarzt",
    "conservativeDentistry": "Zahnarzt",
    "emergencyDentistry": "Zahnarzt",
    "geriatricDentistry": "Zahnarzt",
    "pediatricDentistry": "Zahnarzt",
    "paediatricDentistry": "Zahnarzt",
    "pediatricAndPreventiveDentistry": "Zahnarzt",
    "pedodontics": "Zahnarzt",
    "paedodontics": "Zahnarzt",
    "laserDentistry": "Zahnarzt",
    "laserdentistry": "Zahnarzt",
    "Laser Dentistry": "Zahnarzt",
    "sedationDentistry": "Zahnarzt",
    "dentalAnesthesia": "Zahnarzt",
    "cosmeticBonding": "Zahnarzt",
    "dentalVeneers": "Zahnarzt",
    "toothWhitening": "Zahnarzt",
    "toothWhiteningToothBleaching": "Zahnarzt",
    "painlessRootCanalTreatments": "Zahnarzt",
    "Scaling & polishing": "Zahnarzt",
    "Teeth Whitening": "Zahnarzt",
    "Cavity Filling": "Zahnarzt",
    "Fluoride Application & Sealants": "Zahnarzt",
    "Kids Dentistry": "Zahnarzt",
    "Digital Smile Designing": "Zahnarzt",
    
    "orthodontics": "Kieferorthopädie",
    "Orthodontics": "Kieferorthopädie",
    "lingualOrthodontics": "Kieferorthopädie",
    "dentofacialOrthopedics": "Kieferorthopädie",
    "Invisalign": "Kieferorthopädie",
    
    "oralAndMaxillofacialSurgery": "Oralchirurgie",
    "OralAndMaxillofacialSurgery": "Oralchirurgie",
    " OralAndMaxillofacialSurgery": "Oralchirurgie",
    "dentoalveolarSurgery": "Oralchirurgie",
    "craniofacialAndCleftOralMaxillofacialSurgery": "Oralchirurgie",
    "cosmeticMaxillofacialSurgery": "Oralchirurgie",
    "craniomaxillofacialTraumaSurgery": "Oralchirurgie",
    "orthognathicSurgery": "Oralchirurgie",
    "TMJSurgery": "Oralchirurgie",
    "TMJ Therapy": "Oralchirurgie",
    "TMJ Disorders": "Oralchirurgie",
    "temporomandibularDisorders": "Oralchirurgie",
    "oralSurgery": "Oralchirurgie",
    "oralMedicine": "Oralchirurgie",
    "oralPathology": "Oralchirurgie",
    "oralAndMaxillofacialPathology": "Oralchirurgie",
    "oralRehabilitation": "Oralchirurgie",
    "maxillofacialProsthesis": "Oralchirurgie",
    "orofacialMyofunctionalTherapy": "Oralchirurgie",
    "anorectal surgeries": "Oralchirurgie",
    
    "endodontics": "Zahnarzt",
    "Endodontics": "Zahnarzt",
    
    "prosthodontics": "Zahnarzt",
    "Prosthodontics": "Zahnarzt",
    "crownAndBridge": "Zahnarzt",
    "Crown & Bridges": "Zahnarzt",
    "crownsAndBridges": "Zahnarzt",
    
    "periodontics": "Zahnarzt",
    "Periodontics": "Zahnarzt",
    "periodontics - Children & Adults": "Zahnarzt",
    "PERIODONTIST IMPLANTOLOGIST": "Zahnarzt",
    "periodontalCare": "Zahnarzt",
    
    # Akut- und Spezialversorgung (Acute and Special Care)
    "emergencyMedicine": "Notfallmedizin",
    "pediatricEmergencyMedicine": "Notfallmedizin",
    "traumaAndAccidents": "Notfallmedizin",
    
    "generalSurgery": "Chirurgie",
    "pediatricSurgery": "Chirurgie",
    "vascularSurgery": "Chirurgie",
    "colorectalSurgery": "Chirurgie",
    "bariatricSurgery": "Chirurgie",
    "hepatopancreatobiliarySurgery": "Chirurgie",
    "transplantSurgery": "Chirurgie",
    "cardiacSurgery": "Chirurgie",
    "thoracicSurgery": "Chirurgie",
    "cardiothoracicSurgery": "Chirurgie",
    "cardiovascularThoracicSurgery": "Chirurgie",
    "generalThoracicSurgery": "Chirurgie",
    "breastSurgery": "Chirurgie",
    "endocrineSurgery": "Chirurgie",
    "traumaSurgery": "Chirurgie",
    "burnGeneralSurgery": "Chirurgie",
    "laparoscopicSurgery": "Chirurgie",
    "Advance Laproscopic Surgery": "Chirurgie",
    "Laparoscopic Surgeon": "Chirurgie",
    "upperGIAndForegutSurgery": "Chirurgie",
    "hepatobiliarySurgery": "Chirurgie",
    "pancreaticoduodenectomy": "Chirurgie",
    "dayCareProcedures": "Chirurgie",
    "arthroscopicSurgery": "Chirurgie",
    "Arthroscopy Surgeon / Joint Replacement Surgeon": "Chirurgie",
    "jointReplacementSurgery": "Chirurgie",
    "hipReplacementSurgery": "Chirurgie",
    "hipReplacement": "Chirurgie",
    "replacements": "Chirurgie",
    "Proctology": "Chirurgie",
    "coloproctology": "Chirurgie",
    "Coloproctology": "Chirurgie",
    "Video Endourological Surgeries": "Chirurgie",
    "footSurgery": "Chirurgie",
    "genderAffirmingSurgery": "Chirurgie",
    "gynecologicalSurgery": "Chirurgie",
    
    "plasticSurgery": "Chirurgie",
    "reconstructivePlasticSurgery": "Chirurgie",
    "plasticReconstructionSurgery": "Chirurgie",
    "burnAndTraumaPlasticSurgery": "Chirurgie",
    "aestheticAndCosmeticSurgery": "Chirurgie",
    "cosmeticSurgery": "Chirurgie",
    "cosmeticAndCosmeticSurgery": "Chirurgie",
    "Reconstructive Surgery": "Chirurgie",
    "microsurgeryAndTransplantPlasticSurgery": "Chirurgie",
    "paediatricPlasticSurgery": "Chirurgie",
    
    "neurosurgery": "Chirurgie",
    "spineNeurosurgery": "Chirurgie",
    "pediatricNeurosurgery": "Chirurgie",
    "skullBaseNeurosurgery": "Chirurgie",
    "cerebrovascularNeurosurgery": "Chirurgie",
    "neurotraumaNeurosurgery": "Chirurgie",
    "functionalNeurosurgery": "Chirurgie",
    "peripheralNerveNeurosurgery": "Chirurgie",
    "neurointerventionalSurgery": "Chirurgie",
    "neurocriticalCareNeurosurgery": "Chirurgie",
    "vascularNeurosurgery": "Chirurgie",
    "endovascularNeurosurgery": "Chirurgie",
    "navigationAndUSGuidedTumorResection": "Chirurgie",
    "intraoperativeNeuromonitoring": "Chirurgie",
    "cranialAndSkullBaseEndoscopicSurgery": "Chirurgie",
    "minimalInvasiveSpineSurgery": "Chirurgie",
    "microneurosurgery": "Chirurgie",
    
    "radiology": "Radiologie",
    "interventionalRadiology": "Radiologie",
    "diagnosticRadiology": "Radiologie",
    "breastImaging": "Radiologie",
    "cardiothoracicRadiology": "Radiologie",
    "pediatricRadiology": "Radiologie",
    "musculoskeletalRadiology": "Radiologie",
    "nuclearMedicineAndMolecularImaging": "Radiologie",
    "boneDensitometry": "Radiologie",
    
    "anesthesia": "Anästhesiologie",
    "anesthesiology": "Anästhesiologie",
    "pediatricAnesthesiology": "Anästhesiologie",
    "obstetricAnesthesiology": "Anästhesiologie",
    "painMedicineAnesthesiology": "Anästhesiologie",
    
    "criticalCareMedicine": "Intensivmedizin",
    "pediatricCriticalCareMedicine": "Intensivmedizin",
    "neurocriticalCare": "Intensivmedizin",
    
    "pathology": "Pathologie / Labor",
    "Pathology": "Pathologie / Labor",
    "clinicalPathology": "Pathologie / Labor",
    "anatomicPathology": "Pathologie / Labor",
    "cytopathology": "Pathologie / Labor",
    "histopathology": "Pathologie / Labor",
    "cytology": "Pathologie / Labor",
    "Cytology": "Pathologie / Labor",
    "Hisphathology/ Cytology": "Pathologie / Labor",
    "Histopathology": "Pathologie / Labor",
    "histopathologyLaboratory": "Pathologie / Labor",
    "cytologyLaboratory": "Pathologie / Labor",
    "microbiology": "Pathologie / Labor",
    "Microbiology": "Pathologie / Labor",
    "microbiologyLaboratory": "Pathologie / Labor",
    "bacteriologyLab": "Pathologie / Labor",
    "mycologyLaboratory": "Pathologie / Labor",
    "micrology": "Pathologie / Labor",
    "serology": "Pathologie / Labor",
    "serologyLab": "Pathologie / Labor",
    "viralMarkers": "Pathologie / Labor",
    "virologyLab": "Pathologie / Labor",
    "parasitologyLaboratory": "Pathologie / Labor",
    "immunologyLab": "Pathologie / Labor",
    "immunology": "Pathologie / Labor",
    "Immunology/Hormones": "Pathologie / Labor",
    "clinicalImmunology": "Pathologie / Labor",
    "biochemistry": "Pathologie / Labor",
    "Biochemistry": "Pathologie / Labor",
    "biochemistryLab": "Pathologie / Labor",
    "clinicalBioChemistry": "Pathologie / Labor",
    "bloodBiochemistry": "Pathologie / Labor",
    "hematology": "Pathologie / Labor",
    "Hematology": "Pathologie / Labor",
    "haematology": "Pathologie / Labor",
    "molecularBiology": "Pathologie / Labor",
    "molecularDiagnostics": "Pathologie / Labor",
    "Molecular Diagnostics": "Pathologie / Labor",
    "molecularTests": "Pathologie / Labor",
    "genetics": "Pathologie / Labor",
    "Genetics": "Pathologie / Labor",
    "karyotyping": "Pathologie / Labor",
    "Karyotyping": "Pathologie / Labor",
    "microarrayKaryotyping": "Pathologie / Labor",
    "dnaSexing": "Pathologie / Labor",
    "DNA Sexing": "Pathologie / Labor",
    "Contract Genetic Testing": "Pathologie / Labor",
    "PCR Testing": "Pathologie / Labor",
    "pcrTesting": "Pathologie / Labor",
    "RFT Test": "Pathologie / Labor",
    "Toxicopathology": "Pathologie / Labor",
    "hormones": "Pathologie / Labor",
    "hormonalAssaysLab": "Pathologie / Labor",
    "hormonalAssay": "Pathologie / Labor",
    "hormonalTests": "Pathologie / Labor",
    "autoImmuneTests": "Pathologie / Labor",
    "tumorMarkers": "Pathologie / Labor",
    "immunoassay": "Pathologie / Labor",
    "immunoassays": "Pathologie / Labor",
    "cardiacMarkers": "Pathologie / Labor",
    "bodyFluidExamination": "Pathologie / Labor",
    "epidemiology": "Pathologie / Labor",
    
    # Therapie-nahe Gesundheitsberufe (Therapy-related Health Professions)
    "physicalMedicineAndRehabilitation": "Physiotherapie",
    "physiotherapy": "Physiotherapie",
    "Physiotherapy": "Physiotherapie",
    "Physiotherapist": "Physiotherapie",
    "painMedicinePMR": "Physiotherapie",
    "sportsMedicinePMR": "Physiotherapie",
    "pediatricsSportsMedicine": "Physiotherapie",
    "pediatricRehabilitationMedicine": "Physiotherapie",
    "occupationalAndVocationalRehabilitation": "Physiotherapie",
    "amputeeAndProstheticsAndOrthoticsRehabilitation": "Physiotherapie",
    "neuromuscularMedicinePMR": "Physiotherapie",
    "neuromuscularMedicine": "Physiotherapie",
    "spinalCordInjuryMedicine": "Physiotherapie",
    "pelvicHealthAndUrogynPMR": "Physiotherapie",
    "cardiacAndPulmonaryRehabilitation": "Physiotherapie",
    "geriatricRehabilitation": "Physiotherapie",
    "brainInjuryMedicinePMR": "Physiotherapie",
    "cancerRehabilitation": "Physiotherapie",
    "addictionMedicinePMR": "Physiotherapie",
    "regenerativeMedicinePMR": "Physiotherapie",
    "palliativeRehabilitationAndSupportiveCare": "Physiotherapie",
    "speechMedicinePMR": "Physiotherapie",
    "interventionalPainManagement": "Physiotherapie",
    "jointPreservation": "Physiotherapie",
    "kneePainTreatment": "Physiotherapie",
    "jointInjections": "Physiotherapie",
    "entrapmentNeuropathy": "Physiotherapie",
    "brachialPlexusInjury": "Physiotherapie",
    "carpalTunnelSyndrome": "Physiotherapie",
    "muscleDisordersMyopathyNeuropathy": "Physiotherapie",
    "polytrauma": "Physiotherapie",
    
    "occupationalTherapy": "Ergotherapie",
    "Occupational Therapy": "Ergotherapie",
    "sensoryIntegrationTherapy": "Ergotherapie",
    
    "nutritionAndDietetics": "Ernährungsberatung",
    "dietitian/nutritionist": "Ernährungsberatung",
    "dietitian": "Ernährungsberatung",
    "dietitians": "Ernährungsberatung",
    "nutritionConsultants": "Ernährungsberatung",
    "medicalNutrition": "Ernährungsberatung",
    "nutritionAndWellness": "Ernährungsberatung",
    "weightLossSpecialist": "Ernährungsberatung",
    "weightLossTreatments": "Ernährungsberatung",
    "boneHealth": "Ernährungsberatung",
    
    "obstetricsAndMaternityCare": "Hebamme",
    "maternalFetalMedicineOrPerinatology": "Hebamme",
    "neonatologyPerinatalMedicine": "Hebamme",
    "maternalAndChildHealth": "Hebamme",
    
    # Additional categories
    "reproductiveEndocrinologyAndInfertility": "Gynäkologie / Frauenarzt",
    "reproductiveEndocrinologyAndInfertyInfertility": "Gynäkologie / Frauenarzt",
    "andrologyAndMaleFertility": "Urologie",
    "andrology": "Urologie",
    "Andrology": "Urologie",
    "fertility": "Gynäkologie / Frauenarzt",
    "Fertility": "Gynäkologie / Frauenarzt",
    "Infertility / IVF": "Gynäkologie / Frauenarzt",
    "ivf": "Gynäkologie / Frauenarzt",
    "reproductiveMedicine": "Gynäkologie / Frauenarzt",
    "fertilityPreservation": "Gynäkologie / Frauenarzt",
    "coupleCarrierScreening": "Gynäkologie / Frauenarzt",
    "intrauterineInsemination": "Gynäkologie / Frauenarzt",
    "ovarianRejuvenation": "Gynäkologie / Frauenarzt",
    "stemCellAndPlateletRichPlasmaTherapy": "Gynäkologie / Frauenarzt",
    "preimplantationGeneticTesting": "Gynäkologie / Frauenarzt",
    "exomeSequencing": "Gynäkologie / Frauenarzt",
    "spermDNAFragmentationAnalysis": "Gynäkologie / Frauenarzt",
    "embryoBiopsy": "Gynäkologie / Frauenarzt",
    "laserAssistedHatching": "Gynäkologie / Frauenarzt",
    "endometrialReceptivityArray": "Gynäkologie / Frauenarzt",
    "semenAnalysisWithLeucoscreen": "Gynäkologie / Frauenarzt",
    "geneticCounselling": "Gynäkologie / Frauenarzt",
    "premaStudy": "Gynäkologie / Frauenarzt",
    "familyPlanningAndComplexContraception": "Gynäkologie / Frauenarzt",
    "urogynecologyAndReconstructivePelvisSurgery": "Gynäkologie / Frauenarzt",
    "menopauseAndMidlifeHealth": "Gynäkologie / Frauenarzt",
    "pediatricAndAdolescentGynecology": "Gynäkologie / Frauenarzt",
    "pediatricAdolescentGynecology": "Gynäkologie / Frauenarzt",
    
    "allergyAndImmunology": "Innere Medizin",
    "pediatricAllergyAndImmunology": "Innere Medizin",
    "infectiousDiseases": "Innere Medizin",
    "pediatricInfectiousDiseases": "Innere Medizin",
    "nephrology": "Innere Medizin",
    "Nephrologist": "Innere Medizin",
    "pediatricNephrology": "Innere Medizin",
    "kidneyTransplantation": "Innere Medizin",
    "Chronic Kidney Disease Management": "Innere Medizin",
    "geriatricsInternalMedicine": "Innere Medizin",
    "hospiceAndPalliativeInternalMedicine": "Innere Medizin",
    "hospiceAndPalliativeMedicine": "Innere Medizin",
    "addictionInternalMedicine": "Innere Medizin",
    "addictionMedicinePM": "Innere Medizin",
    "sportsInternalMedicine": "Innere Medizin",
    "sleepMedicine": "Innere Medizin",
    "obstructiveSleepApnoea": "Innere Medizin",
    "chronicDiseasePreventionAndLifestyleMedicine": "Innere Medizin",
    "preventiveMedicine": "Innere Medizin",
    "occupationalAndEnvironmentalMedicine": "Innere Medizin",
    "publicHealth": "Innere Medizin",
    "communityMedicine": "Innere Medizin",
    "communityChildHealth": "Innere Medizin",
    "generalMedicine": "Innere Medizin",
    "medicine": "Innere Medizin",
    "Physician": "Innere Medizin",
    "Cardiologist": "Kardiologie / Herzarzt",
    "Neurologist": "Neurologie",
    
    "headacheMedicine": "Neurologie",
    "Multiple Sclerosis & Other Demyelinating Disorder": "Neurologie",
    "multipleSclerosisOtherDemyelinatingDisorder": "Neurologie",
    "vertigo": "Neurologie",
    "spinalCordDisorders": "Neurologie",
    "neuroinfectiousDisorder": "Neurologie",
    "Traumatic Brain Injury": "Neurologie",
    "tremor": "Neurologie",
    "brainTumor": "Neurologie",
    "brainTrauma": "Neurologie",
    "spineTrauma": "Neurologie",
    "spinalInfection": "Neurologie",
    "spineTumor": "Neurologie",
    "cervicalMyelopathy": "Neurologie",
    
    "jointReconstructionSurgery": "Orthopädie",
    "handOrUpperExtremityAndPeripheralNerveSurgery": "Orthopädie",
    "handAndUpperExtremitiesSurgery": "Orthopädie",
    "handOrUpperExtremitiesAndPeripheralNerveSurgery": "Orthopädie",
    "handOrUpperExtremitiesSurgery": "Orthopädie",
    
    "pediatricNeurodevelopmentalDisabilities": "Kinderarzt / Pädiatrie",
    "developmental–behavioralPediatrics": "Kinderarzt / Pädiatrie",
    "Developmental & Behavioral Paediatrics": "Kinderarzt / Pädiatrie",
    "paediatricMedicalManagement": "Kinderarzt / Pädiatrie",
    "adolescentMedicine": "Kinderarzt / Pädiatrie",
    "adolescentHealth": "Kinderarzt / Pädiatrie",
    "KOUMARABHRITYA": "Kinderarzt / Pädiatrie",
    
    "hairAndNailDisorders": "Dermatologie / Hautarzt",
    "woundHealingAndDermatologicRegenerativeMedicine": "Dermatologie / Hautarzt",
    "autoimmuneDiseases": "Innere Medizin",
    "autoimmune diseases": "Innere Medizin",
    
    "podiatry": "Orthopädie",
    "podiatricOrthopedicsAndBiomechanics": "Orthopädie",
    "podiatricDiabeticLimbSalvageAndWoundCare": "Orthopädie",
    
    "audiology": "HNO",
    "audiologyAndSpeechTherapy": "HNO",
    
    "speechTherapy": "Ergotherapie",
    "speechTherapists": "Ergotherapie",
    "speechAndLanguageTherapy": "Ergotherapie",
    
    "cosmetology": "Dermatologie / Hautarzt",
    "facialAesthetics": "Dermatologie / Hautarzt",
    "facialSpa": "Dermatologie / Hautarzt",
    "antiAging": "Dermatologie / Hautarzt",
    "laserTreatments": "Dermatologie / Hautarzt",
    "bodyContouring": "Dermatologie / Hautarzt",
    "liposuction": "Chirurgie",
    "Gynaecomastia": "Chirurgie",
    
    "sexologist": "Psychiatrie / Psychotherapie",
    "sexualProblems": "Psychiatrie / Psychotherapie",
    "sexualHormonalDisorder": "Endokrinologie",
    "Male Potency / Erectile Dysfunction": "Urologie",
    "Prostate Disease Management": "Urologie",
    "Urinary Cancer": "Urologie",
    "Male Infertility": "Urologie",
    
    "counseling": "Psychiatrie / Psychotherapie",
    "Parent Training & Counselling": "Psychiatrie / Psychotherapie",
    "learningDisabilityTest": "Psychiatrie / Psychotherapie",
    "learningDisability": "Psychiatrie / Psychotherapie",
    "psychologicalAssessment": "Psychiatrie / Psychotherapie",
    "developmentalDelayTreatment": "Kinderarzt / Pädiatrie",
    "familyTherapist": "Psychiatrie / Psychotherapie",
    "substanceAbuseAndAddictionTreatment": "Psychiatrie / Psychotherapie",
    "appliedBehaviourAnalysis": "Psychiatrie / Psychotherapie",
    "behaviourTherapy": "Psychiatrie / Psychotherapie",
    "Autism": "Kinderarzt / Pädiatrie",
    "ADHD": "Kinderarzt / Pädiatrie",
    "neuroOptometricFunctionalVisionAssessment": "Augenarzt / Ophthalmologie",
    "Early Intervention": "Kinderarzt / Pädiatrie",
    
    "homeopathy": "Alternativmedizin",
    "Homeopath": "Alternativmedizin",
    "Ayurveda": "Alternativmedizin",
    "Ayurvedic Clinic": "Alternativmedizin",
    "naturopathy": "Alternativmedizin",
    "alternativeMedicine": "Alternativmedizin",
    "acupunctureTherapists": "Alternativmedizin",
    
    "pharmacy": "Apotheke",
    "physiologist": "Physiotherapie",
    "clinicalInformatics": "Innere Medizin",
    
    "specialEducation": "Ergotherapie",
    "underseaAndHyperbaricMedicine": "Notfallmedizin",
    "burns": "Chirurgie",
    "realignmentProcedures": "Orthopädie",
    "disclusionTimeReduction": "Zahnarzt",
    "Dentures": "Zahnarzt",
    "All on 4/6 implant": "Zahnarzt",
    "Veneers & Laminates": "Zahnarzt",
    
    "veterinaryHospital": "Tierarzt",
}

# Facility type mapping
FACILITY_TYPE_MAP = {
    "clinic": "clinic",
    "hospital": "hospital",
    "dentist": "dentist",
    "doctor": "doctor",
    "farmacy": "pharmacy",
    "pharmacy": "pharmacy",
}

GERMAN_TO_ENGLISH_DIAGNOSIS = {
    "Hausarzt / Allgemeinmedizin": "Primary Care / General Practice",
    "Innere Medizin": "Internal Medicine",
    "Kinderarzt / Pädiatrie": "Pediatrics",
    "Gynäkologie / Frauenarzt": "Gynecology",
    "Dermatologie / Hautarzt": "Dermatology",
    "Kardiologie / Herzarzt": "Cardiology",
    "Orthopädie": "Orthopedics",
    "Neurologie": "Neurology",
    "Psychiatrie / Psychotherapie": "Psychiatry / Psychotherapy",
    "HNO": "ENT",
    "Augenarzt / Ophthalmologie": "Ophthalmology",
    "Urologie": "Urology",
    "Gastroenterologie": "Gastroenterology",
    "Endokrinologie": "Endocrinology",
    "Rheumatologie": "Rheumatology",
    "Pneumologie": "Pulmonology",
    "Onkologie": "Oncology",
    "Zahnarzt": "Dentistry",
    "Kieferorthopädie": "Orthodontics",
    "Oralchirurgie": "Oral Surgery",
    "Notfallmedizin": "Emergency Medicine",
    "Chirurgie": "Surgery",
    "Radiologie": "Radiology",
    "Anästhesiologie": "Anesthesiology",
    "Intensivmedizin": "Intensive Care",
    "Pathologie / Labor": "Pathology / Laboratory Medicine",
    "Physiotherapie": "Physiotherapy",
    "Ergotherapie": "Occupational Therapy",
    "Ernährungsberatung": "Nutrition Counseling",
    "Hebamme": "Midwifery",
    "Alternativmedizin": "Alternative Medicine",
    "Apotheke": "Pharmacy",
    "Tierarzt": "Veterinary Medicine",
    "Allgemeinmedizin": "Primary Care / General Practice",
}


def map_specialty_to_diagnosis(specialties: List[str]) -> str:
    """Map list of specialties to a single canonical English diagnosis category."""
    if not specialties:
        return "Primary Care / General Practice"
    
    # Try to find the first specialty that maps to a German category
    for specialty in specialties:
        if specialty in SPECIALTY_TO_DIAGNOSIS:
            german_category = SPECIALTY_TO_DIAGNOSIS[specialty]
            return GERMAN_TO_ENGLISH_DIAGNOSIS.get(german_category, german_category)
    
    # If no mapping found, return the first specialty or default
    return "Primary Care / General Practice"


def parse_specialties(specialty_str: str) -> List[str]:
    """Parse specialty string from CSV (JSON array format)."""
    if not specialty_str or specialty_str == "null" or specialty_str == "[]":
        return []
    
    try:
        specs = json.loads(specialty_str)
        return specs if isinstance(specs, list) else []
    except json.JSONDecodeError:
        return []


def safe_float(value: Any) -> Optional[float]:
    """Safely convert value to float."""
    if not value or value == "null":
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def compute_trustworthy_score(row: Dict[str, Any]) -> float:
    def is_filled(value: Any) -> bool:
        if value is None:
            return False
        text = str(value).strip().lower()
        return text not in {"", "nan", "none", "null"}

    total_fields = len(row)
    filled_fields = sum(1 for value in row.values() if is_filled(value))
    score = (filled_fields / total_fields) if total_fields else 0.0

    critical_groups = {
        "name": ("doctor_name", "name"),
        "latitude": ("latitude",),
        "longitude": ("longitude",),
    }
    missing_critical = [
        label
        for label, candidates in critical_groups.items()
        if not any(is_filled(row.get(field)) for field in candidates)
    ]
    if missing_critical:
        score = min(score, 0.2)

    return round(10 * score, 2)


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate the great circle distance between two points on Earth in kilometers."""
    R = 6371  # Earth radius in kilometers
    
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    delta_lat = math.radians(lat2 - lat1)
    delta_lon = math.radians(lon2 - lon1)
    
    a = math.sin(delta_lat / 2) ** 2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    
    return R * c


# Inconsistency check constants
MODEL = "gpt-4.1-mini"

INCONSISTENCY_CHECK_COLUMNS = [
    "numberDoctors",
    "description",
    "capacity",
    "specialties",
    "procedure",
    "equipment",
    "capability",
]

SYSTEM_PROMPT = """
You are an expert healthcare data validation agent.

Evaluate the internal consistency of ONE medical facility row in India.

Return only:
- consistency: exactly one of "Valid", "Suspicious", "Contradictory"
- consistency_flags: max ~10 words

Be strict but realistic.
Do NOT hallucinate missing information.
Missing numberDoctors, equipment, procedure, or capacity alone is NOT suspicious.
Only flag when provided fields create a clear mismatch.
Prefer "Suspicious" over "Contradictory" if uncertain.

Reason across:
- staff vs services
- procedure vs equipment
- capacity vs staff
- capability vs equipment
- description vs structured fields
- overclaiming
"""

INCONSISTENCY_SCHEMA = {
    "type": "object",
    "properties": {
        "consistency": {
            "type": "string",
            "enum": ["Valid", "Suspicious", "Contradictory"],
        },
        "consistency_flags": {
            "type": "string",
        },
    },
    "required": ["consistency", "consistency_flags"],
    "additionalProperties": False,
}


def clean_value(x):
    """Clean value for inconsistency check."""
    if x is None or x == "" or str(x).strip().lower() in {"nan", "none", "null"}:
        return ""
    return str(x)[:3000]


def check_consistency(row: Dict[str, Any], client: Optional[OpenAI] = None) -> Dict[str, str]:
    """Run inconsistency check on a single row using OpenAI API."""
    if client is None:
        # Return default values if no client is provided
        return {
            "consistency": "Valid",
            "consistency_flags": ""
        }
    
    row_payload = {col: clean_value(row.get(col, "")) for col in INCONSISTENCY_CHECK_COLUMNS}

    try:
        response = client.responses.create(
            model=MODEL,
            input=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": json.dumps(row_payload, ensure_ascii=False)},
            ],
            text={
                "format": {
                    "type": "json_schema",
                    "name": "consistency_check",
                    "schema": INCONSISTENCY_SCHEMA,
                    "strict": True,
                }
            },
            temperature=0,
        )
        result = json.loads(response.output_text)
    except Exception as e:
        result = {
            "consistency": "Suspicious",
            "consistency_flags": f"API error: {str(e)[:40]}",
        }
        time.sleep(2)
    
    return result


def process_csv_to_dataset(
    csv_path: Path,
    output_path: Path,
    limit: Optional[int] = None,
    latitude: Optional[float] = None,
    longitude: Optional[float] = None,
    top: Optional[int] = None,
    run_consistency_check: bool = False,
    fetch_reviews: bool = False,
) -> None:
    """Process data_full.csv and create dataset.json.
    
    Args:
        csv_path: Path to input CSV file
        output_path: Path to output JSON file
        limit: Maximum number of records to process (for testing)
        latitude: Reference latitude for distance calculation
        longitude: Reference longitude for distance calculation
        top: If specified with lat/lon, only keep top N closest records
        run_consistency_check: Whether to run inconsistency check on records
        fetch_reviews: Whether to fetch Google Places rating and review count
    """
    
    # Initialize OpenAI client if consistency check is requested
    client = None
    if run_consistency_check:
        api_key = os.environ.get("OPENAI_API_KEY")
        if not api_key:
            print("Warning: OPENAI_API_KEY not set. Skipping consistency check.")
            run_consistency_check = False
        else:
            client = OpenAI(api_key=api_key)
    
    # Check for Google Places API key if reviews are requested
    google_api_key = None
    if fetch_reviews:
        google_api_key = os.environ.get("GOOGLE_PLACES_API_KEY")
        if not google_api_key:
            print("Warning: GOOGLE_PLACES_API_KEY not set. Skipping review fetching.")
            fetch_reviews = False
    
    records = []
    
    with open(csv_path, 'r', encoding='utf-8') as f:
        cleaned_lines = (line.replace("\x00", "") for line in f)
        reader = csv.DictReader(cleaned_lines)
        
        for idx, row in enumerate(reader):
            if limit and idx >= limit:
                break
            
            # Parse specialties
            specialty_str = row.get('specialties', '')
            specialties = parse_specialties(specialty_str)
            
            # Map to diagnosis
            diagnosis = map_specialty_to_diagnosis(specialties)
            
            # Get coordinates
            lat = safe_float(row.get('latitude'))
            lon = safe_float(row.get('longitude'))
            
            # Skip records without coordinates
            if lat is None or lon is None:
                continue
            
            # Get facility type
            facility_type = FACILITY_TYPE_MAP.get(
                row.get('facilityTypeId', '').strip(),
                'clinic'
            )
            
            trustworthy_score = compute_trustworthy_score(row)

            # Create record
            record = {
                'name': row.get('name', ''),
                'longitude': lon,
                'latitude': lat,
                'type': facility_type,
                'diagnosis': diagnosis,
                'trustworthy_score': trustworthy_score,
                'description': row.get('description', ''),
                'consistency': 'Valid',  # Default value
                'consistency_flags': '',  # Default value
                'google_rating': None,  # Default value
                'google_rating_count': None,  # Default value
                '_csv_row': row,  # Store original row for later consistency check and review fetching
            }
            
            # Calculate distance if reference location provided
            if latitude is not None and longitude is not None:
                distance = haversine_distance(latitude, longitude, lat, lon)
                record['_distance'] = distance
            
            records.append(record)
    
    # Filter by distance if top N requested
    if top is not None and latitude is not None and longitude is not None:
        print(f"\nFiltering to top {top} closest records...")
        records.sort(key=lambda x: x.get('_distance', float('inf')))
        records = records[:top]
    
    # Fetch Google Places reviews on filtered records
    if fetch_reviews and records and google_api_key:
        print(f"\nFetching Google Places ratings for {len(records)} records...")
        for record in tqdm(records, desc="Fetching reviews"):
            review_data = fetch_google_place_rating(record['_csv_row'], google_api_key)
            record['google_rating'] = review_data['google_rating']
            record['google_rating_count'] = review_data['google_rating_count']
            time.sleep(0.1)  # Small delay to avoid rate limiting
    
    # Run consistency check on filtered records
    if run_consistency_check and records:
        print(f"\nRunning consistency check on {len(records)} records...")
        for record in tqdm(records, desc="Checking consistency"):
            consistency_result = check_consistency(record['_csv_row'], client)
            record['consistency'] = consistency_result['consistency']
            record['consistency_flags'] = consistency_result['consistency_flags']
    
    # Clean up temporary fields
    for record in records:
        record.pop('_csv_row', None)
        record.pop('_distance', None)
    
    # Write output
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(records, f, indent=2, ensure_ascii=False)
    
    print(f"\nProcessed {len(records)} records")
    print(f"Output written to: {output_path}")
    
    if run_consistency_check:
        # Print consistency stats
        consistency_counts = {}
        for record in records:
            cons = record.get('consistency', 'Valid')
            consistency_counts[cons] = consistency_counts.get(cons, 0) + 1
        
        print("\nConsistency check results:")
        for status, count in sorted(consistency_counts.items()):
            print(f"  {status}: {count}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Convert data_full.csv to dataset.json with optional filtering and consistency checking."
    )
    parser.add_argument(
        "--latitude",
        type=float,
        help="Reference latitude for distance calculation"
    )
    parser.add_argument(
        "--longitude",
        type=float,
        help="Reference longitude for distance calculation"
    )
    parser.add_argument(
        "--top",
        type=int,
        help="Number of closest records to keep (requires --latitude and --longitude)"
    )
    parser.add_argument(
        "--check-consistency",
        action="store_true",
        help="Run inconsistency check on the records (requires OPENAI_API_KEY)"
    )
    parser.add_argument(
        "--fetch-reviews",
        action="store_true",
        help="Fetch Google Places rating and review count (requires GOOGLE_PLACES_API_KEY)"
    )
    parser.add_argument(
        "--name",
        type=str,
        default="dataset",
        help="Name for the output dataset file (without .json extension, default: dataset)"
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Maximum number of records to process from CSV (for testing)"
    )
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.top is not None and (args.latitude is None or args.longitude is None):
        parser.error("--top requires both --latitude and --longitude")
    
    project_root = Path(__file__).resolve().parents[1]
    csv_path = project_root / "data" / "data_source" / "data_full.csv"
    output_path = project_root / "data" / f"{args.name}.json"
    
    # Process dataset with filters
    process_csv_to_dataset(
        csv_path,
        output_path,
        limit=args.limit,
        latitude=args.latitude,
        longitude=args.longitude,
        top=args.top,
        run_consistency_check=args.check_consistency,
        fetch_reviews=args.fetch_reviews,
    )
    
    # Print statistics
    print("\n=== Specialty Statistics ===")
    diagnosis_counts: Dict[str, int] = {}
    
    with open(output_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
        for record in data:
            diagnosis = record['diagnosis']
            diagnosis_counts[diagnosis] = diagnosis_counts.get(diagnosis, 0) + 1
    
    print("\nDiagnosis distribution:")
    for diagnosis, count in sorted(diagnosis_counts.items(), key=lambda x: -x[1]):
        print(f"  {count:>5} {diagnosis}")


if __name__ == "__main__":
    main()
