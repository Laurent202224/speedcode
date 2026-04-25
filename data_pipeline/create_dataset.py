#!/usr/bin/env python3
"""
Data pipeline script to convert data_full.csv to dataset.json
Maps specialties to German doctor categories (diagnosis field)
"""

import csv
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

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


def map_specialty_to_diagnosis(specialties: List[str]) -> str:
    """Map list of specialties to a single German diagnosis category."""
    if not specialties:
        return "Allgemeinmedizin"
    
    # Try to find the first specialty that maps to a German category
    for specialty in specialties:
        if specialty in SPECIALTY_TO_DIAGNOSIS:
            return SPECIALTY_TO_DIAGNOSIS[specialty]
    
    # If no mapping found, return the first specialty or default
    return specialties[0] if specialties else "Allgemeinmedizin"


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


def process_csv_to_dataset(
    csv_path: Path,
    output_path: Path,
    limit: Optional[int] = None
) -> None:
    """Process data_full.csv and create dataset.json."""
    
    records = []
    
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        for idx, row in enumerate(reader):
            if limit and idx >= limit:
                break
            
            # Parse specialties
            specialty_str = row.get('specialties', '')
            specialties = parse_specialties(specialty_str)
            
            # Map to diagnosis
            diagnosis = map_specialty_to_diagnosis(specialties)
            
            # Get coordinates
            latitude = safe_float(row.get('latitude'))
            longitude = safe_float(row.get('longitude'))
            
            # Skip records without coordinates
            if latitude is None or longitude is None:
                continue
            
            # Get facility type
            facility_type = FACILITY_TYPE_MAP.get(
                row.get('facilityTypeId', '').strip(),
                'clinic'
            )
            
            # Create record
            record = {
                'name': row.get('name', ''),
                'longitude': longitude,
                'latitude': latitude,
                'type': facility_type,
                'diagnosis': diagnosis,
                'trustworthy_score': 0.8,  # Default value
                'description': row.get('description', ''),
            }
            
            records.append(record)
    
    # Write output
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(records, f, indent=2, ensure_ascii=False)
    
    print(f"Processed {len(records)} records")
    print(f"Output written to: {output_path}")


def main():
    """Main entry point."""
    project_root = Path(__file__).resolve().parents[1]
    csv_path = project_root / "data" / "data_source" / "data_full.csv"
    output_path = project_root / "data" / "dataset.json"
    
    # Process full dataset (or set limit for testing)
    process_csv_to_dataset(csv_path, output_path)
    
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
