from __future__ import annotations

import re
from dataclasses import dataclass
from difflib import SequenceMatcher


TOKEN_RE = re.compile(r"[a-z0-9]+")


@dataclass(frozen=True)
class DiagnosisCategory:
    english_name: str
    german_name: str
    aliases: tuple[str, ...]
    keywords: tuple[str, ...]


@dataclass(frozen=True)
class DiagnosisMatch:
    category: DiagnosisCategory
    score: float
    reason: str

    @property
    def english_name(self) -> str:
        return self.category.english_name


DIAGNOSIS_CATEGORIES: tuple[DiagnosisCategory, ...] = (
    DiagnosisCategory(
        "Primary Care / General Practice",
        "Hausarzt / Allgemeinmedizin",
        ("primary care", "general practice", "general practitioner", "family doctor", "hausarzt"),
        ("fever", "cold", "cough", "checkup", "general", "family", "infection"),
    ),
    DiagnosisCategory(
        "Internal Medicine",
        "Innere Medizin",
        ("internal medicine", "internist", "innere medizin"),
        ("diabetes", "hypertension", "blood pressure", "kidney", "adult medicine"),
    ),
    DiagnosisCategory(
        "Pediatrics",
        "Kinderarzt / Pädiatrie",
        ("pediatrics", "paediatrics", "pediatrician", "child doctor", "kinderarzt"),
        ("child", "baby", "infant", "newborn", "teen", "adolescent"),
    ),
    DiagnosisCategory(
        "Gynecology",
        "Gynäkologie / Frauenarzt",
        ("gynecology", "gynaecology", "obgyn", "women's health", "frauenarzt"),
        ("pregnancy", "period", "ovary", "uterus", "fertility", "vaginal"),
    ),
    DiagnosisCategory(
        "Dermatology",
        "Dermatologie / Hautarzt",
        ("dermatology", "dermatologist", "skin doctor", "hautarzt"),
        ("skin", "rash", "acne", "eczema", "psoriasis", "mole", "hair", "nail"),
    ),
    DiagnosisCategory(
        "Cardiology",
        "Kardiologie / Herzarzt",
        ("cardiology", "cardiologist", "heart doctor", "herzarzt"),
        ("heart", "chest pain", "palpitation", "arrhythmia", "cardiac"),
    ),
    DiagnosisCategory(
        "Orthopedics",
        "Orthopädie",
        ("orthopedics", "orthopaedics", "orthopedic", "bone doctor"),
        ("bone", "joint", "knee", "back pain", "spine", "fracture", "shoulder", "hip"),
    ),
    DiagnosisCategory(
        "Neurology",
        "Neurologie",
        ("neurology", "neurologist"),
        ("headache", "migraine", "seizure", "stroke", "nerve", "brain", "tremor", "vertigo"),
    ),
    DiagnosisCategory(
        "Psychiatry / Psychotherapy",
        "Psychiatrie / Psychotherapie",
        ("psychiatry", "psychotherapy", "therapist", "mental health", "psychiatrist"),
        ("depression", "anxiety", "panic", "adhd", "autism", "stress", "trauma", "counseling"),
    ),
    DiagnosisCategory(
        "ENT",
        "HNO",
        ("ent", "ear nose throat", "otolaryngology", "hno"),
        ("ear", "nose", "throat", "sinus", "tonsil", "hearing", "voice"),
    ),
    DiagnosisCategory(
        "Ophthalmology",
        "Augenarzt / Ophthalmologie",
        ("ophthalmology", "ophthalmologist", "eye doctor", "augenarzt"),
        ("eye", "vision", "glasses", "cataract", "retina", "glaucoma"),
    ),
    DiagnosisCategory(
        "Urology",
        "Urologie",
        ("urology", "urologist"),
        ("urine", "bladder", "prostate", "kidney stone", "erectile", "male fertility"),
    ),
    DiagnosisCategory(
        "Gastroenterology",
        "Gastroenterologie",
        ("gastroenterology", "gastroenterologist", "digestive doctor"),
        ("stomach", "abdomen", "liver", "gut", "digestive", "colon", "constipation"),
    ),
    DiagnosisCategory(
        "Endocrinology",
        "Endokrinologie",
        ("endocrinology", "endocrinologist"),
        ("thyroid", "hormone", "diabetes", "metabolism", "pituitary"),
    ),
    DiagnosisCategory(
        "Rheumatology",
        "Rheumatologie",
        ("rheumatology", "rheumatologist"),
        ("arthritis", "autoimmune", "joint inflammation", "lupus"),
    ),
    DiagnosisCategory(
        "Pulmonology",
        "Pneumologie",
        ("pulmonology", "pulmonologist", "lung doctor"),
        ("lung", "breathing", "asthma", "copd", "shortness of breath"),
    ),
    DiagnosisCategory(
        "Oncology",
        "Onkologie",
        ("oncology", "oncologist", "cancer doctor"),
        ("cancer", "tumor", "chemotherapy", "radiation"),
    ),
    DiagnosisCategory(
        "Dentistry",
        "Zahnarzt",
        ("dentistry", "dentist", "tooth doctor", "zahnarzt"),
        ("tooth", "teeth", "gum", "cavity", "root canal", "dental", "braces"),
    ),
    DiagnosisCategory(
        "Orthodontics",
        "Kieferorthopädie",
        ("orthodontics", "orthodontist", "braces", "invisalign"),
        ("braces", "alignment", "bite", "teeth straightening"),
    ),
    DiagnosisCategory(
        "Oral Surgery",
        "Oralchirurgie",
        ("oral surgery", "maxillofacial surgery", "oral surgeon"),
        ("wisdom tooth", "jaw surgery", "implant surgery", "tooth extraction"),
    ),
    DiagnosisCategory(
        "Emergency Medicine",
        "Notfallmedizin",
        ("emergency medicine", "emergency", "er", "urgent care"),
        ("emergency", "urgent", "accident", "trauma", "bleeding", "critical"),
    ),
    DiagnosisCategory(
        "Surgery",
        "Chirurgie",
        ("surgery", "surgeon", "chirurgie"),
        ("operation", "surgical", "appendix", "hernia", "procedure"),
    ),
    DiagnosisCategory(
        "Radiology",
        "Radiologie",
        ("radiology", "radiologist", "imaging"),
        ("xray", "x-ray", "mri", "ct scan", "ultrasound", "scan"),
    ),
    DiagnosisCategory(
        "Anesthesiology",
        "Anästhesiologie",
        ("anesthesiology", "anaesthesiology", "anesthesia"),
        ("anesthesia", "sedation", "operative pain"),
    ),
    DiagnosisCategory(
        "Intensive Care",
        "Intensivmedizin",
        ("intensive care", "icu", "critical care"),
        ("icu", "ventilator", "critical condition"),
    ),
    DiagnosisCategory(
        "Pathology / Laboratory Medicine",
        "Pathologie / Labor",
        ("pathology", "lab", "laboratory medicine"),
        ("biopsy", "blood test", "lab test", "histology", "cytology"),
    ),
    DiagnosisCategory(
        "Physiotherapy",
        "Physiotherapie",
        ("physiotherapy", "physical therapy", "physiotherapist"),
        ("rehab", "rehabilitation", "mobility", "exercise therapy"),
    ),
    DiagnosisCategory(
        "Occupational Therapy",
        "Ergotherapie",
        ("occupational therapy", "ergotherapy"),
        ("daily living", "fine motor", "sensory integration", "speech therapy"),
    ),
    DiagnosisCategory(
        "Nutrition Counseling",
        "Ernährungsberatung",
        ("nutrition counseling", "dietitian", "nutritionist", "dietician"),
        ("diet", "nutrition", "weight loss", "meal plan"),
    ),
    DiagnosisCategory(
        "Midwifery",
        "Hebamme",
        ("midwifery", "midwife", "hebamme"),
        ("prenatal", "postpartum", "birth support", "maternity"),
    ),
    DiagnosisCategory(
        "Alternative Medicine",
        "Alternativmedizin",
        ("alternative medicine", "homeopathy", "ayurveda", "naturopathy"),
        ("holistic", "homeopathic", "ayurvedic", "acupuncture"),
    ),
    DiagnosisCategory(
        "Pharmacy",
        "Apotheke",
        ("pharmacy", "pharmacist", "apotheke"),
        ("medicine pickup", "prescription", "medication"),
    ),
    DiagnosisCategory(
        "Veterinary Medicine",
        "Tierarzt",
        ("veterinary medicine", "vet", "animal doctor", "tierarzt"),
        ("dog", "cat", "pet", "animal"),
    ),
)


def available_diagnosis_names() -> list[str]:
    return [category.english_name for category in DIAGNOSIS_CATEGORIES]


def classify_diagnosis(text: str) -> DiagnosisMatch:
    normalized_text = _normalize_text(text)
    text_token_list = _tokens(text)
    text_tokens = set(text_token_list)
    best_category = DIAGNOSIS_CATEGORIES[0]
    best_score = 0.0
    best_reason = "default fallback"

    for category in DIAGNOSIS_CATEGORIES:
        score, reason = _score_category(
            category, normalized_text, text_token_list, text_tokens
        )
        if score > best_score:
            best_category = category
            best_score = score
            best_reason = reason

    if best_score == 0:
        best_reason = "no strong specialist signal found, defaulted to primary care"

    return DiagnosisMatch(best_category, round(best_score, 3), best_reason)


def _score_category(
    category: DiagnosisCategory,
    normalized_text: str,
    text_token_list: list[str],
    text_tokens: set[str],
) -> tuple[float, str]:
    best_reason = "name similarity"
    score = 0.0

    for alias in (category.english_name, category.german_name, *category.aliases):
        alias_normalized = _normalize_text(alias)
        alias_tokens = _tokens(alias)
        if alias_tokens and _contains_token_phrase(text_token_list, alias_tokens):
            phrase_score = 6 + len(alias_normalized.split())
            if phrase_score > score:
                score = float(phrase_score)
                best_reason = f"matched phrase '{alias}'"

        similarity = SequenceMatcher(None, normalized_text, alias_normalized).ratio()
        similarity_score = similarity * 4
        if similarity_score > score:
            score = similarity_score
            best_reason = f"closest label '{alias}'"

    keyword_hits = 0
    for keyword in category.keywords:
        keyword_tokens = set(_tokens(keyword))
        if keyword_tokens and keyword_tokens.issubset(text_tokens):
            keyword_hits += 1

    if keyword_hits:
        keyword_score = 2.5 + keyword_hits * 1.5
        if keyword_score > score:
            score = keyword_score
            best_reason = f"matched {keyword_hits} symptom keyword(s)"

    return score, best_reason


def _normalize_text(text: str) -> str:
    return " ".join(_tokens(text))


def _tokens(text: str) -> list[str]:
    return TOKEN_RE.findall(text.casefold())


def _contains_token_phrase(text_tokens: list[str], phrase_tokens: list[str]) -> bool:
    if not phrase_tokens or len(phrase_tokens) > len(text_tokens):
        return False

    phrase_length = len(phrase_tokens)
    for index in range(len(text_tokens) - phrase_length + 1):
        if text_tokens[index : index + phrase_length] == phrase_tokens:
            return True
    return False
