from pathlib import Path
import pandas as pd

# Folder containing your Excel file(s)
base_dir = Path(r"/data") #change path

# load the Excel file
#excel_file = 'VF_Hackathon_Dataset_India_Large.xlsx'
excel_file = 'VF_Hackathon_Dataset_India_Large.xlsx'

file_path = base_dir / excel_file
print(f"Loading: {file_path.name}")

# =========================================================
# CONFIG: Define which methods to use and their weights
# =========================================================
SCORING_CONFIG = [
    {"name": "completeness", "weight": 0.3},
]
  
# =========================================================
# SCORING ENGINE
# =========================================================

def compute_trust_score(row):
    total_weight = sum(item["weight"] for item in SCORING_CONFIG)   # normalize weights if they don't sum to 1

    weighted_sum = 0
    all_reasons = []
    subscores = {}

    for item in SCORING_CONFIG:
        method_name = item["name"]
        weight = item["weight"]

        func = METHODS[method_name] # get the scoring function

        score, reasons = func(row)  # compute score and reasons for this method

        # clamp score to [0,1]  
        score = max(0, min(1, score))

        weighted_sum += weight * score
        subscores[item["name"]] = score

        if reasons:
            all_reasons.extend(reasons)

    final_score = 10 * weighted_sum / total_weight
    critical_missing_flag = any(reason.startswith("FLAG:") for reason in all_reasons)

    return pd.Series({
        "trust_score": round(final_score, 2),
        "critical_missing_flag": critical_missing_flag,
        "subscores": subscores,
        "reasons": all_reasons
    })


# =========================================================
# METHODS (define scoring logic here)
# =========================================================

def score_completeness(row):
    def is_filled(value):
        if pd.isna(value):
            return False
        text = str(value).strip().lower()
        return text not in ["", "nan", "none", "null"]

    total_fields = len(row.index)
    filled_fields = sum(is_filled(row.get(col)) for col in row.index)
    score = (filled_fields / total_fields) if total_fields else 0.0

    reasons = []
    if score < 0.5:
        reasons.append("Many fields are missing overall.")

    # Hard fail flag if critical information is missing
    critical_groups = {
        "name": ["doctor_name", "name"],
        "latitude": ["latitude"],
        "longitude": ["longitude"]
    }

    missing_critical = []
    for label, candidates in critical_groups.items():
        if not any(is_filled(row.get(field)) for field in candidates):
            missing_critical.append(label)

    if missing_critical:
        reasons.append(f"FLAG: Critical information missing ({', '.join(missing_critical)}).")
        score = min(score, 0.2)

    return score, reasons


# =========================================================
# METHOD REGISTRY
# =========================================================

METHODS = {
    "completeness": score_completeness
}

# =========================================================
# MAIN: Load Excel and calculate scores
# =========================================================

# Load source Excel file
df = pd.read_excel(file_path)

# Calculate scores
results = df.apply(compute_trust_score, axis=1)

# Append results
df = pd.concat([df, results], axis=1)

# Save to a new Excel file (original file remains unchanged)
output_file_path = base_dir / f"{file_path.stem}_scored.xlsx"
df.to_excel(output_file_path, index=False)

print(f"Done! Saved to {output_file_path.name}")