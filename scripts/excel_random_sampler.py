import pandas as pd

# ===== SETTINGS (just change these) =====
INPUT_FILE = "VF_Hackathon_Dataset_India_Large.xlsx"
N_SAMPLES = 50
OUTPUT_FILE = "Small_Dataset_N=" + str(N_SAMPLES) + ".xlsx"
RANDOM_SEED = None   # set to e.g. 42 if you want reproducible results
# =======================================

def main():
    # Load Excel file
    df = pd.read_excel(INPUT_FILE)

    # Safety check
    if N_SAMPLES > len(df):
        raise ValueError(f"Requested {N_SAMPLES} rows, but dataset only has {len(df)} rows.")

    # Sample random rows
    sampled_df = df.sample(n=N_SAMPLES, random_state=RANDOM_SEED)

    # Save result
    sampled_df.to_excel(OUTPUT_FILE, index=False)

    print(f"Done. Saved {N_SAMPLES} random rows to '{OUTPUT_FILE}'")

if __name__ == "__main__":
    main()