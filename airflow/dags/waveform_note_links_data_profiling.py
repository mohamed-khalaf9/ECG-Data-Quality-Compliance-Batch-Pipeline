import pandas as pd
import contextlib

# 1. load the first 50k
print("loading raw data...")
df = pd.read_csv("waveform_note_links.csv", nrows=50000)

output_file = "waveform_note_links_report.md"

with open(output_file, "w") as f:
    with contextlib.redirect_stdout(f):
    
        print("# Waveform Note Links Profiling Report\n")

        print("## Overview")
        print("- **Rows:**", len(df))
        print("- **Columns:**", len(df.columns))
        print("\n**Data types:**\n", df.dtypes)

        print("\n## subject_id")
        print("- **nulls:**", df["subject_id"].isna().sum())
        print("- **format valid (digits only):**", df["subject_id"].dropna().astype(str).str.match(r"^\d+$").all())
        print("- **duplicates:**", df["subject_id"].duplicated().sum())

        print("\n## study_id")
        print("- **nulls:**", df["study_id"].isna().sum())
        print("- **format valid (digits only):**", df["study_id"].dropna().astype(str).str.match(r"^\d+$").all())
        print("- **unique check:**", df["study_id"].is_unique)
        print("- **duplicates count:**", df["study_id"].duplicated().sum())
        print("- **unique ratio:**", f"{df['study_id'].nunique() / len(df):.4f}")

        print("\n## subject_id + study_id")
        print("- **duplicate pairs:**", df.duplicated(subset=["subject_id", "study_id"]).sum())
        print("- **unique ratio:**", f"{df.drop_duplicates(subset=['subject_id', 'study_id']).shape[0] / len(df):.4f}")

        print("\n## waveform_path")
        print("- **nulls:**", df["waveform_path"].isna().sum())
        print("- **format valid:**", df["waveform_path"].dropna().astype(str).str.match(r"^files/p\d+/p\d+/s\d+/\d+$").all())
        print("- **unique check:**", df["waveform_path"].is_unique)
        print("- **duplicates count:**", df["waveform_path"].duplicated().sum())
        print("- **unique ratio:**", f"{df['waveform_path'].nunique() / len(df):.4f}")

        # consistency check with study_id
        path_matches = df.apply(lambda x: str(x["study_id"]) in str(x["waveform_path"]), axis=1)
        print("- **study_id in path:**", path_matches.all())

        print("\n## note_id")
        print("- **nulls:**", df["note_id"].isna().sum())
        print("- **unique:**", df["note_id"].is_unique)
        print("- **format valid:**", df["note_id"].dropna().astype(str).str.match(r"^\d+-EK-\d+$").all())
        print("- **duplicates count:**", df["note_id"].duplicated().sum())
        print("- **unique ratio:**", f"{df['note_id'].nunique() / len(df):.4f}")

        print("\n## note_seq")
        print("- **nulls:**", df["note_seq"].isna().sum())
        print("- **min:**", df["note_seq"].min())
        print("- **max:**", df["note_seq"].max())

        print("\n## charttime")
        df["charttime_parsed"] = pd.to_datetime(df["charttime"], errors="coerce")
        print("- **nulls after parsing:**", df["charttime_parsed"].isna().sum())
        print("- **format valid:**", df["charttime_parsed"].notna().all())


print(f"Done! Check the '{output_file}' file in your folder.")