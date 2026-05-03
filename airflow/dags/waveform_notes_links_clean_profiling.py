import pandas as pd
import glob
import contextlib


file_path = glob.glob("output/waveform_note_links_clean*.csv")

if not file_path:
    print("Could not find the file")
    exit()

print(f"loading cleaned data from {file_path[0]}...")
df = pd.read_csv(file_path[0])

output_file = "clean_waveform_note_links_report.md"

with open(output_file, "w") as f:
    with contextlib.redirect_stdout(f):
    
        print("# Clean Waveform Note Links Profiling Report\n")

        print("## Overview")
        print("- **Rows:**", len(df))
        print("- **Columns:**", len(df.columns))

        print("\n## subject_id")
        print("- **nulls:**", df["subject_id"].isna().sum())
        print("- **format valid (digits only):**", df["subject_id"].dropna().astype(str).str.match(r"^\d+$").all())

        print("\n## study_id")
        print("- **nulls:**", df["study_id"].isna().sum())
        print("- **format valid (digits only):**", df["study_id"].dropna().astype(str).str.match(r"^\d+$").all())
        
        print("- **unique check (Expect True):**", df["study_id"].is_unique) 
        
        print("- **duplicates count (Expect 0):**", df["study_id"].duplicated().sum())

        print("\n## waveform_path")
        print("- **nulls:**", df["waveform_path"].isna().sum())
        print("- **format valid:**", df["waveform_path"].dropna().astype(str).str.match(r"^files/p\d+/p\d+/s\d+/\d+$").all())
        
        # consistency check with study_id
        path_matches = df.apply(lambda x: str(x["study_id"]) in str(x["waveform_path"]), axis=1)
        print("- **study_id in path:**", path_matches.all())

        print("\n## note_id")
        print("- **nulls:**", df["note_id"].isna().sum())
        print("- **format valid:**", df["note_id"].dropna().astype(str).str.match(r"^\d+-EK-\d+$").all())

        print("\n## note_seq")
        print("- **nulls:**", df["note_seq"].isna().sum())

        print("\n## charttime")
        df["charttime_parsed"] = pd.to_datetime(df["charttime"], errors="coerce")
        print("- **nulls after parsing:**", df["charttime_parsed"].isna().sum())
        print("- **format valid:**", df["charttime_parsed"].notna().all())


print(f"Done, the '{output_file}' file in your folder.")