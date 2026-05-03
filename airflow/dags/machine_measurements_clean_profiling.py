import pandas as pd
import glob
import contextlib


print("loading clean data...")
try:
    file_path = glob.glob("output/machine_measurements_clean.csv")[0]
    df = pd.read_csv(file_path)
    print(f"Successfully loaded: {file_path}")
except IndexError:
    print("Could not open the file, pipline not finished maybe")
    exit()



output_file = "clean_machine_measurements_report.md"

with open(output_file, "w") as f:
    with contextlib.redirect_stdout(f):
        
        print("# Clean Machine Measurements Profiling Report\n")

        print("## Overview")
        print("- **Rows:**", len(df))
        print("- **Columns:**", len(df.columns))
        print("\n**Data types:**\n", df.dtypes)

        print("\n## subject_id")
        print("- **nulls:**", df["subject_id"].isna().sum())
        print("- **format valid:**", df["subject_id"].dropna().astype(str).str.match(r"^\d+$").all())


        print("\n## study_id")
        print("- **nulls:**", df["study_id"].isna().sum())
        print("- **format valid:**", df["study_id"].dropna().astype(str).str.match(r"^\d+$").all())
        print("- **unique:**", df["study_id"].is_unique)
        print("- **duplicates:**", df["study_id"].duplicated().sum())

        print("\n## cart_id")
        print("- **nulls:**", df["cart_id"].isna().sum())
        print("- **format valid:**", df["cart_id"].dropna().astype(str).str.match(r"^\d+$").all())
        print("- **unique machines:**", df["cart_id"].nunique())

        print("\n## ecg_time")
        df["ecg_time_parsed"] = pd.to_datetime(df["ecg_time"], unit='s', errors="coerce")

        print("- **nulls after parsing:**", df["ecg_time_parsed"].isna().sum())
        print("- **format valid:**", df["ecg_time_parsed"].notna().all())
        print("- **cart_id + ecg_time duplicates:**", df.duplicated(subset=["cart_id", "ecg_time"]).sum())
              
        print("\n## [reports]")
        report_cols = [f"report_{i}" for i in range(18) if f"report_{i}" in df.columns]
        print("**Null coverage per report column:**")
        print(df[report_cols].isna().sum())

        print("\n## [bandwidth]")
        print("- **nulls:**", df["bandwidth"].isna().sum())
        print("- **format valid:**", df["bandwidth"].dropna().astype(str).str.match(r"^\d*\.?\d+-\d*\.?\d+\s*Hz$").all())
        print("- **unique values:**", df["bandwidth"].nunique())

        print("\n## [filtering]")
        print("- **nulls:**", df["filtering"].isna().sum())
        df["filtering_clean"] = df["filtering"].astype(str).str.strip().str.lower()
        print("- **unique values:**", df["filtering_clean"].nunique())

        print("\n## [measurements checks]")
        measure_cols = [
            "rr_interval", "p_onset", "p_end",
            "qrs_onset", "qrs_end", "t_end",
            "p_axis", "qrs_axis", "t_axis",
        ]
        non_negative_cols = ["rr_interval", "p_onset", "p_end", "qrs_onset", "qrs_end", "t_end"]

        for col in measure_cols:
            if col not in df.columns: continue 
            
            print(f"\n### {col}")
            print("- **nulls:**", df[col].isna().sum())
            is_numeric = pd.api.types.is_numeric_dtype(df[col])
            print("- **is numeric:**", is_numeric)
            
            if not is_numeric:
                converted = pd.to_numeric(df[col], errors="coerce")
                print("- **convertible to numeric:**", converted.notna().sum() == df[col].notna().sum())
            else:
                converted = df[col]

            if pd.api.types.is_numeric_dtype(converted):
                print("- **min:**", converted.min())
                print("- **max:**", converted.max())

            if col in non_negative_cols:
                print("- **negative values:**", (converted < 0).sum())

        print("\n## logical order checks")
        cond1 = df["p_onset"] <= df["p_end"]
        cond2 = df["p_end"] <= df["qrs_onset"]
        cond3 = df["qrs_onset"] <= df["qrs_end"]
        cond4 = df["qrs_end"] <= df["t_end"]

        correct_order = cond1 & cond2 & cond3 & cond4
        order_violation = correct_order == False

        print("- **sequence violations:**", order_violation.sum())

        print("\n## derived checks")

        p_duration_check = df["p_end"] - df["p_onset"]
        qrs_duration_check = df["qrs_end"] - df["qrs_onset"]
        qt_proxy_check = df["t_end"] - df["qrs_onset"]

        print("- **negative p_duration:**", (p_duration_check < 0).sum())
        print("- **negative qrs_duration:**", (qrs_duration_check < 0).sum())
        print("- **negative qt_proxy:**", (qt_proxy_check < 0).sum())

        print("- **p_duration > rr_interval:**", (p_duration_check > df["rr_interval"]).sum())
        print("- **qrs_duration > rr_interval:**", (qrs_duration_check > df["rr_interval"]).sum())
        print("- **qt_proxy > rr_interval:**", (qt_proxy_check > df["rr_interval"]).sum())

print(f"Done, the '{output_file}' file in your folder.")