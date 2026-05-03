import pandas as pd
import contextlib

# 1. Load the first 50k
print("loading raw data...")
df = pd.read_csv("machine_measurements.csv", nrows=50000)

output_file = "machine_measurements_report.md"

with open(output_file, "w") as f:
    with contextlib.redirect_stdout(f):
      
        print("# Machine Measurements Profiling Report\n")

        print("## Overview")
        print("- **Rows:**", len(df))
        print("- **Columns:**", len(df.columns))
        print("\n**Data types:**\n", df.dtypes)

        print("\n## subject_id")
        print("- **nulls:**", df["subject_id"].isna().sum())
        print("- **format valid (digits only):**", df["subject_id"].dropna().astype(str).str.match(r"^\d+$").all())
        print("- **duplicates count:**", df["subject_id"].duplicated().sum())

        print("\n## study_id")
        print("- **nulls:**", df["study_id"].isna().sum())
        print("- **format valid (digits only):**", df["study_id"].dropna().astype(str).str.match(r"^\d+$").all())
        print("- **unique check:**", df["study_id"].is_unique)
        print("- **duplicates count:**", df["study_id"].duplicated().sum())
        print("- **unique ratio:**", df["study_id"].nunique() / len(df))

        print("\n## cart_id")
        print("- **nulls:**", df["cart_id"].isna().sum())
        print("- **format valid (digits only):**", df["cart_id"].dropna().astype(str).str.match(r"^\d+$").all())
        print("- **unique machines available:**", df["cart_id"].nunique())

        print("\n## ecg_time")
        df["ecg_time_parsed"] = pd.to_datetime(df["ecg_time"], errors="coerce")
        print("- **nulls after parsing:**", df["ecg_time_parsed"].isna().sum())
        print("- **format valid:**", df["ecg_time_parsed"].notna().all())
        print("- **cart_id + ecg_time duplicate scans:**", df.duplicated(subset=["cart_id", "ecg_time"]).sum())

        print("\n## reports")
        report_cols = [f"report_{i}" for i in range(18) if f"report_{i}" in df.columns]
        print("**Null coverage per report column:**")
        print(df[report_cols].isna().sum())

        print("\n## bandwidth")
        print("- **nulls:**", df["bandwidth"].isna().sum())
        print("- **format valid:**", df["bandwidth"].dropna().astype(str).str.match(r"^\d*\.?\d+-\d*\.?\d+\s*Hz$").all())
        print("- **unique values:**", df["bandwidth"].nunique())
        print("**Top 5 values:**\n", df["bandwidth"].value_counts().head(5))

        print("\n## filtering")
        print("- **nulls:**", df["filtering"].isna().sum())
        df["filtering_clean"] = df["filtering"].astype(str).str.strip().str.lower()
        print("- **unique values:**", df["filtering_clean"].nunique())
        print("**Top 5 values:**\n", df["filtering_clean"].value_counts().head(5))

        print("\n## measurements (overview)")
        measure_cols = [
            "rr_interval", "p_onset", "p_end",
            "qrs_onset", "qrs_end", "t_end",
            "p_axis", "qrs_axis", "t_axis"
        ]
        non_negative_cols = ["rr_interval", "p_onset", "p_end", "qrs_onset", "qrs_end", "t_end"]

        for col in measure_cols:
            print(f"\n### {col}")
            print("- **nulls:**", df[col].isna().sum())
            
            converted = pd.to_numeric(df[col], errors="coerce")
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
        print("- **sequence violations:**", (~correct_order).sum())

        print("\n## derived duration checks")
        df["p_duration"] = df["p_end"] - df["p_onset"]
        df["qrs_duration"] = df["qrs_end"] - df["qrs_onset"]
        df["qt_proxy"] = df["t_end"] - df["qrs_onset"]

        print("- **negative p_duration:**", (df["p_duration"] < 0).sum())
        print("- **negative qrs_duration:**", (df["qrs_duration"] < 0).sum())
        print("- **negative qt_proxy:**", (df["qt_proxy"] < 0).sum())

        print("- **p_duration > rr_interval:**", (df["p_duration"] > df["rr_interval"]).sum())
        print("- **qrs_duration > rr_interval:**", (df["qrs_duration"] > df["rr_interval"]).sum())
        print("- **qt_proxy > rr_interval:**", (df["qt_proxy"] > df["rr_interval"]).sum())


print(f"Done, the '{output_file}' file in your folder.")