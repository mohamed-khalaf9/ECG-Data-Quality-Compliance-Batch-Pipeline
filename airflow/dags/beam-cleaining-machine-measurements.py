import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import csv
from io import StringIO

options = PipelineOptions([
    "--runner=DirectRunner",
    "--direct_num_workers=1",
])



def validate_machine_measurements_row(row):
    import pandas as pd
    import re
    
    try:
        # int conversion, handle (null and format)
        int_cols = [
            "subject_id", "study_id", "cart_id",
            "rr_interval",
            "p_onset", "p_end",
            "qrs_onset", "qrs_end",
            "t_end",
            "p_axis", "qrs_axis", "t_axis"
        ]

        for col in int_cols:
            if row[col] is None or row[col] == "":
                return None
            row[col] = int(row[col])
        
        row["ecg_time"] = pd.to_datetime(
            row["ecg_time"],
            errors="coerce"
        )

        if pd.isna(row["ecg_time"]):
            return None

        row["ecg_time"] = int(row["ecg_time"].timestamp())
        
        # logical order checks for measurements
        if not (
            row["p_onset"] <= row["p_end"] and
            row["p_end"] <= row["qrs_onset"] and
            row["qrs_onset"] <= row["qrs_end"] and
            row["qrs_end"] <= row["t_end"]
        ):
            return None
        
        # computed durations
        p_duration = row["p_end"] - row["p_onset"]
        qrs_duration = row["qrs_end"] - row["qrs_onset"]
        qt_proxy = row["t_end"] - row["qrs_onset"]
        
        # negative checks
        if (
            p_duration < 0 or
            qrs_duration < 0 or
            qt_proxy < 0
        ):
            return None
        
        # storing computed durations
        row["p_duration"] = p_duration
        row["qrs_duration"] = qrs_duration
        row["qt_proxy"] = qt_proxy

        return row

    except:
        return None



# not distributed processing friendly
class TakeFirstN(beam.DoFn):
    def __init__(self, n):
        self.n = n
        self.counter = 0

    def process(self, element):
        if self.counter < self.n:
            self.counter += 1
            yield element
            
def parse_csv_line(line):
    import csv
    from io import StringIO
    return next(csv.reader(StringIO(line)))



with beam.Pipeline(options=options) as p:
    
    machine_measurements_raw = (
        p
        | "read machine measurements" >> beam.io.ReadFromText(
            "/opt/airflow/dags/machine_measurements.csv",
            skip_header_lines=1
        )
        # Using TakeFirstN to limit to 50k rows
        | "take first 200k machine measurements" >> beam.ParDo(TakeFirstN(50000))
        | "parse machine csv safely" >> beam.Map(
        lambda line: next(csv.reader(StringIO(line))) )
        | "convert machine measurements to dict " >> beam.Map(lambda x: {
            "subject_id": x[0],
            "study_id": x[1],
            "cart_id": x[2],
            "ecg_time": str(x[3]), 

            "report_0": x[4], "report_1": x[5], "report_2": x[6], "report_3": x[7],
            "report_4": x[8], "report_5": x[9], "report_6": x[10], "report_7": x[11],
            "report_8": x[12], "report_9": x[13], "report_10": x[14], "report_11": x[15],
            "report_12": x[16], "report_13": x[17], "report_14": x[18], "report_15": x[19],
            "report_16": x[20], "report_17": x[21],

            "bandwidth": x[22],
            "filtering": x[23],
            
            "rr_interval": x[24],
            "p_onset": x[25],
            "p_end": x[26],
            "qrs_onset": x[27],
            "qrs_end": x[28],
            "t_end": x[29],
            "p_axis": x[30],
            "qrs_axis": x[31],
            "t_axis": x[32],
        })
    )

    machine_measurements_valid = (
        machine_measurements_raw
        | "validate machine measurements" >> beam.Map(validate_machine_measurements_row)
        | "drop invalid machine measurements" >> beam.Filter(lambda x: x is not None)
    )

    # dedup by study id
    machine_measurements_dedup_study_id = (
        machine_measurements_valid
        | "key study_id machine measurements" >> beam.Map(lambda x: (x["study_id"], x))
        | "dedup study_id" >> beam.CombinePerKey(lambda rows: sorted(rows, key=lambda r: r["ecg_time"])[0])
        | "drop key machine measurements (study_id)" >> beam.Map(lambda kv: kv[1])
    )

    # dedup by (study_id + ecg_time)
    machine_measurements_clean = (
        machine_measurements_dedup_study_id
        | "key cart+time machine measurements" >> beam.Map(lambda x: ((x["cart_id"], x["ecg_time"]), x))
        | "dedup cart+time" >> beam.CombinePerKey(lambda rows: sorted(rows, key=lambda r: r["study_id"])[0])
        | "drop key cart+time" >> beam.Map(lambda kv: kv[1])
    )
    
    csv_columns = [
        "subject_id", "study_id", "cart_id", "ecg_time",
        "report_0", "report_1", "report_2", "report_3", "report_4", "report_5",
        "report_6", "report_7", "report_8", "report_9", "report_10", "report_11",
        "report_12", "report_13", "report_14", "report_15", "report_16", "report_17",
        "bandwidth", "filtering",
        "rr_interval", "p_onset", "p_end", "qrs_onset", "qrs_end", "t_end",
        "p_axis", "qrs_axis", "t_axis",
        "p_duration", "qrs_duration", "qt_proxy" 
    ]
    
    (
        machine_measurements_clean 
        | "format dict to csv string" >> beam.Map(
            lambda x: ",".join('"{}"'.format(str(x.get(col, "")).replace('"', '""')) if ',' in str(x.get(col, "")) else str(x.get(col, "")) for col in csv_columns)
        )
        | "write machine measurements Clean" >> beam.io.WriteToText(
            "/opt/airflow/dags/output/machine_measurements_clean",
            file_name_suffix=".csv",
            shard_name_template="",
            header=",".join(csv_columns) 
        )
    )
