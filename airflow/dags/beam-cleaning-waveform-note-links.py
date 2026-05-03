import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import csv
from io import StringIO

options = PipelineOptions([
    "--runner=DirectRunner",
    "--direct_num_workers=1",
])



def validate_waveform_note_links_row(row):
    import pandas as pd
    import re
    
    file_path_pattern = re.compile(r"^files/p\d+/p\d+/s\d+/\d+$")
    note_pattern = re.compile(r"^\d+-EK-\d+$")

    try:
        if row["subject_id"] is None or str(row["subject_id"]).strip() == "":
            return None
        subject_str = str(row["subject_id"]).strip()
        if not subject_str.isdigit():
            return None

        row["subject_id"] = int(subject_str)


        if row["study_id"] is None or str(row["study_id"]).strip() == "":
            return None
        study_str = str(row["study_id"]).strip()
        if not study_str.isdigit():
            return None

        row["study_id"] = int(study_str)


        if row["waveform_path"] is None or str(row["waveform_path"]).strip() == "":
            return None
        path = str(row["waveform_path"]).strip()

        # format check
        if not file_path_pattern.match(path):
            return None

        # consistency check with study_id
        if str(row["study_id"]) not in path:
            return None

        row["waveform_path"] = path


        note_val = row.get("note_id")
        if note_val is not None and str(note_val).strip() != "":
            note_str = str(note_val).strip()

            if not note_pattern.match(note_str):
                return None

            row["note_id"] = note_str
        else:
            # normalize empty to None
            row["note_id"] = None


        if row.get("note_seq") is not None and str(row["note_seq"]).strip() != "":
            try:
                row["note_seq"] = int(row["note_seq"])
            except:
                # no drop, just null
                row["note_seq"] = None
        else:
            row["note_seq"] = None

        # no drop, just null
        row["charttime"] = pd.to_datetime(row.get("charttime"), errors="coerce")

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
    
    csv_columns_wnl = [
        "subject_id", "study_id", "waveform_path", "note_id", "note_seq", "charttime"
    ]

    waveform_note_links_raw = (
        p
        | "read waveform_note_links" >> beam.io.ReadFromText("/opt/airflow/dags/waveform_note_links.csv", skip_header_lines=1)
        # Using TakeFirstN to limit to 50k rows
        | "take first 200k waveform notes" >> beam.ParDo(TakeFirstN(50000))
        | "parse waveform csv safely" >> beam.Map(lambda line: next(csv.reader(StringIO(line))))
        | "convert waveform_note_links to dict" >> beam.Map(lambda x: {
            "subject_id": x[0],
            "study_id": x[1],
            "waveform_path": x[2],
            "note_id": x[3],
            "note_seq": x[4],
            "charttime": x[5],
        })
    )

    waveform_note_links_valid = (
        waveform_note_links_raw
        | "validate waveform_note_links" >> beam.Map(validate_waveform_note_links_row)
        | "drop invalid waveform_note_links" >> beam.Filter(lambda x: x is not None)
    )
    
    # dedup by study_id
    waveform_note_links_dedup = (
        waveform_note_links_valid
        | "key study_id waveform_note_links" >> beam.Map(lambda x: (x["study_id"], x))
        | "dedup study_id waveform_note_links" >> beam.CombinePerKey(lambda rows: sorted(rows, key=lambda r: str(r["note_id"]) if r.get("note_id") else "")[0])
        | "drop key waveform_note_links (study_id)" >> beam.Map(lambda kv: kv[1])
    )

    (
        waveform_note_links_dedup 
        | "format waveform dict to csv string" >> beam.Map(
            lambda x: ",".join('"{}"'.format(str(x.get(col, "")).replace('"', '""')) if ',' in str(x.get(col, "")) else str(x.get(col, "")) for col in csv_columns_wnl)
        )
        | "write waveform_note_links Clean" >> beam.io.WriteToText(
            "/opt/airflow/dags/output/waveform_note_links_clean",
            file_name_suffix=".csv",
            shard_name_template="",
            header=",".join(csv_columns_wnl)
        )
    )