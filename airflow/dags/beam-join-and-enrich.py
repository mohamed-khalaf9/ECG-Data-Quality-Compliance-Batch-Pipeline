import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import csv
from io import StringIO

options = PipelineOptions([
    "--runner=DirectRunner",
    "--direct_num_workers=1" 
])

def parse_clean_measurements(line):
    import csv
    from io import StringIO
    try:
        row = next(csv.reader(StringIO(line)))
        return {
            "study_id": int(row[1]),
            "cart_id": int(row[2]),
            "rr_interval": int(row[24]),
            "p_onset": int(row[25]),
            "p_end": int(row[26]),
            "qrs_onset": int(row[27]),
            "qrs_end": int(row[28]),
            "t_end": int(row[29]),
            "p_axis": int(row[30]),
            "qrs_axis": int(row[31]),
            "t_axis": int(row[32])
        }
    except Exception:
        return None

def parse_clean_waveforms(line):
    import csv
    from io import StringIO
    try:
        row = next(csv.reader(StringIO(line)))
        return {
            "study_id": int(row[1]),
            "waveform_path": row[2] if row[2] else None,
            "note_id": row[3] if row[3] else None
        }
    except Exception:
        return None


with beam.Pipeline(options=options) as p:

    machine_measurements_kv = (
        p
        | "read clean measurements" >> beam.io.ReadFromText("/opt/airflow/dags/output/machine_measurements_clean.csv", skip_header_lines=1)
        | "parse measurements" >> beam.Map(parse_clean_measurements)
        | "drop invalid measurements" >> beam.Filter(lambda x: x is not None)
        | "key machine measurements by study_id" >> beam.Map(lambda x: (x["study_id"], x))
    )

    waveform_note_links_kv = (
        p
        | "read clean waveforms" >> beam.io.ReadFromText("/opt/airflow/dags/output/waveform_note_links_clean.csv", skip_header_lines=1)
        | "parse waveforms" >> beam.Map(parse_clean_waveforms)
        | "drop invalid waveforms" >> beam.Filter(lambda x: x is not None)
        | "key waveform_note_links by study_id" >> beam.Map(lambda x: (x["study_id"], x))
    )

    joined = (
        {'p1': machine_measurements_kv, 'p2': waveform_note_links_kv}
        | "CoGroupByKey P1-P2" >> beam.CoGroupByKey()
        | "flatten join" >> beam.Map(lambda kv: {
            "study_id": kv[0],
            "p1": kv[1]["p1"][0] if kv[1]["p1"] else None,
            "p2": kv[1]["p2"][0] if kv[1]["p2"] else None
        })
        | "drop missing machine measurements" >> beam.Filter(lambda x: x["p1"] is not None)
    )

    enriched = (
        joined
        | "build final clinical row" >> beam.Map(lambda x: {
            "study_id": x["study_id"],
            "cart_id": x["p1"]["cart_id"],
            
            # measurements
            "rr_interval": x["p1"]["rr_interval"],
            "p_onset": x["p1"]["p_onset"],
            "p_end": x["p1"]["p_end"],
            "qrs_onset": x["p1"]["qrs_onset"],
            "qrs_end": x["p1"]["qrs_end"],
            "t_end": x["p1"]["t_end"],
            "p_axis": x["p1"]["p_axis"],
            "qrs_axis": x["p1"]["qrs_axis"],
            "t_axis": x["p1"]["t_axis"],
            
            # highlight abnormal
            "p_abnormal": ((x["p1"]["p_end"] - x["p1"]["p_onset"]) > 120),
            "qrs_abnormal": ((x["p1"]["qrs_end"] - x["p1"]["qrs_onset"]) > 120),
            "qt_abnormal": ((x["p1"]["t_end"] - x["p1"]["qrs_onset"]) > 440),
            "axis_abnormal": (
                (x["p1"]["p_axis"] < 0 or x["p1"]["p_axis"] > 75) or
                (x["p1"]["qrs_axis"] < -30 or x["p1"]["qrs_axis"] > 90) or
                (x["p1"]["t_axis"] < 15 or x["p1"]["t_axis"] > 75)
            ),
            
            # final abnormal flag
            "scan_abnormal": (
                ((x["p1"]["p_end"] - x["p1"]["p_onset"]) > 120) or
                ((x["p1"]["qrs_end"] - x["p1"]["qrs_onset"]) > 120) or
                ((x["p1"]["t_end"] - x["p1"]["qrs_onset"]) > 440) or
                (x["p1"]["p_axis"] < 0 or x["p1"]["p_axis"] > 75) or
                (x["p1"]["qrs_axis"] < -30 or x["p1"]["qrs_axis"] > 90) or
                (x["p1"]["t_axis"] < 15 or x["p1"]["t_axis"] > 75)
            ),
            
            "has_note": x["p2"] is not None,
            "note_id": x["p2"]["note_id"] if x["p2"] else None,
            "waveform_path": x["p2"]["waveform_path"] if x["p2"] else None,
        })
    )

    csv_columns_final = [
        "study_id", "cart_id", "rr_interval", "p_onset", "p_end", "qrs_onset", "qrs_end", "t_end", 
        "p_axis", "qrs_axis", "t_axis", "p_abnormal", "qrs_abnormal", "qt_abnormal", 
        "axis_abnormal", "scan_abnormal", "has_note", "note_id", "waveform_path"
    ]

    (
        enriched 
        | "format enriched dict to csv string" >> beam.Map(
            lambda x: ",".join('"{}"'.format(str(x.get(col, "")).replace('"', '""')) if ',' in str(x.get(col, "")) else str(x.get(col, "")) for col in csv_columns_final)
        )
        | "write enriched data" >> beam.io.WriteToText(
            "/opt/airflow/dags/output/enriched_clinical_data",
            file_name_suffix=".csv",
            shard_name_template="",
            header=",".join(csv_columns_final)
        )
    )