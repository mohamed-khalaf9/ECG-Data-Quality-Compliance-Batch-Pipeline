import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import csv
from io import StringIO

options = PipelineOptions([
    "--runner=DirectRunner",
    "--direct_num_workers=1" 
])

def parse_enriched_data(line):
    import csv
    from io import StringIO
    try:
        row = next(csv.reader(StringIO(line)))
        return {
            "study_id": int(row[0]),
            "cart_id": int(row[1]),
            "rr_interval": int(row[2]),
            "p_onset": int(row[3]),
            "p_end": int(row[4]),
            "qrs_onset": int(row[5]),
            "qrs_end": int(row[6]),
            "t_end": int(row[7]),
            "p_axis": int(row[8]),
            "qrs_axis": int(row[9]),
            "t_axis": int(row[10]),
            "scan_abnormal": row[15].strip().lower() == "true",
            "has_note": row[16].strip().lower() == "true",
            "note_id": row[17] if row[17] else None,
            "waveform_path": row[18] if row[18] else None,
        }
    except Exception:
        return None

def aggregate_cart(kv):
    cart_id, values = kv
    
    total = 0
    abnormal = 0
    abnormal_no_note = 0
    abnormal_with_note = 0

    for v in values:
        total += 1
        is_abnormal = v["scan_abnormal"]
        has_note = v["has_note"]

        if is_abnormal:
            abnormal += 1
            if not has_note:
                abnormal_no_note += 1
            else:
                abnormal_with_note += 1
                
                
    compliance_ratio = (abnormal_with_note / abnormal) * 100.0 if abnormal > 0 else 100.0
    compliance = f"{compliance_ratio:.1f}%"

    return {
        "cart_id": cart_id,
        "total_scans": total,
        "abnormal_scans": abnormal,
        "abnormal_no_note_scans": abnormal_no_note,
        "compliance_ratio": compliance
    }


with beam.Pipeline(options=options) as p:

    enriched_data = (
        p
        | "read enriched data" >> beam.io.ReadFromText("/opt/airflow/dags/output/enriched_clinical_data.csv", skip_header_lines=1)
        | "parse enriched csv" >> beam.Map(parse_enriched_data)
        | "drop invalid rows" >> beam.Filter(lambda x: x is not None)
    )

    csv_columns_metrics = [
        "cart_id", "total_scans", "abnormal_scans", 
        "abnormal_no_note_scans", "compliance_ratio"
    ]

    cart_metrics = (
        enriched_data
        | "key by cart_id" >> beam.Map(lambda x: (x["cart_id"], x))
        | "group by cart" >> beam.GroupByKey()
        | "aggregate per cart" >> beam.Map(aggregate_cart)
    )

    (
        cart_metrics
        | "format metrics dict to csv string" >> beam.Map(
            lambda x: ",".join(str(x.get(col, "")) for col in csv_columns_metrics)
        )
        | "write cart metrics" >> beam.io.WriteToText(
            "/opt/airflow/dags/output/cart_metrics",
            file_name_suffix=".csv",
            shard_name_template="",
            header=",".join(csv_columns_metrics)
        )
    )
    
    csv_columns_audit = [
        "study_id", "cart_id", "rr_interval", "p_onset", "p_end", 
        "qrs_onset", "qrs_end", "t_end", "p_axis", "qrs_axis", 
        "t_axis", "note_id", "waveform_path", "scan_abnormal"
    ]

    abnormal_no_note_audit = (
        enriched_data
        | "filter abnormal without note" >> beam.Filter(
            lambda x: x["scan_abnormal"] is True and x["has_note"] is False
        )
        | "format audit output" >> beam.Map(lambda x: {
            "study_id": x["study_id"],
            "cart_id": x["cart_id"],
            "rr_interval": x["rr_interval"],
            "p_onset": x["p_onset"],
            "p_end": x["p_end"],
            "qrs_onset": x["qrs_onset"],
            "qrs_end": x["qrs_end"],
            "t_end": x["t_end"],
            "p_axis": x["p_axis"],
            "qrs_axis": x["qrs_axis"],
            "t_axis": x["t_axis"],
            "note_id": x["note_id"],
            "waveform_path": x["waveform_path"],
            "scan_abnormal": x["scan_abnormal"]
        })
    )

    (
        abnormal_no_note_audit
        | "format audit dict to csv string" >> beam.Map(
            lambda x: ",".join('"{}"'.format(str(x.get(col, "")).replace('"', '""')) if ',' in str(x.get(col, "")) else str(x.get(col, "")) for col in csv_columns_audit)
        )
        | "write abnormal audit" >> beam.io.WriteToText(
            "/opt/airflow/dags/output/abnormal_no_note_audit",
            file_name_suffix=".csv",
            shard_name_template="",
            header=",".join(csv_columns_audit)
        )
    )