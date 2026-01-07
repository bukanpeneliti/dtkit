import sys
import json
from pathlib import Path
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def find_stata_path() -> str | None:
    base_dir = Path("C:/Program Files")
    if not base_dir.exists():
        return None
    stata_dirs = [d for d in base_dir.iterdir() if d.is_dir() and "Stata" in d.name]
    if not stata_dirs:
        return None
    latest_dir = sorted(stata_dirs, key=lambda d: d.name, reverse=True)[0]
    return str(latest_dir)


def stata_to_parquet(dta_path: str, parquet_path: str):
    import stata_setup

    with pd.io.stata.StataReader(dta_path) as reader:
        variable_labels = reader.variable_labels()
        value_labels_def = reader.value_labels()
        lbllist = []
        try:
            for attr in ["_lbllist", "lbllist"]:
                if hasattr(reader, attr):
                    lbllist = getattr(reader, attr)
                    break
            if not lbllist and hasattr(reader, "_get_lbllist"):
                lbllist = reader._get_lbllist()
        except:
            pass
        df = reader.read(convert_categoricals=False)

    stata_notes, stata_formats, stata_types, dataset_label = {}, {}, {}, ""
    try:
        stata_dir = find_stata_path()
        if stata_dir:
            stata_setup.config(stata_dir, "mp", splash=False)
            from pystata import stata

            stata.run(f'use "{dta_path}", clear', echo=False)
            ado_dir = str(Path(__file__).parent)
            stata.run(f'adopath + "{ado_dir}"', echo=False)
            stata.run("cap dtmeta", echo=False)

            try:
                df_vars = stata.pdataframe_from_frame("_dtvars")
                if df_vars is not None and not df_vars.empty:
                    for _, row in df_vars.iterrows():
                        vname = str(row["varname"]).strip()
                        stata_formats[vname] = str(row["format"]).strip()
                        stata_types[vname] = str(row["type"]).strip()
            except:
                pass

            try:
                df_vnotes = stata.pdataframe_from_frame("_dtnotes")
                if df_vnotes is not None and not df_vnotes.empty:
                    for varname, group in df_vnotes.groupby("varname"):
                        stata_notes[varname] = group.sort_values("_note_id")[
                            "_note_text"
                        ].tolist()
            except:
                pass

            try:
                df_dnotes = stata.pdataframe_from_frame("_dtinfo")
                if df_dnotes is not None and not df_dnotes.empty:
                    if "dta_label" in df_dnotes.columns:
                        lbls = df_dnotes["dta_label"].dropna().unique()
                        if len(lbls) > 0:
                            dataset_label = str(lbls[0])
                    mask = df_dnotes["dta_note"].notna() & (df_dnotes["dta_note"] != "")
                    dnotes = df_dnotes[mask]
                    if not dnotes.empty:
                        stata_notes["_dta"] = dnotes.sort_values("dta_note_id")[
                            "dta_note"
                        ].tolist()
            except:
                pass
    except Exception as e:
        print(f"Warning: Could not extract Stata metadata: {e}")

    col_value_labels = {}
    if not lbllist or len(lbllist) != len(df.columns):
        lbllist = [""] * len(df.columns)

    for i, col in enumerate(df.columns):
        label_set_name = lbllist[i] or col
        if label_set_name in value_labels_def:
            raw_labels = value_labels_def[label_set_name]
            col_value_labels[col] = {
                (int(k) if hasattr(k, "item") else k): v for k, v in raw_labels.items()
            }

    stata_metadata = {
        "variable_labels": variable_labels,
        "value_labels": col_value_labels,
        "notes": stata_notes,
        "formats": stata_formats,
        "stata_types": stata_types,
        "dataset_label": dataset_label,
        "column_dtypes": {col: str(df[col].dtype) for col in df.columns},
    }

    table = pa.Table.from_pandas(df)
    existing_meta = table.schema.metadata or {}
    new_meta = {
        **existing_meta,
        b"bpom_stata_metadata": json.dumps(stata_metadata).encode("utf-8"),
    }
    table = table.replace_schema_metadata(new_meta)
    pq.write_table(table, parquet_path)
    print(f"Successfully converted {dta_path} to {parquet_path}")


def parquet_to_stata(parquet_path: str, dta_path: str):
    import stata_setup

    table = pq.read_table(parquet_path)
    df = table.to_pandas()
    meta_bytes = table.schema.metadata.get(b"bpom_stata_metadata")
    variable_labels, value_labels, stata_notes, stata_formats, dataset_label = (
        {},
        {},
        {},
        {},
        "",
    )

    if meta_bytes:
        meta = json.loads(meta_bytes.decode("utf-8"))
        variable_labels = meta.get("variable_labels", {})
        value_labels = meta.get("value_labels", {})
        stata_notes = meta.get("notes", {})
        stata_formats = meta.get("formats", {})
        dataset_label = meta.get("dataset_label", "")

        for col, labels in value_labels.items():
            if col in df.columns:
                col_dtype = df[col].dtype
                casted = {}
                for k, v in labels.items():
                    try:
                        if pd.api.types.is_integer_dtype(col_dtype):
                            key = int(k)
                        elif pd.api.types.is_float_dtype(col_dtype):
                            key = float(k)
                        else:
                            key = k
                    except:
                        key = k
                    casted[key] = v
                value_labels[col] = casted

    df.to_stata(
        dta_path,
        write_index=False,
        variable_labels=variable_labels,
        value_labels=value_labels,
        version=118,
    )

    if stata_notes or stata_formats or dataset_label:
        try:
            stata_dir = find_stata_path()
            if stata_dir:
                stata_setup.config(stata_dir, "mp", splash=False)
                from pystata import stata

                stata.run(f'use "{dta_path}", clear', echo=False)
                if dataset_label:
                    safe_label = dataset_label.replace('"', "'")
                    stata.run(f'label data "{safe_label}"', echo=False)
                for var, fmt in stata_formats.items():
                    if var in df.columns:
                        stata.run(f"format {var} {fmt}", echo=False)
                for key, notes in stata_notes.items():
                    target = "" if key == "_dta" else key
                    for note in notes:
                        safe_note = note.replace('"', "'")
                        stata.run(f"notes {target} : {safe_note}", echo=False)
                stata.run(f'save "{dta_path}", replace', echo=False)
        except Exception as e:
            print(f"Warning: Could not restore Stata metadata: {e}")
    print(f"Successfully converted {parquet_path} to {dta_path}")


if __name__ == "__main__":
    if len(sys.argv) < 4:
        sys.exit(1)
    cmd_type, input_file, output_file = sys.argv[1], sys.argv[2], sys.argv[3]
    if cmd_type == "save":
        stata_to_parquet(input_file, output_file)
    elif cmd_type == "use":
        parquet_to_stata(input_file, output_file)
