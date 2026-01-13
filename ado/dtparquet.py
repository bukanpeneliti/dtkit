import sfi
import pyarrow as pa
import pyarrow.parquet as pq
import json
import os
import tempfile
from typing import Optional, Any

DTMETA_KEY = "dtparquet.dtmeta"


class StreamManager:
    _writer: Any = None
    _temp_path: Optional[str] = None
    _target_path: Optional[str] = None
    _schema: Optional[pa.Schema] = None
    _nolabel: bool = False

    @classmethod
    def init_export(cls, filename: str, nolabel: bool = False):
        cls._target_path = filename
        cls._nolabel = nolabel
        target_dir = os.path.dirname(filename) or "."
        if target_dir and not os.path.exists(target_dir):
            os.makedirs(target_dir, exist_ok=True)

        fd, cls._temp_path = tempfile.mkstemp(suffix=".parquet.tmp", dir=target_dir)
        os.close(fd)
        cls._writer = None
        cls._schema = None

    @classmethod
    def write_chunk(cls):
        var_count = sfi.Data.getVarCount()
        var_names = [sfi.Data.getVarName(i) for i in range(var_count)]
        stata_types = [sfi.Data.getVarType(i) for i in range(var_count)]

        if cls._schema is None:
            schema = build_arrow_schema(var_names, stata_types, cls._nolabel)

            # Extract and add dtmeta to schema metadata on first chunk
            custom_meta = {}
            if not cls._nolabel:
                dtmeta_json = extract_dtmeta()
                custom_meta[DTMETA_KEY] = dtmeta_json

            if custom_meta:
                meta = schema.metadata or {}
                merged_meta = {
                    **{
                        k.decode() if isinstance(k, bytes) else k: v
                        for k, v in meta.items()
                    },
                    **{k: v for k, v in custom_meta.items()},
                }
                schema = schema.with_metadata(merged_meta)

            cls._schema = schema
            if cls._temp_path:
                cls._writer = pq.ParquetWriter(
                    cls._temp_path, schema, compression="NONE"
                )

        data_arrays = []
        missing_val = sfi.Missing.getValue()
        if cls._schema is not None:
            for i in range(var_count):
                arrow_type = cls._schema.field(i).type
                raw_data = sfi.Data.get(i)
                # Map Stata missing values to None for Arrow
                sanitized_data = [None if v == missing_val else v for v in raw_data]
                data_arrays.append(pa.array(sanitized_data, type=arrow_type))

            writer = cls._writer
            if writer is not None:
                table = pa.Table.from_arrays(data_arrays, schema=cls._schema)
                writer.write_table(table)

    @classmethod
    def finalize_export(cls):
        if cls._writer:
            cls._writer.close()
            cls._writer = None

        if cls._temp_path and cls._target_path and os.path.exists(cls._temp_path):
            os.replace(cls._temp_path, cls._target_path)
            cls._temp_path = None

    @classmethod
    def abort_export(cls):
        if cls._writer:
            cls._writer.close()
            cls._writer = None
        if cls._temp_path and os.path.exists(cls._temp_path):
            try:
                os.unlink(cls._temp_path)
            except:
                pass
            cls._temp_path = None


def stata_to_arrow_type(stata_type):
    """Maps Stata storage types to Arrow types."""
    if stata_type == "byte":
        return pa.int8()
    elif stata_type == "int":
        return pa.int16()
    elif stata_type == "long":
        return pa.int32()
    elif stata_type == "float":
        return pa.float32()
    elif stata_type == "double":
        return pa.float64()
    elif stata_type == "strL":
        return pa.string()
    elif stata_type.startswith("str"):
        return pa.string()
    else:
        return pa.string()


def is_foreign_file(schema):
    """Check if Parquet file was created by dtparquet (has Stata metadata)."""
    if schema.metadata:
        if DTMETA_KEY.encode() in schema.metadata:
            return False
        for field in schema:
            if field.metadata and b"stata.type" in field.metadata:
                return False
    return True


def convert_epoch_date(date_val):
    """Convert Unix epoch date (days since 1970-01-01) to Stata epoch (days since 1960-01-01)."""
    if date_val is None:
        return None
    import datetime

    if isinstance(date_val, datetime.date):
        delta = date_val - datetime.date(1970, 1, 1)
        return delta.days + 3653
    return date_val + 3653


def convert_epoch_timestamp(ts_val):
    """Convert Unix epoch timestamp (milliseconds since 1970-01-01) to Stata epoch (milliseconds since 1960-01-01)."""
    if ts_val is None:
        return None
    import datetime

    if isinstance(ts_val, datetime.datetime):
        # Stata tc: milliseconds since 01jan1960 00:00:00
        # Unix epoch starts 315619200000 ms after Stata epoch
        unix_ts_ms = ts_val.timestamp() * 1000
        return unix_ts_ms + 315619200000
    return ts_val + 315619200000


def arrow_to_stata_type(arrow_type, int64_as_string=False):
    """Maps Arrow types back to Stata storage types."""
    if pa.types.is_int8(arrow_type):
        return "byte"
    elif pa.types.is_int16(arrow_type):
        return "int"
    elif pa.types.is_int32(arrow_type):
        return "long"
    elif pa.types.is_int64(arrow_type):
        if int64_as_string:
            return "strL"
        return "double"
    elif pa.types.is_uint64(arrow_type):
        if int64_as_string:
            return "strL"
        return "double"
    elif pa.types.is_float32(arrow_type):
        return "float"
    elif pa.types.is_float64(arrow_type):
        return "double"
    elif pa.types.is_string(arrow_type):
        return "strL"
    elif pa.types.is_binary(arrow_type):
        return "strL"
    elif pa.types.is_dictionary(arrow_type):
        index_type = arrow_type.index_type
        if pa.types.is_int8(index_type):
            return "byte"
        elif pa.types.is_int16(index_type):
            return "int"
        elif pa.types.is_int32(index_type):
            return "long"
        else:
            return "long"
    elif pa.types.is_date(arrow_type):
        return "long"
    elif pa.types.is_timestamp(arrow_type):
        return "double"
    else:
        return "strL"


def add_stata_var(vtype, name):
    """Helper to add variable of correct type."""
    if vtype == "byte":
        sfi.Data.addVarByte(name)
    elif vtype == "int":
        sfi.Data.addVarInt(name)
    elif vtype == "long":
        sfi.Data.addVarLong(name)
    elif vtype == "float":
        sfi.Data.addVarFloat(name)
    elif vtype == "double":
        sfi.Data.addVarDouble(name)
    elif vtype == "strL":
        sfi.Data.addVarStrL(name)
    elif vtype.startswith("str"):
        try:
            width = int(vtype[3:])
        except:
            width = 1
        sfi.Data.addVarStr(name, width)
    else:
        sfi.Data.addVarStrL(name)


def extract_dtmeta():
    """Serializes _dt* frames into a JSON string."""
    metadata = {"schema_version": 1, "min_reader_version": 1, "frames": {}}
    target_frames = ["_dtvars", "_dtlabel", "_dtnotes", "_dtinfo"]

    # Use c(frame) macro for reliable frame tracking in batch mode
    orig_frame = sfi.Macro.getGlobal("c(frame)") or "default"

    for fr_name in target_frames:
        if fr_name in sfi.Frame.getFrames():
            sfi.SFIToolkit.stata(f"cwf {fr_name}")

            var_count = sfi.Data.getVarCount()
            obs_count = sfi.Data.getObsTotal()

            if obs_count == 0:
                continue

            frame_data = {
                "colnames": [sfi.Data.getVarName(i) for i in range(var_count)],
                "types": [sfi.Data.getVarType(i) for i in range(var_count)],
                "data": [sfi.Data.get(i) for i in range(var_count)],
            }
            metadata["frames"][fr_name] = frame_data

    sfi.SFIToolkit.stata(f"cwf {orig_frame}")
    return json.dumps(metadata)


def apply_dtmeta(metadata_json):
    """Restores _dt* frames from JSON string. Returns type mapping from _dtvars."""
    if not metadata_json:
        return {}

    try:
        metadata = json.loads(metadata_json)
    except:
        return {}

    min_version = metadata.get("min_reader_version", 0)
    if min_version > 1:
        raise RuntimeError(
            f"Parquet file requires dtparquet reader version {min_version} or higher (current version is 1)."
        )

    orig_frame = sfi.Macro.getGlobal("c(frame)") or "default"

    type_mapping = {}

    for fr_name, frame_content in metadata.get("frames", {}).items():
        if fr_name in sfi.Frame.getFrames():
            sfi.SFIToolkit.stata(f"capture frame drop {fr_name}")

        sfi.Frame.create(fr_name)
        sfi.SFIToolkit.stata(f"cwf {fr_name}")

        colnames = frame_content["colnames"]
        types = frame_content["types"]
        data = frame_content["data"]

        obs_count = len(data[0]) if data else 0
        sfi.Data.addObs(obs_count)

        for i, (name, vtype) in enumerate(zip(colnames, types)):
            add_stata_var(vtype, name)
            sfi.Data.store(i, None, data[i])

        if fr_name == "_dtvars":
            type_mapping = dict(zip(colnames, types))

    sfi.SFIToolkit.stata(f"cwf {orig_frame}")

    return type_mapping


def build_arrow_schema(var_names, stata_types, nolabel=False):
    """Constructs Parquet schema from Stata variables with dual-layer metadata."""
    fields = []
    for i, (name, t) in enumerate(zip(var_names, stata_types)):
        arrow_type = stata_to_arrow_type(t)
        field_meta = {}
        if not nolabel:
            vlab = sfi.Data.getVarLabel(i)
            if vlab:
                field_meta[b"stata.label"] = vlab.encode("utf-8")
        if t.startswith("str"):
            field_meta[b"stata.type"] = t.encode("utf-8")
        fields.append(pa.field(name, arrow_type, metadata=field_meta))
    return pa.schema(fields)


def save(filename, nolabel=False, chunksize=50000):
    """Saves current Stata memory to Parquet using row-major chunking."""
    var_count = sfi.Data.getVarCount()
    obs_total = sfi.Data.getObsTotal()
    var_names = [sfi.Data.getVarName(i) for i in range(var_count)]
    stata_types = [sfi.Data.getVarType(i) for i in range(var_count)]

    schema = build_arrow_schema(var_names, stata_types, nolabel)

    # Extract and add dtmeta to schema metadata
    custom_meta = {}
    if not nolabel:
        dtmeta_json = extract_dtmeta()
        custom_meta[DTMETA_KEY] = dtmeta_json

    if custom_meta:
        meta = schema.metadata or {}
        merged_meta = {
            **{k.decode() if isinstance(k, bytes) else k: v for k, v in meta.items()},
            **{k: v for k, v in custom_meta.items()},
        }
        schema = schema.with_metadata(merged_meta)

    missing_val = sfi.Missing.getValue()
    var_indices = list(range(var_count))

    with pq.ParquetWriter(filename, schema, compression="NONE") as writer:
        for start in range(0, obs_total, chunksize):
            end = min(start + chunksize, obs_total)
            # Fetch block of rows: list of lists
            raw_data = sfi.Data.get(var_indices, range(start, end))

            # Transpose list of lists to list of columns
            columns = list(zip(*raw_data))

            data_arrays = []
            for i, col_data in enumerate(columns):
                arrow_type = schema.field(i).type
                # Map Stata missing to None for Arrow
                sanitized = [None if v == missing_val else v for v in col_data]
                data_arrays.append(pa.array(sanitized, type=arrow_type))

            table_chunk = pa.Table.from_arrays(data_arrays, schema=schema)
            writer.write_table(table_chunk)


def load(filename, varlist=None, nolabel=False, chunksize=None, int64_as_string=False):
    """Loads Parquet file into Stata. Supports streaming via chunksize."""
    parquet_file = pq.ParquetFile(filename)

    # Check if foreign file (no Stata metadata)
    is_foreign = is_foreign_file(parquet_file.schema_arrow)

    # Metadata handling
    dtmeta_types = {}
    if not nolabel and parquet_file.schema_arrow.metadata:
        dtmeta_json = parquet_file.schema_arrow.metadata.get(DTMETA_KEY.encode())
        if dtmeta_json:
            dtmeta_types = apply_dtmeta(dtmeta_json.decode())

    # Get schema from the first row group if we are streaming, or from the whole file
    schema = parquet_file.schema_arrow
    if varlist:
        indices = [schema.get_field_index(v) for v in varlist]
        schema = pa.schema([schema.field(i) for i in indices])

    # Clear memory
    vcount = sfi.Data.getVarCount()
    if vcount > 0:
        sfi.Data.dropVar(list(range(vcount)))

    total_obs = parquet_file.metadata.num_rows
    sfi.Data.addObs(total_obs)

    # Add variables and track dictionary labels
    dict_labels = {}
    for i, field in enumerate(schema):
        varname = field.name

        # Handle dictionary types
        if pa.types.is_dictionary(field.type):
            if varname in dtmeta_types:
                stata_type = dtmeta_types[varname]
            else:
                stata_type = arrow_to_stata_type(field.type, int64_as_string)

            add_stata_var(stata_type, varname)

            # Extract dictionary categories for value labels
            if not nolabel:
                # categories are not easily accessible from field.type in some versions
                # we'll extract them from the first row group/batch later
                pass

                # Set label if available
                if field.metadata:
                    vlab = field.metadata.get(b"stata.label")
                    if vlab:
                        sfi.Data.setVarLabel(i, vlab.decode("utf-8"))
                else:
                    sfi.Data.setVarLabel(i, "")
        else:
            # Non-dictionary types
            if varname in dtmeta_types:
                stata_type = dtmeta_types[varname]
            elif field.metadata:
                stored_type = field.metadata.get(b"stata.type")
                if stored_type:
                    stata_type = stored_type.decode("utf-8")
                else:
                    stata_type = arrow_to_stata_type(field.type, int64_as_string)
            else:
                stata_type = arrow_to_stata_type(field.type, int64_as_string)

            add_stata_var(stata_type, varname)
            if nolabel:
                sfi.Data.setVarLabel(i, "")
            elif field.metadata:
                vlab = field.metadata.get(b"stata.label")
                if vlab:
                    sfi.Data.setVarLabel(i, vlab.decode("utf-8"))

    # Load data in batches
    current_offset = 0
    # Use 50k as default chunksize for streaming if not specified
    actual_chunksize = chunksize or 50000
    missing_val = sfi.Missing.getValue()

    applied_vls = set()

    for batch in parquet_file.iter_batches(
        batch_size=actual_chunksize, columns=varlist
    ):
        table_chunk = pa.Table.from_batches([batch])
        for i in range(table_chunk.num_columns):
            field = table_chunk.schema.field(i)
            varname = field.name
            column_data = table_chunk.column(i).to_pylist()

            # Handle dictionary decoding and value label creation
            if pa.types.is_dictionary(field.type):
                # Extract indices and dictionary (categories)
                chunk_col = table_chunk.column(i)
                # For chunked columns, we might need to combine or just take the first chunk's dictionary
                if hasattr(chunk_col, "chunks"):
                    first_chunk = chunk_col.chunk(0)
                    categories = first_chunk.dictionary.to_pylist()
                    indices = first_chunk.indices.to_pylist()
                else:
                    categories = chunk_col.dictionary.to_pylist()
                    indices = chunk_col.to_pylist()  # these are the indices

                if not nolabel and varname not in applied_vls:
                    label_name = f"_vl_{varname}"
                    sfi.SFIToolkit.stata(f"capture label drop {label_name}")
                    for cat_idx, cat_value in enumerate(categories):
                        if cat_value is not None:
                            # Escape double quotes for Stata
                            safe_val = str(cat_value).replace('"', '""')
                            sfi.SFIToolkit.stata(
                                f'label define {label_name} {cat_idx} `"{safe_val}"\', modify'
                            )
                    sfi.SFIToolkit.stata(f"label values {varname} {label_name}")
                    applied_vls.add(varname)

                column_data = indices

            # Handle epoch conversion for foreign files
            if is_foreign:
                if pa.types.is_date(field.type):
                    # Cast to int32 to get raw days
                    raw_values = table_chunk.column(i).cast(pa.int32()).to_pylist()
                    column_data = [
                        v + 3653 if v is not None else None for v in raw_values
                    ]
                elif pa.types.is_timestamp(field.type):
                    # Cast to int64 to get raw units
                    unit = field.type.unit
                    raw_values = table_chunk.column(i).cast(pa.int64()).to_pylist()
                    if unit == "s":
                        column_data = [
                            v * 1000 + 315619200000 if v is not None else None
                            for v in raw_values
                        ]
                    elif unit == "ms":
                        column_data = [
                            v + 315619200000 if v is not None else None
                            for v in raw_values
                        ]
                    elif unit == "us":
                        column_data = [
                            v // 1000 + 315619200000 if v is not None else None
                            for v in raw_values
                        ]
                    elif unit == "ns":
                        column_data = [
                            v // 1000000 + 315619200000 if v is not None else None
                            for v in raw_values
                        ]

            # Handle Int64 as String conversion
            if int64_as_string and (
                pa.types.is_int64(field.type) or pa.types.is_uint64(field.type)
            ):
                column_data = [str(v) if v is not None else None for v in column_data]

            # Handle Binary to String conversion
            # Convert bytes to latin-1 strings to prevent SFI from iterating over bytes
            if pa.types.is_binary(field.type):
                column_data = [
                    v.decode("latin-1") if isinstance(v, bytes) else v
                    for v in column_data
                ]

            # Map None back to Stata missing for numeric variables
            # Check the actual Stata type assigned
            stata_type = sfi.Data.getVarType(i)
            if stata_type not in ["strL"] and not stata_type.startswith("str"):
                column_data = [missing_val if v is None else v for v in column_data]

            sfi.Data.store(
                i,
                range(current_offset, current_offset + table_chunk.num_rows),
                column_data,
            )
        current_offset += table_chunk.num_rows


def cleanup_orphaned_tmp_files():
    """Clean up orphaned .tmp files from previous failed operations."""
    import glob
    import os

    for tmp_file in glob.glob("**/*.parquet.tmp", recursive=True):
        try:
            os.unlink(tmp_file)
        except:
            pass

    temp_dir = os.environ.get("TEMP", os.environ.get("TMP", "."))
    for tmp_file in glob.glob(os.path.join(temp_dir, "*.parquet.tmp")):
        try:
            os.unlink(tmp_file)
        except:
            pass


def save_atomic(filename, nolabel=False, chunksize=50000):
    """Atomic version of save: writes to .tmp file then renames."""
    import tempfile
    import os

    target_dir = os.path.dirname(filename) or "."
    if target_dir and not os.path.exists(target_dir):
        os.makedirs(target_dir, exist_ok=True)

    fd, temp_path = tempfile.mkstemp(suffix=".parquet.tmp", dir=target_dir)
    os.close(fd)

    try:
        save(temp_path, nolabel, chunksize)
        os.replace(temp_path, filename)
    except Exception as e:
        if os.path.exists(temp_path):
            try:
                os.unlink(temp_path)
            except:
                pass
        raise e


def load_atomic(filename, nolabel=False, chunksize=None, int64_as_string=False):
    return load(
        filename,
        varlist=None,
        nolabel=nolabel,
        chunksize=chunksize,
        int64_as_string=int64_as_string,
    )
