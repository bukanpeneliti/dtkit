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


def arrow_to_stata_type(arrow_type):
    """Maps Arrow types back to Stata storage types."""
    if pa.types.is_int8(arrow_type):
        return "byte"
    elif pa.types.is_int16(arrow_type):
        return "int"
    elif pa.types.is_int32(arrow_type):
        return "long"
    elif pa.types.is_int64(arrow_type):
        return "double"
    elif pa.types.is_float32(arrow_type):
        return "float"
    elif pa.types.is_float64(arrow_type):
        return "double"
    elif pa.types.is_string(arrow_type) or pa.types.is_binary(arrow_type):
        return "strL"
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


def save(filename, nolabel=False):
    """Saves current Stata memory to Parquet."""
    var_count = sfi.Data.getVarCount()
    var_names = [sfi.Data.getVarName(i) for i in range(var_count)]
    stata_types = [sfi.Data.getVarType(i) for i in range(var_count)]

    schema = build_arrow_schema(var_names, stata_types, nolabel)
    data_arrays = []
    missing_val = sfi.Missing.getValue()
    for i in range(var_count):
        arrow_type = schema.field(i).type
        raw_data = sfi.Data.get(i)
        sanitized_data = [None if v == missing_val else v for v in raw_data]
        data_arrays.append(pa.array(sanitized_data, type=arrow_type))

    table = pa.Table.from_arrays(data_arrays, schema=schema)

    # Strictly respect nolabel for file-level metadata
    custom_meta = {}
    if not nolabel:
        dtmeta_json = extract_dtmeta()
        custom_meta[DTMETA_KEY] = dtmeta_json

    if custom_meta:
        existing_meta = table.schema.metadata or {}
        merged_meta = {
            **existing_meta,
            **{k.encode(): v.encode() for k, v in custom_meta.items()},
        }
        table = table.replace_schema_metadata(merged_meta)

    pq.write_table(table, filename, compression="NONE")


def load(filename, varlist=None, nolabel=False, chunksize=None):
    """Loads Parquet file into Stata. Supports streaming via chunksize."""
    parquet_file = pq.ParquetFile(filename)

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

    # Add variables
    for i, field in enumerate(schema):
        varname = field.name
        if varname in dtmeta_types:
            stata_type = dtmeta_types[varname]
        elif field.metadata:
            stored_type = field.metadata.get(b"stata.type")
            if stored_type:
                stata_type = stored_type.decode("utf-8")
            else:
                stata_type = arrow_to_stata_type(field.type)
        else:
            stata_type = arrow_to_stata_type(field.type)

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

    for batch in parquet_file.iter_batches(
        batch_size=actual_chunksize, columns=varlist
    ):
        table_chunk = pa.Table.from_batches([batch])
        for i in range(table_chunk.num_columns):
            field = table_chunk.schema.field(i)
            column_data = table_chunk.column(i).to_pylist()

            # Map None back to Stata missing for numeric variables
            if pa.types.is_integer(field.type) or pa.types.is_floating(field.type):
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


def save_atomic(filename, nolabel=False):
    """Atomic version of save: writes to .tmp file then renames."""
    import tempfile
    import os

    target_dir = os.path.dirname(filename) or "."
    if target_dir and not os.path.exists(target_dir):
        os.makedirs(target_dir, exist_ok=True)

    with tempfile.NamedTemporaryFile(
        mode="wb", delete=False, suffix=".tmp", dir=target_dir
    ) as tmp:
        temp_path = tmp.name

    try:
        save(temp_path, nolabel)
        os.replace(temp_path, filename)
    except Exception as e:
        if os.path.exists(temp_path):
            try:
                os.unlink(temp_path)
            except:
                pass
        raise e


def load_atomic(filename, nolabel=False, chunksize=None):
    return load(filename, varlist=None, nolabel=nolabel, chunksize=chunksize)
