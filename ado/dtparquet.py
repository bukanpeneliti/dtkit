import sfi
import pyarrow as pa
import pyarrow.parquet as pq
import json
import os

DTMETA_KEY = "dtparquet.dtmeta"


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
    metadata = {"schema_version": 1, "frames": {}}
    target_frames = ["_dtvars", "_dtlabel", "_dtnotes", "_dtinfo"]

    # Debug current frame
    try:
        orig_frame = sfi.Frame().name
        if not orig_frame:
            orig_frame = "default"
    except:
        orig_frame = "default"

    for fr_name in target_frames:
        if fr_name in sfi.Frame.getFrames():
            # Switch frame via Stata command
            sfi.SFIToolkit.stata(f"cwf {fr_name}")

            # Extract data from current frame
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
    """Restores _dt* frames from JSON string."""
    if not metadata_json:
        return

    try:
        metadata = json.loads(metadata_json)
    except:
        return

    try:
        orig_frame = sfi.Frame().name
        if not orig_frame:
            orig_frame = "default"
    except:
        orig_frame = "default"

    for fr_name, frame_content in metadata.get("frames", {}).items():
        # Drop existing frame if any
        if fr_name in sfi.Frame.getFrames():
            sfi.Frame.drop(fr_name)

        # Create and populate frame
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

    sfi.SFIToolkit.stata(f"cwf {orig_frame}")


def save(filename, nolabel=False):
    """Saves current Stata memory to Parquet."""
    var_count = sfi.Data.getVarCount()
    var_names = [sfi.Data.getVarName(i) for i in range(var_count)]
    stata_types = [sfi.Data.getVarType(i) for i in range(var_count)]

    # Extract data (column-major for Phase 1)
    data_arrays = []
    for i in range(var_count):
        data_arrays.append(pa.array(sfi.Data.get(i)))

    # Schema construction
    fields = [
        pa.field(name, stata_to_arrow_type(t))
        for name, t in zip(var_names, stata_types)
    ]
    table = pa.Table.from_arrays(data_arrays, schema=pa.schema(fields))

    # Metadata
    custom_meta = {}
    if not nolabel:
        dtmeta_json = extract_dtmeta()
        custom_meta[DTMETA_KEY] = dtmeta_json

    if custom_meta:
        existing_meta = table.schema.metadata or {}
        # Arrow expects binary keys/values in metadata
        merged_meta = {
            **existing_meta,
            **{k.encode(): v.encode() for k, v in custom_meta.items()},
        }
        table = table.replace_schema_metadata(merged_meta)

    pq.write_table(table, filename)


def load(filename, varlist=None, nolabel=False):
    """Loads Parquet file into Stata memory."""
    table = pq.read_table(filename, columns=varlist)

    # Clear current data
    if sfi.Data.getVarCount() > 0:
        sfi.Data.dropVar("_all")
    sfi.Data.addObs(table.num_rows)

    # Restore variables
    for i, field in enumerate(table.schema):
        stata_type = arrow_to_stata_type(field.type)
        add_stata_var(stata_type, field.name)
        # Convert arrow array to list for sfi.Data.store
        sfi.Data.store(i, None, table.column(i).to_pylist())

    # Restore metadata frames
    if not nolabel and table.schema.metadata:
        dtmeta_json = table.schema.metadata.get(DTMETA_KEY.encode())
        if dtmeta_json:
            apply_dtmeta(dtmeta_json.decode())
