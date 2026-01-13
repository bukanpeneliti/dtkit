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
    metadata = {"schema_version": 1, "frames": {}}
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


def save(filename, nolabel=False):
    """Saves current Stata memory to Parquet."""
    var_count = sfi.Data.getVarCount()
    var_names = [sfi.Data.getVarName(i) for i in range(var_count)]
    stata_types = [sfi.Data.getVarType(i) for i in range(var_count)]

    data_arrays = []
    for i in range(var_count):
        data_arrays.append(pa.array(sfi.Data.get(i)))

    fields = [
        pa.field(name, stata_to_arrow_type(t))
        for name, t in zip(var_names, stata_types)
    ]

    # Strictly respect nolabel for field-level metadata
    updated_fields = []
    for i, field in enumerate(fields):
        field_meta = field.metadata or {}
        if not nolabel:
            vlab = sfi.Data.getVarLabel(i)
            if vlab:
                field_meta[b"stata.label"] = vlab.encode("utf-8")
        if stata_types[i].startswith("str"):
            field_meta[b"stata.type"] = stata_types[i].encode("utf-8")
        updated_fields.append(field.with_metadata(field_meta))

    table = pa.Table.from_arrays(data_arrays, schema=pa.schema(updated_fields))

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

    pq.write_table(table, filename)


def load(filename, varlist=None, nolabel=False):
    table = pq.read_table(filename, columns=varlist)

    if nolabel:
        clean_fields = [pa.field(f.name, f.type) for f in table.schema]
        table = table.cast(pa.schema(clean_fields))

    vcount = sfi.Data.getVarCount()
    if vcount > 0:
        sfi.Data.dropVar(list(range(vcount)))

    cur_obs = sfi.Data.getObsTotal()
    target_obs = table.num_rows
    if cur_obs > target_obs:
        if target_obs > 0:
            sfi.Data.keepObs(list(range(target_obs)))
        else:
            sfi.SFIToolkit.stata("quietly drop _all")
    elif cur_obs < target_obs:
        sfi.Data.addObs(target_obs - cur_obs)

    dtmeta_types = {}
    if not nolabel and table.schema.metadata:
        dtmeta_json = table.schema.metadata.get(DTMETA_KEY.encode())
        if dtmeta_json:
            dtmeta_types = apply_dtmeta(dtmeta_json.decode())

    for i, field in enumerate(table.schema):
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

        sfi.Data.store(i, None, table.column(i).to_pylist())

        if not nolabel:
            if field.metadata:
                vlab = field.metadata.get(b"stata.label")
                if vlab:
                    sfi.Data.setVarLabel(i, vlab.decode("utf-8"))
        else:
            sfi.Data.setVarLabel(i, "")


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


def load_atomic(filename, nolabel=False):
    return load(filename, varlist=None, nolabel=nolabel)
