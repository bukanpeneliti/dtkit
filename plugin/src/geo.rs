//! GeoParquet support: `geo` file metadata, WKB decoding, and the
//! `geometry()` read modes.
//!
//! Stata cannot hold nested or binary geometry through the strL handoff, so
//! WKB geometry columns are converted while reading: to WKT or hex text, to
//! centroid and bounding-box doubles, or dropped.

use polars::prelude::*;
use serde::Deserialize;
use serde_json::Value;
use std::collections::{BTreeMap, HashSet};
use std::fmt::Write as _;
use std::fs::File;

use crate::logic::{display, read_macro, set_macro};

pub const GEO_KEY: &str = "geo";
pub const GEO_SPEC_MACRO: &str = "geometry_spec";

const MAX_WKB_DEPTH: usize = 16;

// --- File metadata ---

#[derive(Debug, Clone, Deserialize)]
pub struct GeoColumnMeta {
    pub encoding: String,
    #[serde(default)]
    pub geometry_types: Vec<String>,
    #[serde(default)]
    pub crs: Option<Value>,
    #[serde(default)]
    pub covering: Option<Value>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GeoMeta {
    pub version: String,
    pub primary_column: String,
    pub columns: BTreeMap<String, GeoColumnMeta>,
}

/// Reads and parses the `geo` key of a single Parquet file. Returns `None`
/// when the file has no valid GeoParquet metadata.
pub fn read_geo_meta(path: &str) -> Option<GeoMeta> {
    let mut r = ParquetReader::new(File::open(path).ok()?);
    let kv = r.get_metadata().ok()?.key_value_metadata().as_ref()?;
    serde_json::from_str(kv.iter().find(|e| e.key == GEO_KEY)?.value.as_deref()?).ok()
}

/// Short CRS label: the string itself, `AUTHORITY:CODE` from PROJJSON, or the
/// GeoParquet default `OGC:CRS84` when the file gives no CRS.
fn crs_label(crs: &Option<Value>) -> String {
    match crs {
        None => "OGC:CRS84".to_string(),
        Some(Value::String(s)) => s.clone(),
        Some(Value::Object(o)) => {
            let id = o.get("id").and_then(Value::as_object);
            let authority = id.and_then(|i| i.get("authority")).and_then(Value::as_str);
            let code = id.and_then(|i| i.get("code")).map(|c| match c {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            });
            match (authority, code) {
                (Some(a), Some(c)) => format!("{a}:{c}"),
                _ => "PROJJSON".to_string(),
            }
        }
        Some(_) => "unknown".to_string(),
    }
}

/// Names of the top-level bbox covering columns (GeoParquet 1.1).
fn covering_roots(col: &GeoColumnMeta) -> Vec<String> {
    let Some(Value::Object(bbox)) = col.covering.as_ref().and_then(|c| c.get("bbox")) else {
        return Vec::new();
    };
    bbox.values()
        .filter_map(|path| path.get(0).and_then(Value::as_str))
        .map(str::to_string)
        .collect()
}

// --- geometry() specification ---

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GeoStorage {
    Wkt,
    Hex,
    Drop,
}

impl GeoStorage {
    fn name(self) -> &'static str {
        match self {
            GeoStorage::Wkt => "wkt",
            GeoStorage::Hex => "hex",
            GeoStorage::Drop => "drop",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GeoSpec {
    pub storage: GeoStorage,
    pub centroid: bool,
    pub bbox: bool,
}

impl GeoSpec {
    /// Parses the `geometry()` option text. `wkt`, `hex` and `drop` select how
    /// the geometry column is stored; `centroid` and `bbox` add coordinate
    /// variables. With no storage word, `centroid` or `bbox` replace the
    /// geometry column and the default is `wkt`.
    pub fn parse(raw: &str) -> Result<Self, String> {
        let mut storage: Option<GeoStorage> = None;
        let (mut centroid, mut bbox) = (false, false);
        for token in raw
            .split(|c: char| c.is_whitespace() || c == ',')
            .filter(|t| !t.is_empty())
        {
            let word = token.to_ascii_lowercase();
            let chosen = match word.as_str() {
                "wkt" => Some(GeoStorage::Wkt),
                "hex" => Some(GeoStorage::Hex),
                "drop" => Some(GeoStorage::Drop),
                "centroid" => {
                    centroid = true;
                    None
                }
                "bbox" => {
                    bbox = true;
                    None
                }
                _ => {
                    return Err(format!(
                        "geometry() accepts wkt, hex, drop, centroid, bbox; got '{token}'"
                    ))
                }
            };
            if let Some(new) = chosen {
                if storage.replace(new).is_some_and(|old| old != new) {
                    return Err("geometry() accepts only one of wkt, hex, drop".to_string());
                }
            }
        }
        let storage = storage.unwrap_or(if centroid || bbox {
            GeoStorage::Drop
        } else {
            GeoStorage::Wkt
        });
        Ok(Self {
            storage,
            centroid,
            bbox,
        })
    }

    fn label(&self) -> String {
        let mut parts: Vec<&str> = Vec::new();
        if self.storage != GeoStorage::Drop || !(self.centroid || self.bbox) {
            parts.push(self.storage.name());
        }
        if self.centroid {
            parts.push("centroid");
        }
        if self.bbox {
            parts.push("bbox");
        }
        parts.join(" ")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Derived {
    CentroidX,
    CentroidY,
    XMin,
    YMin,
    XMax,
    YMax,
}

impl Derived {
    pub fn name(self) -> &'static str {
        match self {
            Derived::CentroidX => "_CX",
            Derived::CentroidY => "_CY",
            Derived::XMin => "_xmin",
            Derived::YMin => "_ymin",
            Derived::XMax => "_xmax",
            Derived::YMax => "_ymax",
        }
    }
}

// --- Per-file plan ---

#[derive(Debug, Clone)]
pub struct GeoPlan {
    pub meta: GeoMeta,
    pub spec: GeoSpec,
    /// Geometry columns stored as WKB.
    pub wkb_columns: Vec<String>,
    /// Columns Stata cannot hold: non-WKB geometry and bbox covering structs.
    pub skipped_columns: Vec<String>,
}

impl GeoPlan {
    /// Builds the plan for `path` from the `geometry()` text. Returns
    /// `Ok(None)` for files without GeoParquet metadata.
    pub fn from_file(path: &str, raw_spec: &str) -> Result<Option<Self>, String> {
        let spec = GeoSpec::parse(raw_spec)?;
        let Some(meta) = read_geo_meta(path) else {
            return Ok(None);
        };
        let mut wkb_columns = Vec::new();
        let mut skipped_columns = Vec::new();
        for (name, col) in &meta.columns {
            if col.encoding.eq_ignore_ascii_case("WKB") {
                wkb_columns.push(name.clone());
            } else {
                skipped_columns.push(name.clone());
            }
            skipped_columns.extend(covering_roots(col));
        }
        Ok(Some(Self {
            meta,
            spec,
            wkb_columns,
            skipped_columns,
        }))
    }

    /// The geometry column that receives centroid and bbox variables.
    pub fn primary(&self) -> Option<&str> {
        let primary = self.meta.primary_column.as_str();
        self.wkb_columns
            .iter()
            .find(|c| c.as_str() == primary)
            .or_else(|| self.wkb_columns.first())
            .map(String::as_str)
    }

    pub fn derived(&self) -> Vec<Derived> {
        if self.primary().is_none() {
            return Vec::new();
        }
        let mut out = Vec::new();
        if self.spec.centroid {
            out.extend([Derived::CentroidX, Derived::CentroidY]);
        }
        if self.spec.bbox {
            out.extend([Derived::XMin, Derived::YMin, Derived::XMax, Derived::YMax]);
        }
        out
    }

    pub fn derived_names(&self) -> Vec<&'static str> {
        self.derived().into_iter().map(Derived::name).collect()
    }

    /// Rewrites the file schema into the Stata-facing column list: removes
    /// unsupported columns and, for `drop`, the geometry columns, then appends
    /// the derived coordinate columns.
    pub fn adjust_schema(&self, schema: &Schema) -> Result<Schema, String> {
        let derived = self.derived();
        let mut fields: Vec<(PlSmallStr, DataType)> = Vec::with_capacity(schema.len() + 6);
        for (name, dt) in schema.iter() {
            let n = name.as_str();
            if derived.iter().any(|d| d.name() == n) {
                return Err(format!(
                    "column '{n}' collides with a geometry() variable; rename it or drop the geometry option"
                ));
            }
            if self.skipped_columns.iter().any(|s| s == n) {
                display(&format!(
                    "dtparquet: skipping GeoParquet column '{n}' (unsupported encoding or bbox covering)"
                ));
                continue;
            }
            if self.spec.storage == GeoStorage::Drop && self.wkb_columns.iter().any(|c| c == n) {
                continue;
            }
            fields.push((name.clone(), dt.clone()));
        }
        for d in derived {
            fields.push((PlSmallStr::from(d.name()), DataType::Float64));
        }
        Ok(Schema::from_iter(fields))
    }

    /// Converts geometry columns of a loaded frame in place and appends the
    /// derived coordinate columns. Only columns named in `keep` are converted.
    pub fn apply(&self, df: &mut DataFrame, keep: &HashSet<&str>) -> PolarsResult<()> {
        let derived = self.derived();
        let derived_wanted: Vec<Derived> = derived
            .into_iter()
            .filter(|d| keep.contains(d.name()))
            .collect();
        let primary = self.primary();
        for name in &self.wkb_columns {
            if df.get_column_index(name).is_none() {
                continue;
            }
            let want_text = self.spec.storage != GeoStorage::Drop && keep.contains(name.as_str());
            let want_derived = primary == Some(name.as_str()) && !derived_wanted.is_empty();
            if !want_text && !want_derived {
                continue;
            }
            let source = df.column(name)?.cast(&DataType::Binary)?;
            let ca = source.binary()?;
            let outputs = convert_column(name, ca, self.spec.storage, want_text, &derived_wanted)?;
            if let Some(text) = outputs.text {
                df.with_column(Series::new(name.as_str().into(), text).into_column())?;
            }
            for (d, values) in derived_wanted.iter().zip(outputs.derived) {
                df.with_column(Series::new(d.name().into(), values).into_column())?;
            }
        }
        Ok(())
    }

    /// Publishes the file's GeoParquet details as local macros for the ado.
    pub fn publish_macros(plan: Option<&GeoPlan>) {
        let Some(p) = plan else {
            set_macro("geo_present", "0", false);
            return;
        };
        let primary = p.primary().unwrap_or("");
        let column = p.meta.columns.get(primary);
        set_macro("geo_present", "1", false);
        set_macro("geo_version", &p.meta.version, false);
        set_macro("geo_primary", primary, false);
        set_macro("geo_columns", &p.wkb_columns.join(" "), false);
        set_macro("geo_mode", &p.spec.label(), false);
        set_macro(
            "geo_encoding",
            column.map(|c| c.encoding.as_str()).unwrap_or(""),
            false,
        );
        set_macro(
            "geo_crs",
            &column.map(|c| crs_label(&c.crs)).unwrap_or_default(),
            false,
        );
        set_macro(
            "geo_types",
            &column.map(|c| c.geometry_types.join(",")).unwrap_or_default(),
            false,
        );
    }
}

/// Reads the `geometry()` text from the calling Stata program.
pub fn spec_from_macro() -> String {
    read_macro(GEO_SPEC_MACRO, false, None)
}

struct ColumnOutputs {
    text: Option<Vec<Option<String>>>,
    derived: Vec<Vec<Option<f64>>>,
}

fn convert_column(
    name: &str,
    ca: &BinaryChunked,
    storage: GeoStorage,
    want_text: bool,
    derived: &[Derived],
) -> PolarsResult<ColumnOutputs> {
    let n = ca.len();
    let need_parse = !derived.is_empty() || (want_text && storage == GeoStorage::Wkt);
    let mut text: Option<Vec<Option<String>>> = want_text.then(|| Vec::with_capacity(n));
    let mut values: Vec<Vec<Option<f64>>> =
        derived.iter().map(|_| Vec::with_capacity(n)).collect();
    for (row, cell) in ca.iter().enumerate() {
        let Some(bytes) = cell else {
            if let Some(t) = text.as_mut() {
                t.push(None);
            }
            values.iter_mut().for_each(|v| v.push(None));
            continue;
        };
        let geometry = if need_parse {
            Some(parse_wkb(bytes).map_err(|e| {
                PolarsError::ComputeError(
                    format!("invalid WKB in column '{name}' at row {}: {e}", row + 1).into(),
                )
            })?)
        } else {
            None
        };
        if let Some(t) = text.as_mut() {
            t.push(Some(match (storage, geometry.as_ref()) {
                (GeoStorage::Wkt, Some(g)) => g.to_wkt(),
                _ => to_hex(bytes),
            }));
        }
        if !derived.is_empty() {
            let g = geometry.as_ref().expect("parsed when derived values are requested");
            let bbox = g.bbox();
            let centroid = g.centroid();
            for (d, out) in derived.iter().zip(values.iter_mut()) {
                out.push(match d {
                    Derived::CentroidX => centroid.map(|c| c.0),
                    Derived::CentroidY => centroid.map(|c| c.1),
                    Derived::XMin => bbox.map(|b| b[0]),
                    Derived::YMin => bbox.map(|b| b[1]),
                    Derived::XMax => bbox.map(|b| b[2]),
                    Derived::YMax => bbox.map(|b| b[3]),
                });
            }
        }
    }
    Ok(ColumnOutputs {
        text,
        derived: values,
    })
}

fn to_hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789ABCDEF";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(DIGITS[(b >> 4) as usize] as char);
        out.push(DIGITS[(b & 0x0F) as usize] as char);
    }
    out
}

// --- WKB decoding ---

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Dims {
    z: bool,
    m: bool,
}

impl Dims {
    fn stride(self) -> usize {
        2 + usize::from(self.z) + usize::from(self.m)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MultiKind {
    Point,
    LineString,
    Polygon,
    Collection,
}

#[derive(Debug)]
enum Body {
    Point(Vec<f64>),
    LineString(Vec<f64>),
    Polygon(Vec<Vec<f64>>),
    Multi(MultiKind, Vec<Geometry>),
}

/// A decoded WKB geometry. Coordinates are flat `[x, y, (z), (m)]` runs.
#[derive(Debug)]
pub struct Geometry {
    dims: Dims,
    body: Body,
}

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], String> {
        let end = self
            .pos
            .checked_add(n)
            .filter(|&e| e <= self.buf.len())
            .ok_or_else(|| "unexpected end of WKB".to_string())?;
        let bytes = &self.buf[self.pos..end];
        self.pos = end;
        Ok(bytes)
    }

    fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    fn u32(&mut self, le: bool) -> Result<u32, String> {
        let b: [u8; 4] = self.take(4)?.try_into().expect("took 4 bytes");
        Ok(if le {
            u32::from_le_bytes(b)
        } else {
            u32::from_be_bytes(b)
        })
    }

    fn f64(&mut self, le: bool) -> Result<f64, String> {
        let b: [u8; 8] = self.take(8)?.try_into().expect("took 8 bytes");
        Ok(if le {
            f64::from_le_bytes(b)
        } else {
            f64::from_be_bytes(b)
        })
    }

    /// Reads a count and checks that `unit` bytes per item can still fit, so
    /// corrupt counts cannot trigger huge allocations.
    fn count(&mut self, le: bool, unit: usize) -> Result<usize, String> {
        let n = self.u32(le)? as usize;
        if n.saturating_mul(unit) > self.remaining() {
            return Err("unexpected end of WKB".to_string());
        }
        Ok(n)
    }

    fn coords(&mut self, le: bool, dims: Dims, n: usize) -> Result<Vec<f64>, String> {
        let total = n * dims.stride();
        let mut out = Vec::with_capacity(total);
        for _ in 0..total {
            out.push(self.f64(le)?);
        }
        Ok(out)
    }

    fn geometry(&mut self, depth: usize) -> Result<Geometry, String> {
        if depth > MAX_WKB_DEPTH {
            return Err("WKB nesting too deep".to_string());
        }
        let le = match self.take(1)?[0] {
            0 => false,
            1 => true,
            b => return Err(format!("invalid byte order {b}")),
        };
        let raw = self.u32(le)?;
        // Accept ISO type codes (1000/2000/3000 offsets) and EWKB flag bits.
        let (mut z, mut m) = (raw & 0x8000_0000 != 0, raw & 0x4000_0000 != 0);
        let has_srid = raw & 0x2000_0000 != 0;
        let iso = raw & 0x0FFF_FFFF;
        match iso / 1000 {
            0 => {}
            1 => z = true,
            2 => m = true,
            3 => (z, m) = (true, true),
            _ => return Err(format!("unsupported geometry type {raw}")),
        }
        if has_srid {
            self.u32(le)?;
        }
        let dims = Dims { z, m };
        let unit = dims.stride() * 8;
        let body = match iso % 1000 {
            1 => {
                let c = self.coords(le, dims, 1)?;
                // WKB encodes POINT EMPTY as NaN coordinates.
                Body::Point(if c[0].is_nan() && c[1].is_nan() {
                    Vec::new()
                } else {
                    c
                })
            }
            2 => {
                let n = self.count(le, unit)?;
                Body::LineString(self.coords(le, dims, n)?)
            }
            3 => {
                let rings = self.count(le, 4)?;
                let mut out = Vec::with_capacity(rings);
                for _ in 0..rings {
                    let n = self.count(le, unit)?;
                    out.push(self.coords(le, dims, n)?);
                }
                Body::Polygon(out)
            }
            kind @ 4..=7 => {
                let n = self.count(le, 9)?;
                let mut members = Vec::with_capacity(n);
                for _ in 0..n {
                    members.push(self.geometry(depth + 1)?);
                }
                let kind = match kind {
                    4 => MultiKind::Point,
                    5 => MultiKind::LineString,
                    6 => MultiKind::Polygon,
                    _ => MultiKind::Collection,
                };
                Body::Multi(kind, members)
            }
            _ => return Err(format!("unsupported geometry type {raw}")),
        };
        Ok(Geometry { dims, body })
    }
}

pub fn parse_wkb(bytes: &[u8]) -> Result<Geometry, String> {
    Reader { buf: bytes, pos: 0 }.geometry(0)
}

// --- Geometry measures ---

#[derive(Default)]
struct CentroidAcc {
    ox: f64,
    oy: f64,
    area: f64,
    area_x: f64,
    area_y: f64,
    length: f64,
    length_x: f64,
    length_y: f64,
    points: f64,
    point_x: f64,
    point_y: f64,
    vertices: f64,
    vertex_x: f64,
    vertex_y: f64,
}

impl CentroidAcc {
    fn add_vertices(&mut self, run: &[f64], stride: usize) {
        for c in run.chunks_exact(stride) {
            self.vertices += 1.0;
            self.vertex_x += c[0] - self.ox;
            self.vertex_y += c[1] - self.oy;
        }
    }

    fn add_line(&mut self, run: &[f64], stride: usize) {
        self.add_vertices(run, stride);
        for pair in run.chunks_exact(stride).collect::<Vec<_>>().windows(2) {
            let (x0, y0) = (pair[0][0] - self.ox, pair[0][1] - self.oy);
            let (x1, y1) = (pair[1][0] - self.ox, pair[1][1] - self.oy);
            let seg = (x1 - x0).hypot(y1 - y0);
            self.length += seg;
            self.length_x += seg * (x0 + x1) / 2.0;
            self.length_y += seg * (y0 + y1) / 2.0;
        }
    }

    /// Adds a polygon ring with the area sign of an exterior ring (`+`) or a
    /// hole (`-`), independent of the ring's winding order.
    fn add_ring(&mut self, ring: &[f64], stride: usize, exterior: bool) {
        self.add_vertices(ring, stride);
        let pts: Vec<(f64, f64)> = ring
            .chunks_exact(stride)
            .map(|c| (c[0] - self.ox, c[1] - self.oy))
            .collect();
        let (mut twice_area, mut cx, mut cy) = (0.0, 0.0, 0.0);
        for pair in pts.windows(2) {
            let ((x0, y0), (x1, y1)) = (pair[0], pair[1]);
            let cross = x0 * y1 - x1 * y0;
            twice_area += cross;
            cx += (x0 + x1) * cross;
            cy += (y0 + y1) * cross;
        }
        if twice_area == 0.0 {
            return;
        }
        let weight = if exterior {
            twice_area.abs() / 2.0
        } else {
            -twice_area.abs() / 2.0
        };
        self.area += weight;
        self.area_x += weight * cx / (3.0 * twice_area);
        self.area_y += weight * cy / (3.0 * twice_area);
    }

    fn add(&mut self, g: &Geometry) {
        let stride = g.dims.stride();
        match &g.body {
            Body::Point(c) => {
                for p in c.chunks_exact(stride) {
                    self.points += 1.0;
                    self.point_x += p[0] - self.ox;
                    self.point_y += p[1] - self.oy;
                }
            }
            Body::LineString(c) => self.add_line(c, stride),
            Body::Polygon(rings) => {
                for (i, ring) in rings.iter().enumerate() {
                    self.add_ring(ring, stride, i == 0);
                }
            }
            Body::Multi(_, members) => members.iter().for_each(|m| self.add(m)),
        }
    }

    /// Highest dimension wins, as in OGC centroids: area, then length, then
    /// points, then the plain vertex mean for degenerate shapes.
    fn finish(&self) -> Option<(f64, f64)> {
        let (sum_x, sum_y, weight) = if self.area > 0.0 {
            (self.area_x, self.area_y, self.area)
        } else if self.length > 0.0 {
            (self.length_x, self.length_y, self.length)
        } else if self.points > 0.0 {
            (self.point_x, self.point_y, self.points)
        } else if self.vertices > 0.0 {
            (self.vertex_x, self.vertex_y, self.vertices)
        } else {
            return None;
        };
        Some((self.ox + sum_x / weight, self.oy + sum_y / weight))
    }
}

impl Geometry {
    fn for_each_run(&self, f: &mut impl FnMut(&[f64], usize)) {
        let stride = self.dims.stride();
        match &self.body {
            Body::Point(c) | Body::LineString(c) => f(c, stride),
            Body::Polygon(rings) => rings.iter().for_each(|r| f(r, stride)),
            Body::Multi(_, members) => members.iter().for_each(|m| m.for_each_run(f)),
        }
    }

    /// `[xmin, ymin, xmax, ymax]`, or `None` for an empty geometry.
    pub fn bbox(&self) -> Option<[f64; 4]> {
        let mut b: Option<[f64; 4]> = None;
        self.for_each_run(&mut |run, stride| {
            for c in run.chunks_exact(stride) {
                let (x, y) = (c[0], c[1]);
                b = Some(match b {
                    None => [x, y, x, y],
                    Some(v) => [v[0].min(x), v[1].min(y), v[2].max(x), v[3].max(y)],
                });
            }
        });
        b
    }

    pub fn centroid(&self) -> Option<(f64, f64)> {
        let b = self.bbox()?;
        // Work relative to a vertex of the geometry: building footprints span
        // about 1e-5 degrees, so absolute longitudes would lose the area terms
        // to rounding.
        let mut acc = CentroidAcc {
            ox: b[0],
            oy: b[1],
            ..Default::default()
        };
        acc.add(self);
        acc.finish()
    }

    fn is_empty(&self) -> bool {
        match &self.body {
            Body::Point(c) | Body::LineString(c) => c.is_empty(),
            Body::Polygon(rings) => rings.is_empty(),
            Body::Multi(_, members) => members.is_empty(),
        }
    }

    pub fn to_wkt(&self) -> String {
        let mut out = String::new();
        self.write_tagged(&mut out);
        out
    }

    fn write_tagged(&self, out: &mut String) {
        out.push_str(match &self.body {
            Body::Point(_) => "POINT",
            Body::LineString(_) => "LINESTRING",
            Body::Polygon(_) => "POLYGON",
            Body::Multi(MultiKind::Point, _) => "MULTIPOINT",
            Body::Multi(MultiKind::LineString, _) => "MULTILINESTRING",
            Body::Multi(MultiKind::Polygon, _) => "MULTIPOLYGON",
            Body::Multi(MultiKind::Collection, _) => "GEOMETRYCOLLECTION",
        });
        out.push_str(match (self.dims.z, self.dims.m) {
            (false, false) => "",
            (true, false) => " Z",
            (false, true) => " M",
            (true, true) => " ZM",
        });
        if self.is_empty() {
            out.push_str(" EMPTY");
        } else {
            out.push(' ');
            self.write_body(out);
        }
    }

    fn write_body(&self, out: &mut String) {
        let stride = self.dims.stride();
        match &self.body {
            Body::Point(c) | Body::LineString(c) => {
                out.push('(');
                write_run(out, c, stride);
                out.push(')');
            }
            Body::Polygon(rings) => {
                out.push('(');
                for (i, ring) in rings.iter().enumerate() {
                    if i > 0 {
                        out.push_str(", ");
                    }
                    out.push('(');
                    write_run(out, ring, stride);
                    out.push(')');
                }
                out.push(')');
            }
            Body::Multi(kind, members) => {
                out.push('(');
                for (i, member) in members.iter().enumerate() {
                    if i > 0 {
                        out.push_str(", ");
                    }
                    if *kind == MultiKind::Collection {
                        member.write_tagged(out);
                    } else if member.is_empty() {
                        out.push_str("EMPTY");
                    } else {
                        member.write_body(out);
                    }
                }
                out.push(')');
            }
        }
    }
}

fn write_run(out: &mut String, run: &[f64], stride: usize) {
    for (i, c) in run.chunks_exact(stride).enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        for (j, v) in c.iter().enumerate() {
            if j > 0 {
                out.push(' ');
            }
            write!(out, "{v}").expect("writing to a String cannot fail");
        }
    }
}
