use polars::prelude::*;
use std::error::Error;
use std::fmt::{Display, Formatter};

// --- Error Type ---

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FilterError {
    message: String,
}
impl FilterError {
    fn unsupported(input: &str) -> Self {
        Self {
            message: format!("Unsupported if expression: {input}"),
        }
    }
}
impl Display for FilterError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}
impl Error for FilterError {}

// --- Public API ---

pub fn convert_if_sql(input: &str) -> String {
    FilterTranslator.translate(input)
}

pub fn compile_if_expr(input: &str) -> Result<Expr, FilterError> {
    compile_filter_semantics(&parse_bool_expr(input))
        .ok_or_else(|| FilterError::unsupported(input.trim()))
}

pub fn apply_if_filter(
    lf: LazyFrame,
    sql_if: Option<&str>,
) -> Result<(LazyFrame, bool), FilterError> {
    match sql_if.filter(|s| !s.trim().is_empty()) {
        Some(raw) => Ok((lf.filter(compile_if_expr(raw)?), true)),
        None => Ok((lf, false)),
    }
}

pub fn apply_cast(df: LazyFrame, type_mapping_json: &str) -> PolarsResult<LazyFrame> {
    if type_mapping_json.is_empty() {
        return Ok(df);
    }
    let type_mapping: serde_json::Map<String, serde_json::Value> =
        serde_json::from_str(type_mapping_json)
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
    let schema = df.clone().collect_schema()?;
    let mut pairs = Vec::new();
    for (t_str, cols_v) in type_mapping {
        if let serde_json::Value::Array(arr) = cols_v {
            let dt = match t_str.as_str() {
                "boolean" => DataType::Boolean,
                "uint8" => DataType::UInt8,
                "uint16" => DataType::UInt16,
                "uint32" => DataType::UInt32,
                "uint64" => DataType::UInt64,
                "int8" => DataType::Int8,
                "int16" => DataType::Int16,
                "int32" => DataType::Int32,
                "int64" => DataType::Int64,
                "float32" => DataType::Float32,
                "float64" => DataType::Float64,
                "string" => DataType::String,
                _ => continue,
            };
            for v in arr {
                if let serde_json::Value::String(n) = v {
                    if schema.get(&n).is_some() {
                        pairs.push((n, dt.clone()));
                    }
                }
            }
        }
    }
    Ok(if pairs.is_empty() {
        df
    } else {
        df.cast(
            pairs.iter().map(|(n, d)| (n.as_str(), d.clone())).collect(),
            true,
        )
    })
}

// --- Internal Implementation (Semantics) ---

fn compile_filter_semantics(expr: &BoolExpr) -> Option<Expr> {
    match expr {
        BoolExpr::Raw(raw) => compile_raw_predicate(raw),
        BoolExpr::And(l, r) => Some(compile_filter_semantics(l)?.and(compile_filter_semantics(r)?)),
        BoolExpr::Or(l, r) => Some(compile_filter_semantics(l)?.or(compile_filter_semantics(r)?)),
        BoolExpr::Not(i) => Some(compile_filter_semantics(i)?.not()),
    }
}

fn compile_raw_predicate(raw: &str) -> Option<Expr> {
    let t = strip_outer_wrapping_parens(raw.trim());
    if t.is_empty() {
        return None;
    }
    if let Some(inner) = t.strip_prefix('!') {
        return Some(compile_raw_predicate(inner.trim())?.not());
    }
    if let Some((lhs, op, rhs)) = split_top_level_comparison(t) {
        let l = compile_value_expr(lhs)?;
        let r = compile_value_expr(rhs)?;
        return match op {
            "==" | "=" => Some(l.eq(r)),
            "!=" => Some(l.neq(r)),
            ">" => Some(l.gt(r)),
            ">=" => Some(l.gt_eq(r)),
            "<" => Some(l.lt(r)),
            "<=" => Some(l.lt_eq(r)),
            _ => None,
        };
    }
    let call = parse_single_call(t)?;
    match call.name.as_str() {
        "missing" if call.args.len() == 1 => {
            Some(compile_value_expr(call.args[0].trim())?.is_null())
        }
        "inrange" if call.args.len() == 3 => {
            let v = compile_value_expr(call.args[0].trim())?;
            Some(
                v.clone()
                    .gt_eq(compile_value_expr(call.args[1].trim())?)
                    .and(v.lt_eq(compile_value_expr(call.args[2].trim())?)),
            )
        }
        "inlist" if call.args.len() >= 2 => {
            let v = compile_value_expr(call.args[0].trim())?;
            let mut list: Option<Expr> = None;
            for a in call.args.iter().skip(1) {
                let n = v.clone().eq(compile_value_expr(a.trim())?);
                list = Some(match list {
                    Some(ex) => ex.or(n),
                    None => n,
                });
            }
            list
        }
        _ => None,
    }
}

fn compile_value_expr(input: &str) -> Option<Expr> {
    let v = strip_outer_wrapping_parens(input.trim());
    if v.is_empty() {
        return None;
    }
    if let Some(q) = strip_quoted(v) {
        return Some(lit(q));
    }
    if let Ok(n) = v.parse::<i64>() {
        return Some(lit(n));
    }
    if let Ok(n) = v.parse::<f64>() {
        return Some(lit(n));
    }
    if let Some(call) = parse_single_call(v) {
        return match call.name.as_str() {
            "real" if call.args.len() == 1 => {
                Some(compile_value_expr(call.args[0].trim())?.cast(DataType::Float64))
            }
            "string" if call.args.len() == 1 => {
                Some(compile_value_expr(call.args[0].trim())?.cast(DataType::String))
            }
            _ => None,
        };
    }
    (v.chars().next()?.is_ascii_alphabetic() || v.starts_with('_')).then(|| col(v))
}

// --- Internal Implementation (Parsing & Language) ---

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TokenKind {
    Whitespace,
    Text,
    Quoted,
    And,
    Or,
    Bang,
    LParen,
    RParen,
}
struct Token {
    kind: TokenKind,
    text: String,
}
enum BoolExpr {
    Raw(String),
    And(Box<BoolExpr>, Box<BoolExpr>),
    Or(Box<BoolExpr>, Box<BoolExpr>),
    Not(Box<BoolExpr>),
}
enum RawChunk {
    Text(String),
    Quoted(String),
    Call(CallNode),
}
#[derive(Clone)]
struct CallNode {
    name: String,
    args: Vec<String>,
}

struct FilterTranslator;
impl FilterTranslator {
    fn translate(&self, input: &str) -> String {
        let expr = parse_bool_expr(input);
        emit_boolean(&expr).replace('"', "'")
    }
}

fn parse_bool_expr(input: &str) -> BoolExpr {
    let tokens = tokenize_boolean(input);
    let mut p = BooleanParser { tokens, pos: 0 };
    p.parse_or(false)
}

struct BooleanParser {
    tokens: Vec<Token>,
    pos: usize,
}
impl BooleanParser {
    fn parse_or(&mut self, stop: bool) -> BoolExpr {
        let mut n = self.parse_and(stop);
        while self.match_kind(TokenKind::Or) {
            n = BoolExpr::Or(Box::new(n), Box::new(self.parse_and(stop)));
        }
        n
    }
    fn parse_and(&mut self, stop: bool) -> BoolExpr {
        let mut n = self.parse_unary(stop);
        while self.match_kind(TokenKind::And) {
            n = BoolExpr::And(Box::new(n), Box::new(self.parse_unary(stop)));
        }
        n
    }
    fn parse_unary(&mut self, stop: bool) -> BoolExpr {
        let start = self.pos;
        self.skip_ws();
        if self.match_kind(TokenKind::Bang) {
            self.skip_ws();
            if self.match_kind(TokenKind::LParen) {
                let i = self.parse_or(true);
                self.skip_ws();
                if self.match_kind(TokenKind::RParen) {
                    return BoolExpr::Not(Box::new(i));
                }
            }
        }
        self.pos = start;
        self.parse_raw(stop)
    }
    fn parse_raw(&mut self, stop: bool) -> BoolExpr {
        let start = self.pos;
        let mut d = 0usize;
        while let Some(t) = self.tokens.get(self.pos) {
            match t.kind {
                TokenKind::LParen => {
                    d += 1;
                    self.pos += 1;
                }
                TokenKind::RParen => {
                    if d == 0 {
                        if stop {
                            break;
                        }
                        self.pos += 1;
                    } else {
                        d -= 1;
                        self.pos += 1;
                    }
                }
                TokenKind::And | TokenKind::Or if d == 0 => break,
                _ => self.pos += 1,
            }
        }
        if start == self.pos {
            return if let Some(t) = self.tokens.get(self.pos) {
                self.pos += 1;
                BoolExpr::Raw(t.text.clone())
            } else {
                BoolExpr::Raw(String::new())
            };
        }
        BoolExpr::Raw(
            self.tokens[start..self.pos]
                .iter()
                .map(|t| t.text.as_str())
                .collect(),
        )
    }
    fn skip_ws(&mut self) {
        while matches!(
            self.tokens.get(self.pos).map(|t| t.kind),
            Some(TokenKind::Whitespace)
        ) {
            self.pos += 1;
        }
    }
    fn match_kind(&mut self, k: TokenKind) -> bool {
        self.skip_ws();
        if self.tokens.get(self.pos).map(|t| t.kind) == Some(k) {
            self.pos += 1;
            true
        } else {
            false
        }
    }
}

fn tokenize_boolean(input: &str) -> Vec<Token> {
    let mut tokens = Vec::new();
    let mut i = 0usize;
    while i < input.len() {
        let ch = input[i..].chars().next().unwrap();
        match ch {
            '&' => {
                tokens.push(Token {
                    kind: TokenKind::And,
                    text: "&".into(),
                });
                i += 1;
            }
            '|' => {
                tokens.push(Token {
                    kind: TokenKind::Or,
                    text: "|".into(),
                });
                i += 1;
            }
            '!' => {
                tokens.push(Token {
                    kind: TokenKind::Bang,
                    text: "!".into(),
                });
                i += 1;
            }
            '(' => {
                tokens.push(Token {
                    kind: TokenKind::LParen,
                    text: "(".into(),
                });
                i += 1;
            }
            ')' => {
                tokens.push(Token {
                    kind: TokenKind::RParen,
                    text: ")".into(),
                });
                i += 1;
            }
            '"' | '\'' => {
                let (q, n) = consume_quoted(input, i, ch);
                tokens.push(Token {
                    kind: TokenKind::Quoted,
                    text: q,
                });
                i = n;
            }
            _ if ch.is_whitespace() => {
                let s = i;
                while i < input.len() && input[i..].chars().next().unwrap().is_whitespace() {
                    i += input[i..].chars().next().unwrap().len_utf8();
                }
                tokens.push(Token {
                    kind: TokenKind::Whitespace,
                    text: input[s..i].into(),
                });
            }
            _ => {
                let s = i;
                while i < input.len() {
                    let n = input[i..].chars().next().unwrap();
                    if n.is_whitespace() || matches!(n, '&' | '|' | '!' | '(' | ')' | '"' | '\'') {
                        break;
                    }
                    i += n.len_utf8();
                }
                tokens.push(Token {
                    kind: TokenKind::Text,
                    text: input[s..i].into(),
                });
            }
        }
    }
    tokens
}

fn emit_boolean(expr: &BoolExpr) -> String {
    match expr {
        BoolExpr::Raw(raw) => emit_raw(raw),
        BoolExpr::And(l, r) => format!("{} AND {}", emit_boolean(l).trim(), emit_boolean(r).trim()),
        BoolExpr::Or(l, r) => format!("{} OR {}", emit_boolean(l).trim(), emit_boolean(r).trim()),
        BoolExpr::Not(i) => format!("NOT ({})", emit_boolean(i).trim()),
    }
}

fn emit_raw(raw: &str) -> String {
    let mut out = String::new();
    for chunk in parse_raw_chunks(raw) {
        match chunk {
            RawChunk::Text(t) => out.push_str(&emit_text_chunk(&t)),
            RawChunk::Quoted(q) => out.push_str(&q),
            RawChunk::Call(c) => emit_call(&c, &mut out),
        }
    }
    out
}

fn parse_raw_chunks(input: &str) -> Vec<RawChunk> {
    let mut chunks = Vec::new();
    let mut txt = String::new();
    let mut i = 0usize;
    while i < input.len() {
        let ch = input[i..].chars().next().unwrap();
        if ch == '"' || ch == '\'' {
            if !txt.is_empty() {
                chunks.push(RawChunk::Text(std::mem::take(&mut txt)));
            }
            let (q, n) = consume_quoted(input, i, ch);
            chunks.push(RawChunk::Quoted(q));
            i = n;
            continue;
        }
        if ch.is_ascii_alphabetic() || ch == '_' {
            if let Some((c, n)) = try_parse_call(input, i) {
                if !txt.is_empty() {
                    chunks.push(RawChunk::Text(std::mem::take(&mut txt)));
                }
                chunks.push(RawChunk::Call(c));
                i = n;
                continue;
            }
        }
        txt.push(ch);
        i += ch.len_utf8();
    }
    if !txt.is_empty() {
        chunks.push(RawChunk::Text(txt));
    }
    chunks
}

fn emit_call(c: &CallNode, out: &mut String) {
    let args: Vec<String> = c.args.iter().map(|a| emit_raw(a).trim().into()).collect();
    match c.name.as_str() {
        "missing" if args.len() == 1 => {
            if strip_trailing_bang(out) {
                out.push_str(&format!("{} IS NOT NULL", args[0]));
            } else {
                out.push_str(&format!("{} IS NULL", args[0]));
            }
        }
        "inrange" if args.len() == 3 => {
            if let Some(m) = parse_single_call(&c.args[0]) {
                if m.name == "mod" && m.args.len() == 2 {
                    out.push_str(&format!(
                        "mod({} BETWEEN {}) AND {}, {}",
                        emit_raw(&m.args[0]).trim(),
                        emit_raw(&m.args[1]).trim(),
                        args[1],
                        args[2]
                    ));
                    return;
                }
            }
            out.push_str(&format!("{} BETWEEN {} AND {}", args[0], args[1], args[2]));
        }
        "inlist" if args.len() >= 2 => {
            out.push_str(&format!("{} IN ({})", args[0], args[1..].join(", ")))
        }
        "mod" if args.len() == 2 => out.push_str(&format!("({} % {})", args[0], args[1])),
        "ceil" if args.len() == 1 => out.push_str(&format!("CEILING({})", args[0])),
        "floor" if args.len() == 1 => out.push_str(&format!("FLOOR({})", args[0])),
        "round" if args.len() == 1 => out.push_str(&format!("ROUND({})", args[0])),
        "real" if args.len() == 1 => out.push_str(&format!("CAST({} AS REAL)", args[0])),
        "string" if args.len() == 1 => out.push_str(&format!("CAST({} AS VARCHAR)", args[0])),
        _ => out.push_str(&format!("{}({})", c.name, args.join(", "))),
    }
}

fn emit_text_chunk(input: &str) -> String {
    let mut out = String::new();
    let chars: Vec<char> = input.chars().collect();
    let mut i = 0usize;
    while i < chars.len() {
        if chars[i] == '=' && i + 1 < chars.len() && chars[i + 1] == '=' {
            out.push('=');
            i += 2;
            continue;
        }
        if chars[i] == '&' || chars[i] == '|' {
            let r = if chars[i] == '&' { " AND " } else { " OR " };
            i += 1;
            while i < chars.len() && chars[i].is_whitespace() {
                i += 1;
            }
            out = out.trim_end().to_string();
            out.push_str(r);
            continue;
        }
        if chars[i] == '!' {
            let mut l = i + 1;
            while l < chars.len() && chars[l].is_whitespace() {
                l += 1;
            }
            if l < chars.len() && chars[l] == '(' {
                out = out.trim_end().to_string();
                out.push_str("NOT (");
                i = l + 1;
                continue;
            }
        }
        out.push(chars[i]);
        i += 1;
    }
    out
}

fn parse_single_call(input: &str) -> Option<CallNode> {
    let chunks = parse_raw_chunks(input);
    if chunks.len() == 1 {
        if let RawChunk::Call(c) = &chunks[0] {
            return Some(c.clone());
        }
    }
    if chunks.len() > 1
        && chunks.iter().all(|c| match c {
            RawChunk::Call(_) => true,
            RawChunk::Text(t) => t.trim().is_empty(),
            _ => false,
        })
    {
        return chunks.iter().find_map(|c| {
            if let RawChunk::Call(cl) = c {
                Some(cl.clone())
            } else {
                None
            }
        });
    }
    None
}

fn try_parse_call(input: &str, start: usize) -> Option<(CallNode, usize)> {
    let mut i = start;
    while i < input.len() {
        let c = input[i..].chars().next()?;
        if c.is_ascii_alphanumeric() || c == '_' {
            i += c.len_utf8();
        } else {
            break;
        }
    }
    let name = input[start..i].to_string();
    while i < input.len() && input[i..].chars().next()?.is_whitespace() {
        i += input[i..].chars().next()?.len_utf8();
    }
    if input.get(i..i + 1)? != "(" {
        return None;
    }
    let close = find_matching_paren(input, i)?;
    Some((
        CallNode {
            name,
            args: split_call_args(&input[i + 1..close]),
        },
        close + 1,
    ))
}

fn find_matching_paren(input: &str, open: usize) -> Option<usize> {
    let mut d = 0isize;
    let mut q: Option<char> = None;
    for (i, ch) in input.char_indices().skip(open) {
        if let Some(active) = q {
            if ch == active {
                q = None;
            }
            continue;
        }
        match ch {
            '\'' | '"' => q = Some(ch),
            '(' => d += 1,
            ')' => {
                d -= 1;
                if d == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
    }
    None
}

fn split_call_args(input: &str) -> Vec<String> {
    let mut args = Vec::new();
    let mut s = 0usize;
    let mut d = 0isize;
    let mut q: Option<char> = None;
    for (i, ch) in input.char_indices() {
        if let Some(active) = q {
            if ch == active {
                q = None;
            }
            continue;
        }
        match ch {
            '\'' | '"' => q = Some(ch),
            '(' => d += 1,
            ')' => d -= 1,
            ',' if d == 0 => {
                args.push(input[s..i].into());
                s = i + 1;
            }
            _ => {}
        }
    }
    args.push(input[s..].into());
    args
}

fn consume_quoted(input: &str, start: usize, quote: char) -> (String, usize) {
    let mut i = start + 1;
    while i < input.len() {
        let ch = input[i..].chars().next().unwrap();
        i += ch.len_utf8();
        if ch == quote {
            break;
        }
    }
    (input[start..i].into(), i)
}

fn strip_trailing_bang(out: &mut String) -> bool {
    let t = out.trim_end();
    if t.ends_with('!') {
        let b_pos = t.len() - 1;
        if b_pos > 0 {
            let prev = t[..b_pos].chars().next_back().unwrap();
            if matches!(prev, '=' | '<' | '>' | '!') {
                return false;
            }
        }
        out.truncate(b_pos);
        *out = out.trim_end().into();
        return true;
    }
    false
}

fn split_top_level_comparison(input: &str) -> Option<(&str, &'static str, &str)> {
    let (mut d, mut q) = (0isize, None::<char>);
    for (i, ch) in input.char_indices() {
        if let Some(active) = q {
            if ch == active {
                q = None;
            }
            continue;
        }
        match ch {
            '\'' | '"' => q = Some(ch),
            '(' => d += 1,
            ')' => d -= 1,
            _ if d == 0 => {
                for op in ["==", "!=", ">=", "<=", ">", "<", "="] {
                    if input[i..].starts_with(op) {
                        return Some((input[..i].trim(), op, input[i + op.len()..].trim()));
                    }
                }
            }
            _ => {}
        }
    }
    None
}

fn strip_outer_wrapping_parens(input: &str) -> &str {
    let mut c = input.trim();
    while c.starts_with('(') && c.ends_with(')') {
        let (mut d, mut ok) = (0isize, true);
        for (i, ch) in c.char_indices() {
            match ch {
                '(' => d += 1,
                ')' => {
                    d -= 1;
                    if d == 0 && i < c.len() - 1 {
                        ok = false;
                        break;
                    }
                }
                _ => {}
            }
        }
        if !ok {
            break;
        }
        c = c[1..c.len() - 1].trim();
    }
    c
}

fn strip_quoted(input: &str) -> Option<String> {
    let f = input.chars().next()?;
    if (f == '\'' || f == '"') && input.ends_with(f) && input.len() >= 2 {
        Some(input[1..input.len() - 1].into())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn filter_logic() {
        assert_eq!(convert_if_sql("x == 1 & y == 2"), "x = 1 AND y = 2");
        assert_eq!(convert_if_sql("missing(age)"), "age IS NULL");
        assert_eq!(convert_if_sql("!missing(age)"), "age IS NOT NULL");
        assert!(compile_if_expr("year > 2015").is_ok());
    }
}
