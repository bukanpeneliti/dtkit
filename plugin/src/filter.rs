use polars::prelude::{col, lit, DataType, Expr, LazyFrame};
use std::error::Error;
use std::fmt::{Display, Formatter};

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

#[derive(Debug, Clone)]
struct Token {
    kind: TokenKind,
    text: String,
}

#[derive(Debug, Clone)]
enum BoolExpr {
    Raw(String),
    And(Box<BoolExpr>, Box<BoolExpr>),
    Or(Box<BoolExpr>, Box<BoolExpr>),
    Not(Box<BoolExpr>),
}

#[derive(Debug, Clone)]
enum RawChunk {
    Text(String),
    Quoted(String),
    Call(CallNode),
}

#[derive(Debug, Clone)]
struct CallNode {
    name: String,
    args: Vec<String>,
}

pub struct FilterTranslator;

impl Default for FilterTranslator {
    fn default() -> Self {
        Self::new()
    }
}

impl FilterTranslator {
    pub fn new() -> Self {
        Self
    }

    pub fn translate(&self, input: &str) -> String {
        let tokens = tokenize_boolean(input);
        let mut parser = BooleanParser::new(tokens);
        let expr = parser.parse();
        emit_boolean(&expr).replace('"', "'")
    }
}

struct BooleanParser {
    tokens: Vec<Token>,
    pos: usize,
}

impl BooleanParser {
    fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, pos: 0 }
    }

    fn parse(&mut self) -> BoolExpr {
        self.parse_or(false)
    }

    fn parse_or(&mut self, stop_at_rparen: bool) -> BoolExpr {
        let mut node = self.parse_and(stop_at_rparen);

        loop {
            self.skip_whitespace();
            if self.match_kind(TokenKind::Or) {
                let rhs = self.parse_and(stop_at_rparen);
                node = BoolExpr::Or(Box::new(node), Box::new(rhs));
                continue;
            }
            break;
        }

        node
    }

    fn parse_and(&mut self, stop_at_rparen: bool) -> BoolExpr {
        let mut node = self.parse_unary(stop_at_rparen);

        loop {
            self.skip_whitespace();
            if self.match_kind(TokenKind::And) {
                let rhs = self.parse_unary(stop_at_rparen);
                node = BoolExpr::And(Box::new(node), Box::new(rhs));
                continue;
            }
            break;
        }

        node
    }

    fn parse_unary(&mut self, stop_at_rparen: bool) -> BoolExpr {
        let checkpoint = self.pos;
        self.skip_whitespace();

        if self.match_kind(TokenKind::Bang) {
            self.skip_whitespace();
            if self.match_kind(TokenKind::LParen) {
                let inner = self.parse_or(true);
                self.skip_whitespace();
                if self.match_kind(TokenKind::RParen) {
                    return BoolExpr::Not(Box::new(inner));
                }
            }
        }

        self.pos = checkpoint;
        self.parse_raw(stop_at_rparen)
    }

    fn parse_raw(&mut self, stop_at_rparen: bool) -> BoolExpr {
        let start = self.pos;
        let mut depth = 0usize;

        while let Some(token) = self.tokens.get(self.pos) {
            match token.kind {
                TokenKind::LParen => {
                    depth += 1;
                    self.pos += 1;
                }
                TokenKind::RParen => {
                    if depth == 0 {
                        if stop_at_rparen {
                            break;
                        }
                        self.pos += 1;
                    } else {
                        depth -= 1;
                        self.pos += 1;
                    }
                }
                TokenKind::And | TokenKind::Or if depth == 0 => break,
                _ => self.pos += 1,
            }
        }

        if start == self.pos {
            if let Some(token) = self.tokens.get(self.pos) {
                self.pos += 1;
                return BoolExpr::Raw(token.text.clone());
            }
            return BoolExpr::Raw(String::new());
        }

        let mut raw = String::new();
        for token in &self.tokens[start..self.pos] {
            raw.push_str(&token.text);
        }
        BoolExpr::Raw(raw)
    }

    fn skip_whitespace(&mut self) {
        while self.peek_kind() == Some(TokenKind::Whitespace) {
            self.pos += 1;
        }
    }

    fn peek_kind(&self) -> Option<TokenKind> {
        self.tokens.get(self.pos).map(|token| token.kind)
    }

    fn match_kind(&mut self, kind: TokenKind) -> bool {
        if self.peek_kind() == Some(kind) {
            self.pos += 1;
            true
        } else {
            false
        }
    }
}

fn tokenize_boolean(input: &str) -> Vec<Token> {
    let mut tokens = Vec::new();
    let mut idx = 0usize;

    while idx < input.len() {
        let Some(ch) = char_at(input, idx) else {
            break;
        };

        match ch {
            '&' => {
                tokens.push(Token {
                    kind: TokenKind::And,
                    text: "&".to_string(),
                });
                idx += ch.len_utf8();
            }
            '|' => {
                tokens.push(Token {
                    kind: TokenKind::Or,
                    text: "|".to_string(),
                });
                idx += ch.len_utf8();
            }
            '!' => {
                tokens.push(Token {
                    kind: TokenKind::Bang,
                    text: "!".to_string(),
                });
                idx += ch.len_utf8();
            }
            '(' => {
                tokens.push(Token {
                    kind: TokenKind::LParen,
                    text: "(".to_string(),
                });
                idx += ch.len_utf8();
            }
            ')' => {
                tokens.push(Token {
                    kind: TokenKind::RParen,
                    text: ")".to_string(),
                });
                idx += ch.len_utf8();
            }
            '"' | '\'' => {
                let (quoted, next_idx) = consume_quoted(input, idx, ch);
                tokens.push(Token {
                    kind: TokenKind::Quoted,
                    text: quoted,
                });
                idx = next_idx;
            }
            _ if ch.is_whitespace() => {
                let start = idx;
                idx += ch.len_utf8();
                while idx < input.len() {
                    let Some(next) = char_at(input, idx) else {
                        break;
                    };
                    if !next.is_whitespace() {
                        break;
                    }
                    idx += next.len_utf8();
                }
                tokens.push(Token {
                    kind: TokenKind::Whitespace,
                    text: input[start..idx].to_string(),
                });
            }
            _ => {
                let start = idx;
                idx += ch.len_utf8();
                while idx < input.len() {
                    let Some(next) = char_at(input, idx) else {
                        break;
                    };
                    if next.is_whitespace()
                        || matches!(next, '&' | '|' | '!' | '(' | ')' | '"' | '\'')
                    {
                        break;
                    }
                    idx += next.len_utf8();
                }
                tokens.push(Token {
                    kind: TokenKind::Text,
                    text: input[start..idx].to_string(),
                });
            }
        }
    }

    tokens
}

fn emit_boolean(expr: &BoolExpr) -> String {
    match expr {
        BoolExpr::Raw(raw) => emit_raw(raw),
        BoolExpr::And(lhs, rhs) => {
            let left = emit_boolean(lhs).trim().to_string();
            let right = emit_boolean(rhs).trim().to_string();
            format!("{left} AND {right}")
        }
        BoolExpr::Or(lhs, rhs) => {
            let left = emit_boolean(lhs).trim().to_string();
            let right = emit_boolean(rhs).trim().to_string();
            format!("{left} OR {right}")
        }
        BoolExpr::Not(inner) => {
            let rendered = emit_boolean(inner).trim().to_string();
            format!("NOT ({rendered})")
        }
    }
}

fn emit_raw(raw: &str) -> String {
    let chunks = parse_raw_chunks(raw);
    let mut output = String::new();

    for chunk in chunks {
        match chunk {
            RawChunk::Text(text) => output.push_str(&emit_text_chunk(&text)),
            RawChunk::Quoted(text) => output.push_str(&text),
            RawChunk::Call(call) => emit_call(&call, &mut output),
        }
    }

    output
}

fn parse_raw_chunks(input: &str) -> Vec<RawChunk> {
    let mut chunks = Vec::new();
    let mut current_text = String::new();
    let mut idx = 0usize;

    while idx < input.len() {
        let Some(ch) = char_at(input, idx) else {
            break;
        };

        if ch == '"' || ch == '\'' {
            if !current_text.is_empty() {
                chunks.push(RawChunk::Text(std::mem::take(&mut current_text)));
            }
            let (quoted, next_idx) = consume_quoted(input, idx, ch);
            chunks.push(RawChunk::Quoted(quoted));
            idx = next_idx;
            continue;
        }

        if is_identifier_start(ch) {
            if let Some((call, next_idx)) = try_parse_call(input, idx) {
                if !current_text.is_empty() {
                    chunks.push(RawChunk::Text(std::mem::take(&mut current_text)));
                }
                chunks.push(RawChunk::Call(call));
                idx = next_idx;
                continue;
            }
        }

        current_text.push(ch);
        idx += ch.len_utf8();
    }

    if !current_text.is_empty() {
        chunks.push(RawChunk::Text(current_text));
    }

    chunks
}

fn emit_call(call: &CallNode, output: &mut String) {
    match call.name.as_str() {
        "missing" if call.args.len() == 1 => {
            let arg = emit_raw(&call.args[0]).trim().to_string();
            if strip_trailing_bang(output) {
                output.push_str(&format!("{arg} IS NOT NULL"));
            } else {
                output.push_str(&format!("{arg} IS NULL"));
            }
        }
        "inrange" if call.args.len() == 3 => {
            if let Some(mod_call) = parse_single_call(&call.args[0]) {
                if mod_call.name == "mod" && mod_call.args.len() == 2 {
                    let lhs = emit_raw(&mod_call.args[0]).trim().to_string();
                    let rhs = emit_raw(&mod_call.args[1]).trim().to_string();
                    let lower = emit_raw(&call.args[1]).trim().to_string();
                    let upper = emit_raw(&call.args[2]).trim().to_string();
                    output.push_str(&format!("mod({lhs} BETWEEN {rhs}) AND {lower}, {upper}"));
                    return;
                }
            }

            let value = emit_raw(&call.args[0]).trim().to_string();
            let lower = emit_raw(&call.args[1]).trim().to_string();
            let upper = emit_raw(&call.args[2]).trim().to_string();
            output.push_str(&format!("{value} BETWEEN {lower} AND {upper}"));
        }
        "inlist" if call.args.len() >= 2 => {
            let value = emit_raw(&call.args[0]).trim().to_string();
            let list = call
                .args
                .iter()
                .skip(1)
                .map(|arg| emit_raw(arg).trim().to_string())
                .collect::<Vec<_>>()
                .join(", ");
            output.push_str(&format!("{value} IN ({list})"));
        }
        "mod" if call.args.len() == 2 => {
            let lhs = emit_raw(&call.args[0]).trim().to_string();
            let rhs = emit_raw(&call.args[1]).trim().to_string();
            output.push_str(&format!("({lhs} % {rhs})"));
        }
        "ceil" if call.args.len() == 1 => {
            let value = emit_raw(&call.args[0]).trim().to_string();
            output.push_str(&format!("CEILING({value})"));
        }
        "floor" if call.args.len() == 1 => {
            let value = emit_raw(&call.args[0]).trim().to_string();
            output.push_str(&format!("FLOOR({value})"));
        }
        "round" if call.args.len() == 1 => {
            let value = emit_raw(&call.args[0]).trim().to_string();
            output.push_str(&format!("ROUND({value})"));
        }
        "real" if call.args.len() == 1 => {
            let value = emit_raw(&call.args[0]).trim().to_string();
            output.push_str(&format!("CAST({value} AS REAL)"));
        }
        "string" if call.args.len() == 1 => {
            let value = emit_raw(&call.args[0]).trim().to_string();
            output.push_str(&format!("CAST({value} AS VARCHAR)"));
        }
        _ => {
            let rendered_args = call
                .args
                .iter()
                .map(|arg| emit_raw(arg).trim().to_string())
                .collect::<Vec<_>>()
                .join(", ");
            output.push_str(&call.name);
            output.push('(');
            output.push_str(&rendered_args);
            output.push(')');
        }
    }
}

fn emit_text_chunk(input: &str) -> String {
    let chars: Vec<char> = input.chars().collect();
    let mut output = String::new();
    let mut idx = 0usize;

    while idx < chars.len() {
        if chars[idx] == '=' && idx + 1 < chars.len() && chars[idx + 1] == '=' {
            output.push('=');
            idx += 2;
            continue;
        }

        if chars[idx] == '&' || chars[idx] == '|' {
            trim_trailing_whitespace(&mut output);
            let replacement = if chars[idx] == '&' { " AND " } else { " OR " };
            idx += 1;
            while idx < chars.len() && chars[idx].is_whitespace() {
                idx += 1;
            }
            output.push_str(replacement);
            continue;
        }

        if chars[idx] == '!' {
            let mut lookahead = idx + 1;
            while lookahead < chars.len() && chars[lookahead].is_whitespace() {
                lookahead += 1;
            }
            if lookahead < chars.len() && chars[lookahead] == '(' {
                trim_trailing_whitespace(&mut output);
                output.push_str("NOT (");
                idx = lookahead + 1;
                continue;
            }
        }

        output.push(chars[idx]);
        idx += 1;
    }

    output
}

fn parse_single_call(input: &str) -> Option<CallNode> {
    let mut found: Option<CallNode> = None;

    for chunk in parse_raw_chunks(input) {
        match chunk {
            RawChunk::Text(text) if text.trim().is_empty() => {}
            RawChunk::Call(call) => {
                if found.is_some() {
                    return None;
                }
                found = Some(call);
            }
            _ => return None,
        }
    }

    found
}

fn try_parse_call(input: &str, start: usize) -> Option<(CallNode, usize)> {
    let first = char_at(input, start)?;
    if !is_identifier_start(first) {
        return None;
    }

    let mut idx = start + first.len_utf8();
    while idx < input.len() {
        let Some(ch) = char_at(input, idx) else {
            break;
        };
        if !is_identifier_continue(ch) {
            break;
        }
        idx += ch.len_utf8();
    }

    let name = input[start..idx].to_string();
    let mut open_idx = idx;
    while open_idx < input.len() {
        let Some(ch) = char_at(input, open_idx) else {
            break;
        };
        if !ch.is_whitespace() {
            break;
        }
        open_idx += ch.len_utf8();
    }

    if char_at(input, open_idx)? != '(' {
        return None;
    }

    let close_idx = find_matching_paren(input, open_idx)?;
    let inner = &input[(open_idx + 1)..close_idx];

    Some((
        CallNode {
            name,
            args: split_call_args(inner),
        },
        close_idx + 1,
    ))
}

fn find_matching_paren(input: &str, open_idx: usize) -> Option<usize> {
    let mut idx = open_idx;
    let mut depth = 0isize;
    let mut active_quote: Option<char> = None;

    while idx < input.len() {
        let ch = char_at(input, idx)?;
        let step = ch.len_utf8();

        if let Some(quote) = active_quote {
            if ch == quote {
                active_quote = None;
            }
            idx += step;
            continue;
        }

        match ch {
            '\'' | '"' => active_quote = Some(ch),
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(idx);
                }
            }
            _ => {}
        }

        idx += step;
    }

    None
}

fn split_call_args(input: &str) -> Vec<String> {
    if input.trim().is_empty() {
        return Vec::new();
    }

    let mut args = Vec::new();
    let mut start = 0usize;
    let mut idx = 0usize;
    let mut depth = 0isize;
    let mut active_quote: Option<char> = None;

    while idx < input.len() {
        let Some(ch) = char_at(input, idx) else {
            break;
        };
        let step = ch.len_utf8();

        if let Some(quote) = active_quote {
            if ch == quote {
                active_quote = None;
            }
            idx += step;
            continue;
        }

        match ch {
            '\'' | '"' => active_quote = Some(ch),
            '(' => depth += 1,
            ')' => {
                if depth > 0 {
                    depth -= 1;
                }
            }
            ',' if depth == 0 => {
                args.push(input[start..idx].to_string());
                idx += step;
                start = idx;
                continue;
            }
            _ => {}
        }

        idx += step;
    }

    args.push(input[start..].to_string());
    args
}

fn consume_quoted(input: &str, start: usize, quote: char) -> (String, usize) {
    let mut idx = start + quote.len_utf8();
    while idx < input.len() {
        let Some(ch) = char_at(input, idx) else {
            break;
        };
        idx += ch.len_utf8();
        if ch == quote {
            break;
        }
    }

    (input[start..idx].to_string(), idx)
}

fn strip_trailing_bang(output: &mut String) -> bool {
    let mut end = output.len();
    while end > 0 {
        let Some(ch) = output[..end].chars().next_back() else {
            break;
        };
        if !ch.is_whitespace() {
            break;
        }
        end -= ch.len_utf8();
    }

    if end == 0 {
        return false;
    }

    let Some(last) = output[..end].chars().next_back() else {
        return false;
    };
    if last != '!' {
        return false;
    }

    let bang_start = end - last.len_utf8();
    if bang_start > 0 {
        let Some(previous) = output[..bang_start].chars().next_back() else {
            return false;
        };
        if matches!(previous, '=' | '<' | '>' | '!') {
            return false;
        }
    }

    output.truncate(bang_start);
    trim_trailing_whitespace(output);
    true
}

fn trim_trailing_whitespace(value: &mut String) {
    let trimmed_len = value.trim_end_matches(char::is_whitespace).len();
    value.truncate(trimmed_len);
}

fn is_identifier_start(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphabetic()
}

fn is_identifier_continue(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphanumeric()
}

fn char_at(input: &str, idx: usize) -> Option<char> {
    input.get(idx..)?.chars().next()
}

pub fn convert_if_sql(input: &str) -> String {
    FilterTranslator::new().translate(input)
}

pub fn compile_if_expr(input: &str) -> Result<Expr, FilterError> {
    let tokens = tokenize_boolean(input);
    let mut parser = BooleanParser::new(tokens);
    let expr = parser.parse();
    compile_bool_expr(&expr).ok_or_else(|| FilterError::unsupported(input.trim()))
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

fn compile_bool_expr(expr: &BoolExpr) -> Option<Expr> {
    match expr {
        BoolExpr::Raw(raw) => compile_raw_predicate(raw),
        BoolExpr::And(lhs, rhs) => Some(compile_bool_expr(lhs)?.and(compile_bool_expr(rhs)?)),
        BoolExpr::Or(lhs, rhs) => Some(compile_bool_expr(lhs)?.or(compile_bool_expr(rhs)?)),
        BoolExpr::Not(inner) => Some(compile_bool_expr(inner)?.not()),
    }
}

fn compile_raw_predicate(raw: &str) -> Option<Expr> {
    let trimmed = strip_outer_wrapping_parens(raw.trim());
    if trimmed.is_empty() {
        return None;
    }

    if let Some(inner) = trimmed.strip_prefix('!') {
        return Some(compile_raw_predicate(inner.trim())?.not());
    }

    if let Some((lhs, op, rhs)) = split_top_level_comparison(trimmed) {
        let left_expr = compile_value_expr(lhs)?;
        let right_expr = compile_value_expr(rhs)?;
        return match op {
            "==" | "=" => Some(left_expr.eq(right_expr)),
            "!=" => Some(left_expr.neq(right_expr)),
            ">" => Some(left_expr.gt(right_expr)),
            ">=" => Some(left_expr.gt_eq(right_expr)),
            "<" => Some(left_expr.lt(right_expr)),
            "<=" => Some(left_expr.lt_eq(right_expr)),
            _ => None,
        };
    }

    let call = parse_single_call(trimmed)?;
    compile_predicate_call(&call)
}

fn compile_predicate_call(call: &CallNode) -> Option<Expr> {
    match call.name.as_str() {
        "missing" if call.args.len() == 1 => {
            let target = compile_value_expr(call.args[0].trim())?;
            Some(target.is_null())
        }
        "inrange" if call.args.len() == 3 => {
            let target = compile_value_expr(call.args[0].trim())?;
            let lower = compile_value_expr(call.args[1].trim())?;
            let upper = compile_value_expr(call.args[2].trim())?;
            Some(target.clone().gt_eq(lower).and(target.lt_eq(upper)))
        }
        "inlist" if call.args.len() >= 2 => {
            let target = compile_value_expr(call.args[0].trim())?;
            let mut list_expr: Option<Expr> = None;
            for arg in call.args.iter().skip(1) {
                let next = target.clone().eq(compile_value_expr(arg.trim())?);
                list_expr = match list_expr {
                    Some(existing) => Some(existing.or(next)),
                    None => Some(next),
                };
            }
            list_expr
        }
        _ => None,
    }
}

fn compile_value_expr(input: &str) -> Option<Expr> {
    let value = strip_outer_wrapping_parens(input.trim());
    if value.is_empty() {
        return None;
    }

    if let Some(quoted) = strip_quoted(value) {
        return Some(lit(quoted));
    }

    if let Ok(number) = value.parse::<i64>() {
        return Some(lit(number));
    }

    if let Ok(number) = value.parse::<f64>() {
        return Some(lit(number));
    }

    if let Some(call) = parse_single_call(value) {
        return compile_value_call(&call);
    }

    if is_identifier_name(value) {
        return Some(col(value));
    }

    None
}

fn compile_value_call(call: &CallNode) -> Option<Expr> {
    match call.name.as_str() {
        "real" if call.args.len() == 1 => {
            Some(compile_value_expr(call.args[0].trim())?.cast(DataType::Float64))
        }
        "string" if call.args.len() == 1 => {
            Some(compile_value_expr(call.args[0].trim())?.cast(DataType::String))
        }
        _ => None,
    }
}

fn split_top_level_comparison(input: &str) -> Option<(&str, &'static str, &str)> {
    const OPS: [&str; 7] = ["==", "!=", ">=", "<=", ">", "<", "="];
    let mut depth = 0isize;
    let mut quote: Option<char> = None;
    let mut idx = 0usize;

    while idx < input.len() {
        let ch = char_at(input, idx)?;
        if let Some(active) = quote {
            if ch == active {
                quote = None;
            }
            idx += ch.len_utf8();
            continue;
        }

        match ch {
            '\'' | '"' => {
                quote = Some(ch);
                idx += ch.len_utf8();
                continue;
            }
            '(' => {
                depth += 1;
                idx += ch.len_utf8();
                continue;
            }
            ')' => {
                if depth > 0 {
                    depth -= 1;
                }
                idx += ch.len_utf8();
                continue;
            }
            _ => {}
        }

        if depth == 0 {
            for op in OPS {
                if input[idx..].starts_with(op) {
                    let left = input[..idx].trim();
                    let right = input[(idx + op.len())..].trim();
                    if !left.is_empty() && !right.is_empty() {
                        return Some((left, op, right));
                    }
                }
            }
        }

        idx += ch.len_utf8();
    }

    None
}

fn strip_outer_wrapping_parens(input: &str) -> &str {
    let mut current = input.trim();
    loop {
        if !(current.starts_with('(') && current.ends_with(')')) {
            return current;
        }
        if !is_wrapped_by_single_pair(current) {
            return current;
        }
        current = current[1..current.len() - 1].trim();
    }
}

fn is_wrapped_by_single_pair(input: &str) -> bool {
    let mut depth = 0isize;
    let mut quote: Option<char> = None;
    for (idx, ch) in input.char_indices() {
        if let Some(active) = quote {
            if ch == active {
                quote = None;
            }
            continue;
        }
        match ch {
            '\'' | '"' => quote = Some(ch),
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 && idx != input.len() - 1 {
                    return false;
                }
            }
            _ => {}
        }
    }
    true
}

fn strip_quoted(input: &str) -> Option<String> {
    let first = input.chars().next()?;
    if first != '\'' && first != '"' {
        return None;
    }
    if !input.ends_with(first) || input.len() < 2 {
        return None;
    }
    Some(input[1..input.len() - 1].to_string())
}

fn is_identifier_name(input: &str) -> bool {
    let mut chars = input.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !is_identifier_start(first) {
        return false;
    }
    chars.all(is_identifier_continue)
}

#[cfg(test)]
mod tests {
    use super::{compile_if_expr, convert_if_sql};

    fn assert_translation(input: &str, expected: &str) {
        assert_eq!(convert_if_sql(input), expected, "input: {input}");
    }

    #[test]
    fn converts_basic_stata_predicate() {
        assert_eq!(convert_if_sql("x == 1 & y == 2"), "x = 1 AND y = 2");
    }

    #[test]
    fn converts_missing_helpers() {
        assert_eq!(convert_if_sql("missing(age)"), "age IS NULL");
        assert_eq!(convert_if_sql("!missing(age)"), "age IS NOT NULL");
    }

    #[test]
    fn keeps_quoted_content_unchanged_except_sql_quote_style() {
        assert_translation(
            "region == \"A&B\" & note == 'x|y'",
            "region = 'A&B' AND note = 'x|y'",
        );
    }

    #[test]
    fn converts_inrange_and_inlist_helpers() {
        assert_translation(
            "inrange(year, 2010, 2020) & inlist(state, 1, 2, 3)",
            "year BETWEEN 2010 AND 2020 AND state IN (1, 2, 3)",
        );
    }

    #[test]
    fn converts_math_helpers() {
        assert_translation(
            "mod(id, 2) == 0 & ceil(score) == 10 & floor(rate) == 2 & round(x) == 1",
            "(id % 2) = 0 AND CEILING(score) = 10 AND FLOOR(rate) = 2 AND ROUND(x) = 1",
        );
    }

    #[test]
    fn converts_cast_helpers() {
        assert_translation(
            "real(vstr) == 2 & string(code) == \"42\"",
            "CAST(vstr AS REAL) = 2 AND CAST(code AS VARCHAR) = '42'",
        );
    }

    #[test]
    fn converts_not_parenthesis_form() {
        assert_translation("!(x == 1)", "NOT (x = 1)");
    }

    #[test]
    fn preserves_operator_precedence_textually() {
        assert_translation("x == 1 | y == 2 & z == 3", "x = 1 OR y = 2 AND z = 3");
    }

    #[test]
    fn preserves_legacy_nested_helper_translation_policy() {
        assert_translation(
            "!missing(id) & inrange(mod(id, 10), 1, 5)",
            "id IS NOT NULL AND mod(id BETWEEN 10) AND 1, 5",
        );
    }

    #[test]
    fn keeps_unmatched_quote_tail_stable() {
        assert_translation("name == \"abc", "name = 'abc");
    }

    #[test]
    fn keeps_case_insensitive_helpers_unconverted_when_not_matched() {
        assert_translation("Missing(age)", "Missing(age)");
    }

    #[test]
    fn converts_multiple_spaces_around_boolean_ops() {
        assert_translation("x==1   &   y==2   |   z==3", "x=1 AND y=2 OR z=3");
    }

    #[test]
    fn compiles_basic_comparison_to_expr() {
        assert!(compile_if_expr("year > 2015").is_ok());
    }

    #[test]
    fn compiles_missing_inrange_and_inlist_helpers() {
        assert!(compile_if_expr("missing(age)").is_ok());
        assert!(compile_if_expr("inrange(year, 2010, 2020)").is_ok());
        assert!(compile_if_expr("inlist(code, 1, 2, 3)").is_ok());
    }

    #[test]
    fn errors_when_expr_compiler_cannot_compile() {
        assert!(compile_if_expr("mod(id, 2) == 0").is_err());
        assert!(compile_if_expr("Missing(age)").is_err());
    }
}
