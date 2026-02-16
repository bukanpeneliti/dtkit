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

mod filter_impl_lang;

use filter_impl_lang::{
    char_at, is_identifier_continue, is_identifier_start, parse_bool_expr, parse_single_call,
    BoolExpr, CallNode, FilterTranslator,
};

pub fn convert_if_sql(input: &str) -> String {
    FilterTranslator::new().translate(input)
}

pub fn compile_if_expr(input: &str) -> Result<Expr, FilterError> {
    let expr = parse_bool_expr(input);
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
