use regex::Regex;

pub struct StataToSqlRegexConverter {
    replacements: Vec<(Regex, String)>,
}

impl StataToSqlRegexConverter {
    pub fn new() -> Self {
        let mut converter = StataToSqlRegexConverter {
            replacements: Vec::new(),
        };
        converter.add_patterns();
        converter
    }

    fn add_patterns(&mut self) {
        self.add_replacement(r"!missing\s*\(\s*([^)]+)\s*\)", "$1 IS NOT NULL");
        self.add_replacement(r"missing\s*\(\s*([^)]+)\s*\)", "$1 IS NULL");
        self.add_replacement(
            r"inrange\s*\(\s*([^,]+)\s*,\s*([^,]+)\s*,\s*([^)]+)\s*\)",
            "$1 BETWEEN $2 AND $3",
        );
        self.add_replacement(r"inlist\s*\(\s*([^,]+)\s*,\s*([^)]+)\s*\)", "$1 IN ($2)");
        self.add_replacement(r"mod\s*\(\s*([^,]+)\s*,\s*([^)]+)\s*\)", "($1 % $2)");
        self.add_replacement(r"ceil\s*\(\s*([^)]+)\s*\)", "CEILING($1)");
        self.add_replacement(r"floor\s*\(\s*([^)]+)\s*\)", "FLOOR($1)");
        self.add_replacement(r"round\s*\(\s*([^)]+)\s*\)", "ROUND($1)");
        self.add_replacement(r"real\s*\(\s*([^)]+)\s*\)", "CAST($1 AS REAL)");
        self.add_replacement(r"string\s*\(\s*([^)]+)\s*\)", "CAST($1 AS VARCHAR)");
        self.add_replacement(r"\s*&\s*", " AND ");
        self.add_replacement(r"\s*\|\s*", " OR ");
        self.add_replacement(r"==", "=");
        self.add_replacement(r"!\s*\(", "NOT (");
    }

    fn add_replacement(&mut self, pattern: &str, replacement: &str) {
        let regex = Regex::new(pattern).unwrap();
        self.replacements.push((regex, replacement.to_string()));
    }

    fn split_preserving_quotes(&self, input: &str) -> Vec<(String, bool)> {
        let mut parts = Vec::new();
        let mut current = String::new();
        let mut in_quote = false;
        let mut quote_char = None;

        for ch in input.chars() {
            match ch {
                '"' | '\'' if !in_quote => {
                    if !current.is_empty() {
                        parts.push((current.clone(), false));
                        current.clear();
                    }
                    current.push(ch);
                    in_quote = true;
                    quote_char = Some(ch);
                }
                _ if in_quote && Some(ch) == quote_char => {
                    current.push(ch);
                    parts.push((current.clone(), true));
                    current.clear();
                    in_quote = false;
                    quote_char = None;
                }
                _ => current.push(ch),
            }
        }

        if !current.is_empty() {
            parts.push((current, in_quote));
        }

        parts
    }

    pub fn convert(&self, input: &str) -> String {
        let mut result = String::new();
        for (content, is_quoted) in self.split_preserving_quotes(input) {
            if is_quoted {
                result.push_str(&content);
            } else {
                let mut processed = content;
                for (regex, replacement) in &self.replacements {
                    processed = regex
                        .replace_all(&processed, replacement.as_str())
                        .to_string();
                }
                result.push_str(&processed);
            }
        }

        result.replace('"', "'")
    }
}

pub fn stata_to_sql(input: &str) -> String {
    StataToSqlRegexConverter::new().convert(input)
}

#[cfg(test)]
mod tests {
    use super::stata_to_sql;

    fn assert_translation(input: &str, expected: &str) {
        assert_eq!(stata_to_sql(input), expected, "input: {input}");
    }

    #[test]
    fn converts_basic_stata_predicate() {
        assert_eq!(stata_to_sql("x == 1 & y == 2"), "x = 1 AND y = 2");
    }

    #[test]
    fn converts_missing_helpers() {
        assert_eq!(stata_to_sql("missing(age)"), "age IS NULL");
        assert_eq!(stata_to_sql("!missing(age)"), "age IS NOT NULL");
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
    fn handles_nested_helper_calls() {
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
}
