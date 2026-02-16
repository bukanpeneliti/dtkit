use dtparquet::if_filter::compile_if_expr;

#[test]
fn filter_compiler_is_strict() {
    assert!(compile_if_expr("id == 1").is_ok());
    assert!(compile_if_expr("mod(id, 2) == 0").is_err());
}
