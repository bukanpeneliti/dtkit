program dtparquet
    version 16
    gettoken subcmd 0 : 0
    
    if "`subcmd'" == "save" {
        _dtparquet_save `0'
    }
    else if "`subcmd'" == "use" {
        _dtparquet_use `0'
    }
    else {
        di as err "Subcommand `subcmd' not recognized"
        exit 198
    }
end

program _dtparquet_save
    syntax anything(name=filename) [if] [in], [replace vars(varlist)]
    
    local filename `filename'
    if !strpos("`filename'", ".") {
        local filename "`filename'.parquet"
    }
    
    capture confirm file "`filename'"
    if _rc == 0 & "`replace'" == "" {
        di as err "file `filename' already exists"
        exit 110
    }
    
    // 1. Create temporary DTA file with the subset of data
    preserve
    if "`if'`in'" != "" quietly keep `if' `in'
    if "`vars'" != "" quietly keep `vars'
    tempfile tmpdta
    quietly save "`tmpdta'"
    restore
    
    // 2. Find dtparquet.py
    quietly findfile dtparquet.ado
    local py_path = subinstr("`r(fn)'", "dtparquet.ado", "dtparquet.py", 1)
    
    // 3. Execute uv run
    di "dtparquet: converting to parquet..."
    shell uv run --with pandas --with pyarrow --with stata_setup --with pystata python "`py_path'" save "`tmpdta'" "`filename'"
    
    if _rc {
        di as err "dtparquet error: failed to save parquet file"
        exit _rc
    }
end

program _dtparquet_use
    syntax anything(name=filename) [, clear vars(varlist)]
    
    local filename `filename'
    if !strpos("`filename'", ".") {
        local filename "`filename'.parquet"
    }
    
    if "`clear'" == "" {
        quietly describe
        if r(N) > 0 | r(k) > 0 {
            di as err "no; dataset in memory has changed since last saved"
            exit 4
        }
    }
    
    // 1. Prepare temporary DTA path
    tempfile tmpdta
    
    // 2. Find dtparquet.py
    quietly findfile dtparquet.ado
    local py_path = subinstr("`r(fn)'", "dtparquet.ado", "dtparquet.py", 1)
    
    // 3. Execute uv run
    di "dtparquet: loading parquet file..."
    shell uv run --with pandas --with pyarrow --with stata_setup --with pystata python "`py_path'" use "`filename'" "`tmpdta'"
    
    if _rc {
        di as err "dtparquet error: failed to load parquet file"
        exit _rc
    }
    
    // 4. Load the temporary DTA
    use "`tmpdta'", clear
    if "`vars'" != "" quietly keep `vars'
end
