program dtparquet
    version 16
    syntax [anything] [if] [in] [, replace clear *]
    
    gettoken subcmd anything : anything
    
    if "`subcmd'" == "save" {
        _dtparquet_save `anything' `if' `in', `replace' `options'
    }
    else if "`subcmd'" == "use" {
        _dtparquet_use `anything', `clear' `options'
    }
    else {
        di as err "Subcommand `subcmd' not recognized"
        exit 198
    }
end

program _dtparquet_save
    syntax anything(name=filename) [if] [in], [replace varlist(varlist)]
    local filename `filename'
    if !strpos("`filename'", ".") {
        local filename "`filename'.parquet"
    }
    capture confirm file "`filename'"
    if _rc == 0 & "`replace'" == "" {
        di as err "file `filename' already exists"
        exit 110
    }
    marksample touse

    quietly findfile dtparquet.ado
    local ado_full_path `r(fn)'

    python: import sys, importlib, pathlib
    python: ado_dir = str(pathlib.Path(r"`ado_full_path'").parent)
    python: sys.path.insert(0, ado_dir) if ado_dir not in sys.path else None
    python: import dtparquet; importlib.reload(dtparquet)
    python: dtparquet.stata_to_parquet(r"`filename'", varlist=r"`varlist'", ifcond=r"`touse'")
end

program _dtparquet_use
    syntax anything(name=filename) [, clear varlist(varlist)]
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

    quietly findfile dtparquet.ado
    local ado_full_path `r(fn)'

    python: import sys, importlib, pathlib
    python: ado_dir = str(pathlib.Path(r"`ado_full_path'").parent)
    python: sys.path.insert(0, ado_dir) if ado_dir not in sys.path else None
    python: import dtparquet; importlib.reload(dtparquet)
    python: dtparquet.parquet_to_stata(r"`filename'", varlist=r"`varlist'", clear=True)
end
