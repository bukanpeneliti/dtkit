pub mod read {
    use crate::mapping::{FieldSpec, TransferWriterKind};
    use crate::transfer::TransferColumnSpec;
    use std::collections::HashSet;
    use std::path::Path;

    #[derive(Debug)]
    pub struct ReadScanPlan {
        pub selected_column_list: Vec<String>,
        pub transfer_columns: Vec<TransferColumnSpec>,
        pub can_use_eager: bool,
        pub schema_handoff_mode: &'static str,
    }

    pub struct ReadBoundaryInputs {
        pub variables_as_str: String,
        pub all_columns_unfiltered: Vec<FieldSpec>,
        pub schema_handoff_mode: &'static str,
        pub cast_json: String,
    }

    pub use crate::read::build_read_scan_plan;
    pub use crate::read::resolve_read_boundary_inputs;
}

pub mod write {
    use crate::mapping::FieldSpec;

    pub struct WriteScanPlan {
        pub selected_infos: Vec<crate::write::ExportField>,
        pub start_row: usize,
        pub rows_to_read: usize,
        pub row_width_bytes: usize,
        pub partition_cols: Vec<polars::datatypes::PlSmallStr>,
        pub dtmeta_json: String,
        pub schema_handoff_mode: &'static str,
    }

    pub struct WriteBoundaryInputs {
        pub selected_vars: String,
        pub all_columns: Vec<crate::write::ExportField>,
        pub schema_handoff_mode: &'static str,
    }

    pub use crate::write::build_write_scan_plan;
    pub use crate::write::resolve_write_boundary_inputs;
}

pub use read::ReadBoundaryInputs;
pub use read::ReadScanPlan;
pub use write::WriteBoundaryInputs;
pub use write::WriteScanPlan;
