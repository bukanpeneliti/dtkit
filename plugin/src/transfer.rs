pub mod reader {
    use crate::mapping::TransferWriterKind;

    #[derive(Clone, Debug)]
    pub struct TransferColumnSpec {
        pub name: String,
        pub stata_type: u32,
        pub writer_kind: TransferWriterKind,
        pub strls: Vec<String>,
        pub max_string_len: usize,
    }

    pub use crate::read::build_transfer_columns;
    pub use crate::read::estimate_transfer_row_width_bytes;
    pub use crate::read::sink_dataframe_in_batches;
    pub use crate::read::write_numeric_column_range;
    pub use crate::read::write_string_column_range;
}

pub mod writer {
    #[derive(Clone, Debug)]
    pub struct ExportField {
        pub name: String,
        pub stata_type: u32,
        pub width: usize,
    }

    pub use crate::write::read_batch_from_columns;
    pub use crate::write::series_from_stata_column;
    pub use crate::write::validate_stata_schema;
}

pub use reader::TransferColumnSpec;
pub use writer::ExportField;
