use crate::utilities::BatchMode;

#[derive(Debug, Clone)]
pub struct ReadArgs {
    pub file_path: String,
    pub varlist: String,
    pub start_row: usize,
    pub max_rows: usize,
    pub sql_if: Option<String>,
    pub sort_by: String,
    pub parallel_strategy: Option<BatchMode>,
    pub safe_relaxed: bool,
    pub asterisk_to_variable_name: Option<String>,
    pub order_by: String,
    pub order_by_type: usize,
    pub order_descending: f64,
    pub stata_offset: usize,
    pub random_share: f64,
    pub random_seed: u64,
    pub batch_size: usize,
}

#[derive(Debug, Clone)]
pub struct SaveArgs {
    pub file_path: String,
    pub varlist: String,
    pub start_row: usize,
    pub max_rows: usize,
    pub sql_if: Option<String>,
    pub sort_by: String,
    pub compression_codec: String,
    pub compression_level: Option<usize>,
    pub include_labels: bool,
    pub include_notes: bool,
    pub overwrite: bool,
    pub batch_size: usize,
}

#[derive(Debug, Clone)]
pub struct DescribeArgs {
    pub file_path: String,
    pub detailed: bool,
    pub memory_savvy: bool,
    pub sorting: Option<String>,
    pub compress: bool,
    pub asterisk_to_variable_name: Option<String>,
    pub compress_string_to_numeric: bool,
}

#[derive(Debug, Clone)]
pub struct HasMetadataKeyArgs {
    pub file_path: String,
    pub key: String,
}

#[derive(Debug, Clone)]
pub struct LoadMetaArgs {
    pub file_path: String,
}

#[derive(Debug, Clone)]
pub enum CommandArgs {
    SetupCheck,
    Read(ReadArgs),
    Save(SaveArgs),
    Describe(DescribeArgs),
    HasMetadataKey(HasMetadataKeyArgs),
    LoadMeta(LoadMetaArgs),
}
