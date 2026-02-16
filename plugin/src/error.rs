use crate::stata_interface::ST_retcode;

#[derive(Debug, Clone)]
pub enum DtparquetError {
    MissingArg(&'static str),
    InvalidArg(&'static str, String),
    FileNotFound(String),
    SubcommandUnknown(String),
    SubcommandArgCount(&'static str, usize),
    IoError(String),
    Custom(String),
    Polars(String),
    Stata(String),
}

impl DtparquetError {
    pub fn to_retcode(&self) -> ST_retcode {
        match self {
            DtparquetError::MissingArg(_) => 198,
            DtparquetError::InvalidArg(_, _) => 198,
            DtparquetError::SubcommandArgCount(_, _) => 198,
            DtparquetError::SubcommandUnknown(_) => 198,
            DtparquetError::FileNotFound(_) => 601,
            DtparquetError::IoError(_) => 198,
            DtparquetError::Custom(_) => 198,
            DtparquetError::Polars(_) => 198,
            DtparquetError::Stata(_) => 198,
        }
    }

    pub fn display_msg(&self) -> String {
        match self {
            DtparquetError::MissingArg(msg) => format!("Error: {}", msg),
            DtparquetError::InvalidArg(field, val) => {
                format!("Error: invalid {} '{}'", field, val)
            }
            DtparquetError::SubcommandArgCount(cmd, count) => {
                format!("Error: {} requires {} arguments", cmd, count)
            }
            DtparquetError::SubcommandUnknown(name) => {
                format!("Error: Unknown subfunction '{}'", name)
            }
            DtparquetError::FileNotFound(path) => {
                format!("File does not exist ({})", path)
            }
            DtparquetError::IoError(msg) => format!("Error: {}", msg),
            DtparquetError::Custom(msg) => format!("Error: {}", msg),
            DtparquetError::Polars(msg) => format!("Polars error: {}", msg),
            DtparquetError::Stata(msg) => format!("Stata error: {}", msg),
        }
    }
}

impl From<std::io::Error> for DtparquetError {
    fn from(err: std::io::Error) -> Self {
        DtparquetError::IoError(err.to_string())
    }
}

impl From<polars::error::PolarsError> for DtparquetError {
    fn from(err: polars::error::PolarsError) -> Self {
        DtparquetError::Polars(err.to_string())
    }
}

impl From<String> for DtparquetError {
    fn from(s: String) -> Self {
        DtparquetError::Custom(s)
    }
}
