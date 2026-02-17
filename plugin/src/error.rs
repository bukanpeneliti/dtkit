use crate::stata_interface::ST_retcode;

#[derive(Debug, Clone, thiserror::Error)]
pub enum DtparquetError {
    #[error("Error: {0}")]
    MissingArg(&'static str),
    #[error("Error: invalid {0} '{1}'")]
    InvalidArg(&'static str, String),
    #[error("File does not exist ({0})")]
    FileNotFound(String),
    #[error("Error: Unknown subfunction '{0}'")]
    SubcommandUnknown(String),
    #[error("Error: {0} requires {1} arguments")]
    SubcommandArgCount(&'static str, usize),
    #[error("Error: {0}")]
    IoError(String),
    #[error("Error: {0}")]
    Custom(String),
    #[error("Polars error: {0}")]
    Polars(String),
    #[error("Stata error: {0}")]
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
        self.to_string()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retcode_contract_preserved() {
        assert_eq!(DtparquetError::MissingArg("x").to_retcode(), 198);
        assert_eq!(
            DtparquetError::InvalidArg("field", "value".to_string()).to_retcode(),
            198
        );
        assert_eq!(
            DtparquetError::SubcommandArgCount("read", 16).to_retcode(),
            198
        );
        assert_eq!(
            DtparquetError::SubcommandUnknown("cmd".to_string()).to_retcode(),
            198
        );
        assert_eq!(
            DtparquetError::FileNotFound("missing.parquet".to_string()).to_retcode(),
            601
        );
        assert_eq!(DtparquetError::IoError("io".to_string()).to_retcode(), 198);
        assert_eq!(
            DtparquetError::Custom("custom".to_string()).to_retcode(),
            198
        );
        assert_eq!(
            DtparquetError::Polars("polars".to_string()).to_retcode(),
            198
        );
        assert_eq!(DtparquetError::Stata("stata".to_string()).to_retcode(), 198);
    }

    #[test]
    fn display_message_contract_preserved() {
        assert_eq!(
            DtparquetError::MissingArg("missing").display_msg(),
            "Error: missing"
        );
        assert_eq!(
            DtparquetError::InvalidArg("field", "value".to_string()).display_msg(),
            "Error: invalid field 'value'"
        );
        assert_eq!(
            DtparquetError::SubcommandArgCount("save", 12).display_msg(),
            "Error: save requires 12 arguments"
        );
        assert_eq!(
            DtparquetError::SubcommandUnknown("abc".to_string()).display_msg(),
            "Error: Unknown subfunction 'abc'"
        );
        assert_eq!(
            DtparquetError::FileNotFound("x.parquet".to_string()).display_msg(),
            "File does not exist (x.parquet)"
        );
        assert_eq!(
            DtparquetError::IoError("io".to_string()).display_msg(),
            "Error: io"
        );
        assert_eq!(
            DtparquetError::Custom("custom".to_string()).display_msg(),
            "Error: custom"
        );
        assert_eq!(
            DtparquetError::Polars("p".to_string()).display_msg(),
            "Polars error: p"
        );
        assert_eq!(
            DtparquetError::Stata("s".to_string()).display_msg(),
            "Stata error: s"
        );
    }

    #[test]
    fn from_conversions_preserved() {
        let io = std::io::Error::other("io");
        assert!(matches!(
            DtparquetError::from(io),
            DtparquetError::IoError(_)
        ));

        let polars = polars::error::PolarsError::ComputeError("p".into());
        assert!(matches!(
            DtparquetError::from(polars),
            DtparquetError::Polars(_)
        ));

        assert!(matches!(
            DtparquetError::from("x".to_string()),
            DtparquetError::Custom(_)
        ));
    }
}
