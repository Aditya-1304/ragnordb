//! This file contains error types for the entire codebase of this database
//! Just have 2 errors for now, will add more as the codebase grows

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("not implemented: {0}")]
    NotImplemented(&'static str), // this is use for stub or Todo

    #[error("invalid argument: {0}")]
    InvalidArgument(String), // parse failures, type mismatch yada yada any user facing error
}

pub type Result<T> = std::result::Result<T, Error>;
