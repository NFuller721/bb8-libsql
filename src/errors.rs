use std::{fmt, error};

use std::sync::mpsc::RecvError;

#[derive(Debug)]
pub enum ConnectionManagerError {
    LibsqlError(libsql::Error),
    RecvError(RecvError),
}

impl fmt::Display for ConnectionManagerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConnectionManagerError::LibsqlError(err) => write!(f, "Libsql Error: `{}`", err),
            ConnectionManagerError::RecvError(err) => write!(f, "Recv Error: `{}`", err),
        }
    }
}

impl error::Error for ConnectionManagerError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            Self::LibsqlError(err) => Some(err),
            Self::RecvError(err) => Some(err),
        }
    }
}

impl From<libsql::Error> for ConnectionManagerError {
    fn from(value: libsql::Error) -> Self {
        ConnectionManagerError::LibsqlError(value) 
    }
}

impl From<RecvError> for ConnectionManagerError {
    fn from(value: RecvError) -> Self {
        ConnectionManagerError::RecvError(value) 
    }
}
