// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Defines a [SparkError] for representing failures in various Spark operations.
//! Most of these are wrappers for tonic or arrow error messages
use std::error::Error;
use std::fmt::Debug;
use std::io::Write;

use arrow::error::ArrowError;
use thiserror::Error;

use tonic::Code;

#[cfg(feature = "datafusion")]
use datafusion::error::DataFusionError;
#[cfg(feature = "polars")]
use polars::error::PolarsError;

/// Different `Spark` Error types
#[derive(Error, Debug)]
pub enum SparkError {
    #[error("Aborted: {0}")]
    Aborted(String),

    #[error("Already Exists: {0}")]
    AlreadyExists(String),

    #[error("Analysis Exception: {0}")]
    AnalysisException(String),

    #[error("Apache Arrow Error: {0}")]
    ArrowError(#[from] ArrowError),

    #[error("Cancelled: {0}")]
    Cancelled(String),

    #[error("Data Loss Exception: {0}")]
    DataLoss(String),

    #[error("Deadline Exceeded: {0}")]
    DeadlineExceeded(String),

    #[error("External Error: {0}")]
    ExternalError(Box<dyn Error + Send + Sync>),

    #[error("Failed Precondition: {0}")]
    FailedPrecondition(String),

    #[error("Invalid Connection Url: {0}")]
    InvalidConnectionUrl(String),

    #[error("Invalid Argument: {0}")]
    InvalidArgument(String),

    #[error("Io Error: {0}")]
    IoError(String, std::io::Error),

    #[error("Not Found: {0}")]
    NotFound(String),

    #[error("Not Yet Implemented: {0}")]
    NotYetImplemented(String),

    #[error("Permission Denied: {0}")]
    PermissionDenied(String),

    #[error("Resource Exhausted: {0}")]
    ResourceExhausted(String),

    #[error("Spark Session ID is not the same: {0}")]
    SessionNotSameException(String),

    #[error("Unauthenticated: {0}")]
    Unauthenticated(String),

    #[error("Unavailable: {0}")]
    Unavailable(String),

    #[error("Unkown: {0}")]
    Unknown(String),

    #[error("Unimplemented; {0}")]
    Unimplemented(String),

    #[error("Invalid UUID")]
    Uuid(#[from] uuid::Error),

    #[error("Out of Range: {0}")]
    OutOfRange(String),
}

impl SparkError {
    /// Wraps an external error in an `SparkError`.
    pub fn from_external_error(error: Box<dyn Error + Send + Sync>) -> Self {
        Self::ExternalError(error)
    }
}

impl From<std::io::Error> for SparkError {
    fn from(error: std::io::Error) -> Self {
        SparkError::IoError(error.to_string(), error)
    }
}

impl From<std::str::Utf8Error> for SparkError {
    fn from(error: std::str::Utf8Error) -> Self {
        SparkError::AnalysisException(error.to_string())
    }
}

impl From<std::string::FromUtf8Error> for SparkError {
    fn from(error: std::string::FromUtf8Error) -> Self {
        SparkError::AnalysisException(error.to_string())
    }
}

impl From<tonic::Status> for SparkError {
    fn from(status: tonic::Status) -> Self {
        // Include the gRPC status code in the error message for better debugging
        let details = if status.metadata().is_empty() {
            status.message().to_string()
        } else {
            // Try to extract any additional error details from metadata
            let mut msg = status.message().to_string();

            // Spark Connect may include error details as binary metadata
            for kv in status.metadata().iter() {
                match kv {
                    tonic::metadata::KeyAndValueRef::Ascii(key, value) => {
                        if let Ok(v) = value.to_str() {
                            msg.push_str(&format!("\n[{}]: {}", key.as_str(), v));
                        }
                    }
                    tonic::metadata::KeyAndValueRef::Binary(key, value) => {
                        if let Ok(v) = std::str::from_utf8(value.as_ref()) {
                            msg.push_str(&format!("\n[{}]: {}", key.as_str(), v));
                        }
                    }
                }
            }

            msg
        };

        // If the tonic Status carries a source error, append it for context
        if let Some(src) = status.source() {
            let details = format!("{}\ncaused by: {}", details, src);
            return match status.code() {
                Code::Ok => SparkError::AnalysisException(details),
                Code::Unknown => SparkError::Unknown(details),
                Code::Aborted => SparkError::Aborted(details),
                Code::NotFound => SparkError::NotFound(details),
                Code::Internal => SparkError::AnalysisException(details),
                Code::DataLoss => SparkError::DataLoss(details),
                Code::Cancelled => SparkError::Cancelled(details),
                Code::OutOfRange => SparkError::OutOfRange(details),
                Code::Unavailable => SparkError::Unavailable(details),
                Code::AlreadyExists => SparkError::AnalysisException(details),
                Code::InvalidArgument => SparkError::InvalidArgument(details),
                Code::DeadlineExceeded => SparkError::DeadlineExceeded(details),
                Code::Unimplemented => SparkError::Unimplemented(details),
                Code::Unauthenticated => SparkError::Unauthenticated(details),
                Code::PermissionDenied => SparkError::PermissionDenied(details),
                Code::ResourceExhausted => SparkError::ResourceExhausted(details),
                Code::FailedPrecondition => SparkError::FailedPrecondition(details),
            };
        }

        match status.code() {
            Code::Ok => SparkError::AnalysisException(details),
            Code::Unknown => SparkError::Unknown(details),
            Code::Aborted => SparkError::Aborted(details),
            Code::NotFound => SparkError::NotFound(details),
            Code::Internal => SparkError::AnalysisException(details),
            Code::DataLoss => SparkError::DataLoss(details),
            Code::Cancelled => SparkError::Cancelled(details),
            Code::OutOfRange => SparkError::OutOfRange(details),
            Code::Unavailable => SparkError::Unavailable(details),
            Code::AlreadyExists => SparkError::AnalysisException(details),
            Code::InvalidArgument => SparkError::InvalidArgument(details),
            Code::DeadlineExceeded => SparkError::DeadlineExceeded(details),
            Code::Unimplemented => SparkError::Unimplemented(details),
            Code::Unauthenticated => SparkError::Unauthenticated(details),
            Code::PermissionDenied => SparkError::PermissionDenied(details),
            Code::ResourceExhausted => SparkError::ResourceExhausted(details),
            Code::FailedPrecondition => SparkError::FailedPrecondition(details),
        }
    }
}

impl From<serde_json::Error> for SparkError {
    fn from(value: serde_json::Error) -> Self {
        SparkError::AnalysisException(value.to_string())
    }
}

#[cfg(feature = "datafusion")]
impl From<DataFusionError> for SparkError {
    fn from(_value: DataFusionError) -> Self {
        SparkError::AnalysisException("Error converting to DataFusion DataFrame".to_string())
    }
}

#[cfg(feature = "polars")]
impl From<PolarsError> for SparkError {
    fn from(_value: PolarsError) -> Self {
        SparkError::AnalysisException("Error converting to Polars DataFrame".to_string())
    }
}

impl From<tonic::codegen::http::uri::InvalidUri> for SparkError {
    fn from(value: tonic::codegen::http::uri::InvalidUri) -> Self {
        SparkError::InvalidConnectionUrl(value.to_string())
    }
}

impl From<tonic::transport::Error> for SparkError {
    fn from(value: tonic::transport::Error) -> Self {
        SparkError::InvalidConnectionUrl(value.to_string())
    }
}

impl<W: Write> From<std::io::IntoInnerError<W>> for SparkError {
    fn from(error: std::io::IntoInnerError<W>) -> Self {
        SparkError::IoError(error.to_string(), error.into())
    }
}
