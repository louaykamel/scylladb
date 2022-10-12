//! This module implements the cql error decoder.

use super::{
    consistency::Consistency,
    decoder::{
        self,
        ColumnDecoder,
        Decoder,
        Frame,
    },
};
use anyhow::{
    bail,
    ensure,
};
use std::{
    convert::TryFrom,
    io::Cursor,
};
use thiserror::Error;

#[derive(Error, Debug, Clone)]
#[error("{message}")]
/// The CQL error structure.
pub struct CqlError {
    /// The Error code.
    pub code: ErrorCodes,
    /// The message string.
    pub message: String,
    /// The additional Error information.
    pub additional: Option<Additional>,
}

impl CqlError {
    /// Get the CQL error from the frame decoder.
    pub fn new(decoder: &mut Decoder) -> anyhow::Result<CqlError> {
        Self::try_from(decoder)
    }
}

impl TryFrom<&mut Decoder> for CqlError {
    type Error = anyhow::Error;

    fn try_from(decoder: &mut Decoder) -> Result<Self, Self::Error> {
        ensure!(decoder.is_error());
        let body_kind = decoder.header_flags().body_kind();
        let code = unsafe { std::mem::transmute(body_kind) };
        let message = decoder::string(decoder.reader())?;
        let additional: Option<Additional>;
        match code {
            ErrorCodes::UnavailableException => {
                additional = Some(Additional::UnavailableException(UnavailableException::try_from(
                    decoder.reader(),
                )?))
            }
            ErrorCodes::WriteTimeout => {
                additional = Some(Additional::WriteTimeout(WriteTimeout::try_from(decoder.reader())?))
            }
            ErrorCodes::ReadTimeout => {
                additional = Some(Additional::ReadTimeout(ReadTimeout::try_from(decoder.reader())?))
            }
            ErrorCodes::ReadFailure => {
                additional = Some(Additional::ReadFailure(ReadFailure::try_from(decoder.reader())?))
            }
            ErrorCodes::FunctionFailure => {
                additional = Some(Additional::FunctionFailure(FunctionFailure::try_from(
                    decoder.reader(),
                )?))
            }
            ErrorCodes::WriteFailure => {
                additional = Some(Additional::WriteFailure(WriteFailure::try_from(decoder.reader())?))
            }
            ErrorCodes::AlreadyExists => {
                additional = Some(Additional::AlreadyExists(AlreadyExists::try_from(decoder.reader())?))
            }
            ErrorCodes::Unprepared => {
                additional = Some(Additional::Unprepared(Unprepared::try_from(decoder.reader())?))
            }
            _ => {
                additional = None;
            }
        }
        Ok(CqlError {
            code,
            message,
            additional,
        })
    }
}

impl CqlError {
    /// try copy unprepared_id if the error is Unprepared error
    pub fn try_unprepared_id(&self) -> Option<[u8; 16]> {
        if let Some(Additional::Unprepared(Unprepared { id })) = self.additional.as_ref() {
            Some(*id)
        } else {
            None
        }
    }
}

// ErrorCodes as consts
/// The Error code of `SERVER_ERROR`.
pub const SERVER_ERROR: i32 = 0x0000;
/// The Error code of `PROTOCOL_ERROR`.
pub const PROTOCOL_ERROR: i32 = 0x000A;
/// The Error code of `AUTHENTICATION_ERROR`.
pub const AUTHENTICATION_ERROR: i32 = 0x0100;
/// The Error code of `UNAVAILABLE_EXCEPTION`.
pub const UNAVAILABLE_EXCEPTION: i32 = 0x1000;
/// The Error code of `OVERLOADED`.
pub const OVERLOADED: i32 = 0x1001;
/// The Error code of `IS_BOOSTRAPPING`.
pub const IS_BOOSTRAPPING: i32 = 0x1002;
/// The Error code of `TRUNCATE_ERROR`.
pub const TRUNCATE_ERROR: i32 = 0x1003;
/// The Error code of `WRITE_TIMEOUT`.
pub const WRITE_TIMEOUT: i32 = 0x1100;
/// The Error code of `READ_TIMEOUT`.
pub const READ_TIMEOUT: i32 = 0x1200;
/// The Error code of `READ_FAILURE`.
pub const READ_FAILURE: i32 = 0x1300;
/// The Error code of `FUNCTION_FAILURE`.
pub const FUNCTION_FAILURE: i32 = 0x1400;
/// The Error code of `WRITE_FAILURE`.
pub const WRITE_FAILURE: i32 = 0x1500;
/// The Error code of `SYNTAX_ERROR`.
pub const SYNTAX_ERROR: i32 = 0x2000;
/// The Error code of `UNAUTHORIZED`.
pub const UNAUTHORIZED: i32 = 0x2100;
/// The Error code of `INVALID`.
pub const INVALID: i32 = 0x2200;
/// The Error code of `CONFIGURE_ERROR`.
pub const CONFIGURE_ERROR: i32 = 0x2300;
/// The Error code of `ALREADY_EXISTS`.
pub const ALREADY_EXISTS: i32 = 0x2400;
/// The Error code of `UNPREPARED`.
pub const UNPREPARED: i32 = 0x2500;

#[derive(Debug, Clone, Copy)]
#[repr(i32)]
/// The Error code enum.
pub enum ErrorCodes {
    /// The Error code is `SERVER_ERROR`.
    ServerError = 0x0000,
    /// The Error code is `PROTOCOL_ERROR`.
    ProtocolError = 0x000A,
    /// The Error code is `AUTHENTICATION_ERROR`.
    AuthenticationError = 0x0100,
    /// The Error code is `UNAVAILABLE_EXCEPTION`.
    UnavailableException = 0x1000,
    /// The Error code is `OVERLOADED`.
    Overloaded = 0x1001,
    /// The Error code is `IS_BOOSTRAPPING`.
    IsBoostrapping = 0x1002,
    /// The Error code is `TRUNCATE_ERROR`.
    TruncateError = 0x1003,
    /// The Error code is `WRITE_TIMEOUT`.
    WriteTimeout = 0x1100,
    /// The Error code is `READ_TIMEOUT`.
    ReadTimeout = 0x1200,
    /// The Error code is `READ_FAILURE`.
    ReadFailure = 0x1300,
    /// The Error code is `FUNCTION_FAILURE`.
    FunctionFailure = 0x1400,
    /// The Error code is `WRITE_FAILURE`.
    WriteFailure = 0x1500,
    /// The Error code is `SYNTAX_ERROR`.
    SyntaxError = 0x2000,
    /// The Error code is `UNAUTHORIZED`.
    Unauthorized = 0x2100,
    /// The Error code is `INVALID`.
    Invalid = 0x2200,
    /// The Error code is `CONFIGURE_ERROR`.
    ConfigureError = 0x2300,
    /// The Error code is `ALREADY_EXISTS`.
    AlreadyExists = 0x2400,
    /// The Error code is `UNPREPARED`.
    Unprepared = 0x2500,
}

#[derive(Debug, Clone)]
/// The additional error information enum.
pub enum Additional {
    /// The additional error information is `UnavailableException`.
    UnavailableException(UnavailableException),
    /// The additional error information is `WriteTimeout`.
    WriteTimeout(WriteTimeout),
    /// The additional error information is `ReadTimeout`.
    ReadTimeout(ReadTimeout),
    /// The additional error information is `ReadFailure`.
    ReadFailure(ReadFailure),
    /// The additional error information is `FunctionFailure`.
    FunctionFailure(FunctionFailure),
    /// The additional error information is `WriteFailure`.
    WriteFailure(WriteFailure),
    /// The additional error information is `AlreadyExists`.
    AlreadyExists(AlreadyExists),
    /// The additional error information is `Unprepared`.
    Unprepared(Unprepared),
}
#[derive(Debug, Clone)]
/// The unavailable exception structure.
pub struct UnavailableException {
    /// The consistency level.
    pub cl: Consistency,
    /// The number of nodes that should be alive to respect the consistency levels.
    pub required: i32,
    /// The number of replicas that were known to be alive when the request had been processed.
    pub alive: i32,
}
impl TryFrom<&mut Cursor<Vec<u8>>> for UnavailableException {
    type Error = anyhow::Error;

    fn try_from(reader: &mut Cursor<Vec<u8>>) -> Result<Self, Self::Error> {
        let cl = Consistency::try_from(u16::try_decode_column(reader)?)?;
        let required = i32::try_decode_column(reader)?;
        let alive = i32::try_decode_column(reader)?;
        Ok(Self { cl, required, alive })
    }
}
#[derive(Debug, Clone)]
/// The addtional error information, `WriteTimeout`, stucture.
pub struct WriteTimeout {
    /// The consistency level of the query having triggered the exception.
    pub cl: Consistency,
    /// Representing the number of nodes having acknowledged the request.
    pub received: i32,
    /// Representing the number of replicas whose acknowledgement is required to achieve `cl`.
    pub blockfor: i32,
    /// That describe the type of the write that timed out.
    pub writetype: WriteType,
}
impl TryFrom<&mut Cursor<Vec<u8>>> for WriteTimeout {
    type Error = anyhow::Error;

    fn try_from(reader: &mut Cursor<Vec<u8>>) -> Result<Self, Self::Error> {
        let cl = Consistency::try_from(u16::try_decode_column(reader)?)?;
        let received = i32::try_decode_column(reader)?;
        let blockfor = i32::try_decode_column(reader)?;
        let writetype = WriteType::try_from(reader)?;
        Ok(Self {
            cl,
            received,
            blockfor,
            writetype,
        })
    }
}
#[derive(Debug, Clone)]
/// The addtional error information, `ReadTimeout`, stucture.
pub struct ReadTimeout {
    /// The consistency level of the query having triggered the exception.
    pub cl: Consistency,
    /// Representing the number of nodes having answered the request.
    pub received: i32,
    /// Representing the number of replicas whose response is required to achieve `cl`.
    pub blockfor: i32,
    /// If its value is 0, it means the replica that was asked for data has not responded.
    /// Otherwise, the value is != 0.
    pub data_present: u8,
}
impl ReadTimeout {
    /// Check whether the the replica that was asked for data had not responded.
    pub fn replica_had_not_responded(&self) -> bool {
        self.data_present == 0
    }
}
impl TryFrom<&mut Cursor<Vec<u8>>> for ReadTimeout {
    type Error = anyhow::Error;

    fn try_from(reader: &mut Cursor<Vec<u8>>) -> Result<Self, Self::Error> {
        let cl = Consistency::try_from(u16::try_decode_column(reader)?)?;
        let received = i32::try_decode_column(reader)?;
        let blockfor = i32::try_decode_column(reader)?;
        let data_present = u8::try_decode_column(reader)?;
        Ok(Self {
            cl,
            received,
            blockfor,
            data_present,
        })
    }
}
#[derive(Debug, Clone)]
/// The addtional error information, `ReadFailure`, stucture.
pub struct ReadFailure {
    /// The consistency level of the query having triggered the exception.
    pub cl: Consistency,
    /// Representing the number of nodes having answered the request.
    pub received: i32,
    /// Representing the number of replicas whose acknowledgement is required to
    /// achieve <cl>.
    pub blockfor: i32,
    /// The number of nodes that experience a failure while executing the request.
    pub num_failures: i32,
    /// If its value is 0, it means the replica that was asked for data had not
    /// responded. Otherwise, the value is != 0.
    pub data_present: u8,
}
impl ReadFailure {
    /// Check whether the the replica that was asked for data had not responded.
    pub fn replica_had_not_responded(&self) -> bool {
        self.data_present == 0
    }
}
impl TryFrom<&mut Cursor<Vec<u8>>> for ReadFailure {
    type Error = anyhow::Error;

    fn try_from(reader: &mut Cursor<Vec<u8>>) -> Result<Self, Self::Error> {
        let cl = Consistency::try_from(u16::try_decode_column(reader)?)?;
        let received = i32::try_decode_column(reader)?;
        let blockfor = i32::try_decode_column(reader)?;
        let num_failures = i32::try_decode_column(reader)?;
        let data_present = u8::try_decode_column(reader)?;
        Ok(Self {
            cl,
            received,
            blockfor,
            num_failures,
            data_present,
        })
    }
}
#[derive(Debug, Clone)]
/// The addtional error information, `FunctionFailure`, stucture.
pub struct FunctionFailure {
    /// The keyspace of the failed function.
    pub keyspace: String,
    /// The name of the failed function.
    pub function: String,
    /// One string for each argument type (as CQL type) of the failed function.
    pub arg_types: Vec<String>,
}

impl TryFrom<&mut Cursor<Vec<u8>>> for FunctionFailure {
    type Error = anyhow::Error;

    fn try_from(reader: &mut Cursor<Vec<u8>>) -> Result<Self, Self::Error> {
        let keyspace = decoder::string(reader)?;
        let function = decoder::string(reader)?;
        let arg_types = decoder::string_list(reader)?;
        Ok(Self {
            keyspace,
            function,
            arg_types,
        })
    }
}
#[derive(Debug, Clone)]
/// The addtional error information, `WriteFailure`, stucture.
pub struct WriteFailure {
    /// The consistency level of the query having triggered the exception.
    pub cl: Consistency,
    /// Representing the number of nodes having answered the request.
    pub received: i32,
    /// Representing the number of replicas whose acknowledgement is required to achieve `cl`.
    pub blockfor: i32,
    /// Representing the number of nodes that experience a failure while executing the request.
    pub num_failures: i32,
    /// Describes the type of the write that timed out.
    pub writetype: WriteType,
}

impl TryFrom<&mut Cursor<Vec<u8>>> for WriteFailure {
    type Error = anyhow::Error;

    fn try_from(reader: &mut Cursor<Vec<u8>>) -> Result<Self, Self::Error> {
        let cl = Consistency::try_from(u16::try_decode_column(reader)?)?;
        let received = i32::try_decode_column(reader)?;
        let blockfor = i32::try_decode_column(reader)?;
        let num_failures = i32::try_decode_column(reader)?;
        let writetype = WriteType::try_from(reader)?;
        Ok(Self {
            cl,
            received,
            blockfor,
            num_failures,
            writetype,
        })
    }
}
#[derive(Debug, Clone)]
/// The addtional error information, `AlreadyExists`, stucture.
pub struct AlreadyExists {
    /// Representing either the keyspace that already exists, or the keyspace in which the table that
    /// already exists is.
    pub ks: String,
    /// Representing the name of the table that already exists. If the query was attempting to create a
    /// keyspace, <table> will be present but will be the empty string.
    pub table: String,
}

impl TryFrom<&mut Cursor<Vec<u8>>> for AlreadyExists {
    type Error = anyhow::Error;

    fn try_from(reader: &mut Cursor<Vec<u8>>) -> Result<Self, Self::Error> {
        let ks = decoder::string(reader)?;
        let table = decoder::string(reader)?;
        Ok(Self { ks, table })
    }
}
#[derive(Debug, Clone)]
/// The addtional error information, `Unprepared`, stucture.
pub struct Unprepared {
    /// The unprepared id.
    pub id: [u8; 16],
}

impl TryFrom<&mut Cursor<Vec<u8>>> for Unprepared {
    type Error = anyhow::Error;

    fn try_from(reader: &mut Cursor<Vec<u8>>) -> Result<Self, Self::Error> {
        Ok(Self {
            id: decoder::prepared_id(reader)?,
        })
    }
}
#[derive(Debug, Clone)]
/// The type of the write that timed out.
pub enum WriteType {
    /// Simple write type.
    Simple,
    /// Batch write type.
    Batch,
    /// UnloggedBatch write type.
    UnloggedBatch,
    /// Counter write type.
    Counter,
    /// BatchLog write type.
    BatchLog,
    /// Cas write type.
    Cas,
    /// View write type.
    View,
    /// Cdc write type.
    Cdc,
}

impl TryFrom<&mut Cursor<Vec<u8>>> for WriteType {
    type Error = anyhow::Error;

    fn try_from(reader: &mut Cursor<Vec<u8>>) -> Result<Self, Self::Error> {
        Ok(match &decoder::string(reader)?[..] {
            "SIMPLE" => WriteType::Simple,
            "BATCH" => WriteType::Batch,
            "UNLOGGED_BATCH" => WriteType::UnloggedBatch,
            "COUNTER" => WriteType::Counter,
            "BATCH_LOG" => WriteType::BatchLog,
            "CAS" => WriteType::Cas,
            "VIEW" => WriteType::View,
            "CDC" => WriteType::Cdc,
            _ => bail!("unexpected writetype error"),
        })
    }
}

impl TryFrom<&mut Cursor<Vec<u8>>> for ErrorCodes {
    type Error = anyhow::Error;

    fn try_from(reader: &mut Cursor<Vec<u8>>) -> Result<Self, Self::Error> {
        let code = i32::try_decode_column(reader)?;
        unsafe { Ok(std::mem::transmute(code)) }
    }
}
