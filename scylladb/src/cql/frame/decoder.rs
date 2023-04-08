//! This module implements the frame decoder.

use super::{
    error,
    header,
    opcode,
    result,
    rows::{
        Flags,
        Metadata,
        PagingState,
    },
};
use crate::{
    cql::{
        compression::{
            Compression,
            MyCompression,
        },
        rows::AnyIter,
        ColType,
        ColumnSpec,
        TableSpec,
    },
    prelude::Row,
};
use anyhow::{
    anyhow,
    ensure,
};
use chrono::{
    NaiveDate,
    NaiveDateTime,
    NaiveTime,
};
use std::{
    collections::{
        BTreeMap,
        BTreeSet,
        BinaryHeap,
        HashMap,
        HashSet,
        LinkedList,
        VecDeque,
    },
    convert::{
        TryFrom,
        TryInto,
    },
    hash::Hash,
    io::{
        Cursor,
        Read,
    },
    net::{
        IpAddr,
        Ipv4Addr,
        Ipv6Addr,
    },
};
/// RowsDecoder trait to decode the rows result from scylla
pub trait RowsDecoder: Sized {
    /// The Row to decode. Must implement [`super::Row`].
    type Row: Row;
    /// Try to decode the provided Decoder with an expected Rows result
    fn try_decode_rows(decoder: Decoder) -> anyhow::Result<Option<Self>>;
    /// Decode the provided Decoder with deterministic Rows result
    fn decode_rows(decoder: Decoder) -> Option<Self> {
        Self::try_decode_rows(decoder).unwrap()
    }
}

impl<T> RowsDecoder for T
where
    T: Row,
{
    type Row = T;

    fn try_decode_rows(decoder: Decoder) -> anyhow::Result<Option<Self>> {
        Ok(Self::Row::rows_iter(decoder)?.next())
    }
}

impl<T> RowsDecoder for crate::prelude::Iter<T>
where
    T: Row,
{
    type Row = T;

    fn try_decode_rows(decoder: Decoder) -> anyhow::Result<Option<Self>> {
        ensure!(decoder.is_rows(), "Decoded response is not rows!");
        let rows_iter = Self::Row::rows_iter(decoder)?;
        if rows_iter.is_empty() && !rows_iter.has_more_pages() {
            Ok(None)
        } else {
            Ok(Some(rows_iter))
        }
    }
}

/// LwtDecoder trait to decode the LWT result from scylla
pub struct LwtDecoder;
impl LwtDecoder {
    /// Try to decode the provided Decoder with an expected Void result
    pub fn try_decode_lwt(mut decoder: Decoder) -> anyhow::Result<AnyIter> {
        if decoder.is_error() {
            Err(anyhow!(decoder.get_error()?))
        } else if decoder.is_rows() {
            Ok(AnyIter::new(decoder)?)
        } else {
            let body_kind = decoder.header_flags.body_kind();
            Err(anyhow!(
                "Decoder opcode is {}, and body kind: {}",
                decoder.opcode(),
                body_kind,
            ))
        }
    }
    /// Decode the provided Decoder with an deterministic Lwt result
    pub fn decode_lwt(decoder: Decoder) -> AnyIter {
        Self::try_decode_lwt(decoder).unwrap()
    }
}

/// VoidDecoder trait to decode the VOID result from scylla
pub struct VoidDecoder;

impl VoidDecoder {
    /// Try to decode the provided Decoder with an expected Void result
    pub fn try_decode_void(mut decoder: Decoder) -> anyhow::Result<()> {
        if decoder.is_error() {
            Err(anyhow!(decoder.get_error()?))
        } else {
            Ok(())
        }
    }
    /// Decode the provided Decoder with an deterministic Void result
    pub fn decode_void(decoder: Decoder) {
        Self::try_decode_void(decoder).unwrap()
    }
}

impl TryFrom<Vec<u8>> for Decoder {
    type Error = anyhow::Error;

    fn try_from(buffer: Vec<u8>) -> Result<Self, Self::Error> {
        Decoder::new(buffer, MyCompression::get())
    }
}

/// The CQL frame trait.
pub trait Frame {
    /// Get the frame version.
    fn version(&self) -> u8;
    /// G the frame header flags.
    fn flags(&self) -> &HeaderFlags;
    /// Get the stream in the frame header.
    fn stream(&self) -> i16;
    /// Get the opcode in the frame header.
    fn opcode(&self) -> u8;
    /// Get the length of the frame body.
    fn length(&self) -> usize;
    /// Check whether the opcode is `AUTHENTICATE`.
    fn is_authenticate(&self) -> bool;
    /// Check whether the opcode is `AUTH_CHALLENGE`.
    fn is_auth_challenge(&self) -> bool;
    /// Check whether the opcode is `AUTH_SUCCESS`.
    fn is_auth_success(&self) -> bool;
    /// Check whether the opcode is `SUPPORTED`.
    fn is_supported(&self) -> bool;
    /// Check whether the opcode is `READY`.
    fn is_ready(&self) -> bool;
    /// Check whether the body kind is `VOID`.
    fn is_void(&self) -> bool;
    /// Check whether the body kind is `ROWS`.
    fn is_rows(&self) -> bool;
    /// Check whether the opcode is `ERROR`.
    fn is_error(&self) -> bool;
    /// Get the `CqlError`.
    fn get_error(&mut self) -> anyhow::Result<error::CqlError>;
    /// Get Void `()`
    fn get_void(&self) -> anyhow::Result<()>;
    /// Check whether the error is `UNPREPARED`.
    fn is_unprepared(&self) -> bool;
    /// Check whether the error is `ALREADY_EXISTS.
    fn is_already_exists(&self) -> bool;
    /// Check whether the error is `CONFIGURE_ERROR.
    fn is_configure_error(&self) -> bool;
    /// Check whether the error is `INVALID.
    fn is_invalid(&self) -> bool;
    /// Check whether the error is `UNAUTHORIZED.
    fn is_unauthorized(&self) -> bool;
    /// Check whether the error is `SYNTAX_ERROR.
    fn is_syntax_error(&self) -> bool;
    /// Check whether the error is `WRITE_FAILURE.
    fn is_write_failure(&self) -> bool;
    /// Check whether the error is `FUNCTION_FAILURE.
    fn is_function_failure(&self) -> bool;
    /// Check whether the error is `READ_FAILURE.
    fn is_read_failure(&self) -> bool;
    /// Check whether the error is `READ_TIMEOUT.
    fn is_read_timeout(&self) -> bool;
    /// Check whether the error is `WRITE_TIMEOUT.
    fn is_write_timeout(&self) -> bool;
    /// Check whether the error is `TRUNCATE_ERROR.
    fn is_truncate_error(&self) -> bool;
    /// Check whether the error is `IS_BOOSTRAPPING.
    fn is_boostrapping(&self) -> bool;
    /// Check whether the error is `OVERLOADED.
    fn is_overloaded(&self) -> bool;
    /// Check whether the error is `UNAVAILABLE_EXCEPTION.
    fn is_unavailable_exception(&self) -> bool;
    /// Check whether the error is `AUTHENTICATION_ERROR.
    fn is_authentication_error(&self) -> bool;
    /// Check whether the error is `PROTOCOL_ERROR.
    fn is_protocol_error(&self) -> bool;
    /// Check whether the error is `SERVER_ERROR.
    fn is_server_error(&self) -> bool;
    /// The the metadata.
    fn metadata(&mut self) -> anyhow::Result<Metadata>;
}
/// The frame decoder structure.
#[derive(Debug, Clone)]
pub struct Decoder {
    reader: Cursor<Vec<u8>>,
    header: Header,
    header_flags: HeaderFlags,
}

impl Decoder {
    /// Create a new decoder with an assigned compression type.
    pub fn new(mut buffer: Vec<u8>, decompressor: impl Compression) -> anyhow::Result<Self> {
        buffer = decompressor.decompress(buffer)?;
        ensure!(buffer.len() >= 9, "Buffer is too small!");
        let mut reader = Cursor::new(buffer);
        let mut header_buf = [0u8; 9];
        reader.read_exact(&mut header_buf)?;
        let mut header = Header::new(header_buf);
        let header_flags = header.header_flags(&mut reader)?;
        Ok(Decoder {
            header,
            reader,
            header_flags,
        })
    }
    /// Get the decoder buffer referennce.
    pub fn reader(&mut self) -> &mut Cursor<Vec<u8>> {
        &mut self.reader
    }
    /// Get the decoder buffer.
    pub fn into_buffer(self) -> Vec<u8> {
        self.reader.into_inner()
    }
    /// Get the header flags
    pub fn header_flags(&self) -> &HeaderFlags {
        &self.header_flags
    }
}

#[derive(Debug, Clone)]
/// Cql frame header
pub struct Header {
    buffer: [u8; 9],
}

impl Header {
    /// Creates cql Header
    pub fn new(buffer: [u8; 9]) -> Self {
        Self { buffer }
    }
    /// Returns the header version
    pub fn version(&self) -> u8 {
        self.buffer[0]
    }
    /// Returns the header flags
    pub fn flags(&self) -> u8 {
        self.buffer[1]
    }
    /// Returns the header opcode
    pub fn opcode(&self) -> u8 {
        self.buffer[4]
    }
    /// Returns the body length
    pub fn length(&self) -> usize {
        i32::from_be_bytes(self.buffer[5..9].try_into().unwrap()) as usize
    }
    /// Checks if the header frame is error
    pub fn is_error(&self) -> bool {
        self.opcode() == opcode::ERROR
    }
    /// Checks if the header frame is result
    pub fn is_result(&self) -> bool {
        self.opcode() == opcode::RESULT
    }
    /// Returns the computed header flags
    pub fn header_flags(&mut self, reader: &mut Cursor<Vec<u8>>) -> anyhow::Result<HeaderFlags> {
        let flags = self.flags();
        let compression = flags & header::COMPRESSION == header::COMPRESSION;
        let tracing;
        if flags & header::TRACING == header::TRACING {
            let mut tracing_id = [0; 16];
            reader.read_exact(&mut tracing_id)?;
            tracing = Some(tracing_id);
        } else {
            tracing = None;
        }
        let warnings = if flags & header::WARNING == header::WARNING {
            let string_list = string_list(reader)?;
            Some(string_list)
        } else {
            None
        };
        let custom_payload = flags & header::CUSTOM_PAYLOAD == header::CUSTOM_PAYLOAD;
        let mut body_kind = 0;
        if self.is_error() || self.is_result() {
            let mut buf = [0u8; 4];
            reader.read_exact(&mut buf)?;
            body_kind = i32::from_be_bytes(buf);
        }
        Ok(HeaderFlags {
            compression,
            tracing,
            warnings,
            custom_payload,
            body_kind,
        })
    }
}
#[allow(dead_code)]
#[derive(Debug, Clone)]
/// The header flags structure in the CQL frame.
pub struct HeaderFlags {
    compression: bool,
    tracing: Option<[u8; 16]>,
    custom_payload: bool,
    warnings: Option<Vec<String>>,
    // Body kind (if available)
    body_kind: i32,
}

#[allow(dead_code)]
impl HeaderFlags {
    /// Get whether the frame is compressed.
    pub fn compression(&self) -> bool {
        self.compression
    }
    /// Take the tracing id of the frame.
    pub fn take_tracing_id(&mut self) -> Option<[u8; 16]> {
        self.tracing.take()
    }
    /// Take the warnings of the frame.
    fn take_warnings(&mut self) -> Option<Vec<String>> {
        self.warnings.take()
    }
    /// Returns the body kind (if available), else it's zero
    pub fn body_kind(&self) -> i32 {
        self.body_kind
    }
}

impl Frame for Decoder {
    fn version(&self) -> u8 {
        self.header.version()
    }
    fn flags(&self) -> &HeaderFlags {
        &self.header_flags
    }
    fn stream(&self) -> i16 {
        ((self.header.buffer[2] as i16) << 8) | self.header.buffer[3] as i16
    }
    fn opcode(&self) -> u8 {
        self.header.opcode()
    }
    fn length(&self) -> usize {
        self.header.length()
    }
    fn is_authenticate(&self) -> bool {
        self.opcode() == opcode::AUTHENTICATE
    }
    fn is_auth_challenge(&self) -> bool {
        self.opcode() == opcode::AUTH_CHALLENGE
    }
    fn is_auth_success(&self) -> bool {
        self.opcode() == opcode::AUTH_SUCCESS
    }
    fn is_supported(&self) -> bool {
        self.opcode() == opcode::SUPPORTED
    }
    fn is_ready(&self) -> bool {
        self.opcode() == opcode::READY
    }
    fn is_void(&self) -> bool {
        (self.opcode() == opcode::RESULT) && (self.header_flags.body_kind() == result::VOID)
    }
    fn is_rows(&self) -> bool {
        (self.opcode() == opcode::RESULT) && (self.header_flags.body_kind() == result::ROWS)
    }
    fn is_error(&self) -> bool {
        self.header.is_error()
    }
    fn get_error(&mut self) -> anyhow::Result<error::CqlError> {
        if self.is_error() {
            error::CqlError::new(self)
        } else {
            Err(anyhow!("Not error"))
        }
    }
    fn get_void(&self) -> anyhow::Result<()> {
        if self.is_void() {
            Ok(())
        } else {
            Err(anyhow!("Not void"))
        }
    }
    fn is_unprepared(&self) -> bool {
        self.opcode() == opcode::ERROR && self.header_flags.body_kind() == error::UNPREPARED
    }
    fn is_already_exists(&self) -> bool {
        self.opcode() == opcode::ERROR && self.header_flags.body_kind() == error::ALREADY_EXISTS
    }
    fn is_configure_error(&self) -> bool {
        self.opcode() == opcode::ERROR && self.header_flags.body_kind() == error::CONFIGURE_ERROR
    }
    fn is_invalid(&self) -> bool {
        self.opcode() == opcode::ERROR && self.header_flags.body_kind() == error::INVALID
    }
    fn is_unauthorized(&self) -> bool {
        self.opcode() == opcode::ERROR && self.header_flags.body_kind() == error::UNAUTHORIZED
    }
    fn is_syntax_error(&self) -> bool {
        self.opcode() == opcode::ERROR && self.header_flags.body_kind() == error::SYNTAX_ERROR
    }
    fn is_write_failure(&self) -> bool {
        self.opcode() == opcode::ERROR && self.header_flags.body_kind() == error::WRITE_FAILURE
    }
    fn is_function_failure(&self) -> bool {
        self.opcode() == opcode::ERROR && self.header_flags.body_kind() == error::FUNCTION_FAILURE
    }
    fn is_read_failure(&self) -> bool {
        self.opcode() == opcode::ERROR && self.header_flags.body_kind() == error::READ_FAILURE
    }
    fn is_read_timeout(&self) -> bool {
        self.opcode() == opcode::ERROR && self.header_flags.body_kind() == error::READ_TIMEOUT
    }
    fn is_write_timeout(&self) -> bool {
        self.opcode() == opcode::ERROR && self.header_flags.body_kind() == error::WRITE_TIMEOUT
    }
    fn is_truncate_error(&self) -> bool {
        self.opcode() == opcode::ERROR && self.header_flags.body_kind() == error::TRUNCATE_ERROR
    }
    fn is_boostrapping(&self) -> bool {
        self.opcode() == opcode::ERROR && self.header_flags.body_kind() == error::IS_BOOSTRAPPING
    }
    fn is_overloaded(&self) -> bool {
        self.opcode() == opcode::ERROR && self.header_flags.body_kind() == error::OVERLOADED
    }
    fn is_unavailable_exception(&self) -> bool {
        self.opcode() == opcode::ERROR && self.header_flags.body_kind() == error::UNAVAILABLE_EXCEPTION
    }
    fn is_authentication_error(&self) -> bool {
        self.opcode() == opcode::ERROR && self.header_flags.body_kind() == error::AUTHENTICATION_ERROR
    }
    fn is_protocol_error(&self) -> bool {
        self.opcode() == opcode::ERROR && self.header_flags.body_kind() == error::PROTOCOL_ERROR
    }
    fn is_server_error(&self) -> bool {
        self.opcode() == opcode::ERROR && self.header_flags.body_kind() == error::SERVER_ERROR
    }
    fn metadata(&mut self) -> anyhow::Result<Metadata> {
        ensure!(self.is_rows());
        let flags = Flags::from_i32(i32::try_decode_column(self.reader())?);
        let columns_count = i32::try_decode_column(self.reader())?;
        let paging_state;
        if flags.has_more_pages() {
            let paging_state_len = i32::try_decode_column(self.reader())?;
            if paging_state_len == -1 {
                paging_state = PagingState::new(None);
            } else {
                let mut paging_vector = vec![0u8; paging_state_len as usize];
                self.reader().read_exact(&mut paging_vector)?;
                paging_state = PagingState::new(paging_vector.into());
            }
        } else {
            paging_state = PagingState::new(None)
        }
        let mut global_table_spec = None;
        let mut columns_specs = Vec::new();
        if !flags.no_metadata() {
            if flags.global_table_spec() {
                let keyspace = string(self.reader())?;
                let table_name = string(self.reader())?;
                global_table_spec.replace(TableSpec::new(keyspace, table_name));
                for _ in 0..columns_count {
                    let col_name = string(self.reader())?;
                    let col_type = ColType::try_from(self.reader())?;
                    let col_spec = ColumnSpec::new(None, col_name, col_type);
                    columns_specs.push(col_spec);
                }
            } else {
                for _ in 0..columns_count {
                    let keyspace = string(self.reader())?;
                    let table_name = string(self.reader())?;
                    let col_name = string(self.reader())?;
                    let col_type = ColType::try_from(self.reader())?;
                    let col_spec = ColumnSpec::new(TableSpec::new(keyspace, table_name).into(), col_name, col_type);
                    columns_specs.push(col_spec);
                }
            }
            columns_specs.reverse();
        }
        Ok(Metadata::new(
            flags,
            columns_count,
            paging_state,
            global_table_spec,
            columns_specs,
        ))
    }
}

/// The column decoder trait to decode the frame.
pub trait ColumnDecoder {
    /// Decode the column value, include encoded length
    fn try_decode<R: Read>(reader: &mut R) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        let len = i32::try_decode_column(reader)?;
        if len > 0 {
            let mut handle = reader.take(len as u64);
            Self::try_decode_column(&mut handle)
        } else {
            let mut empty = std::io::empty();
            Self::try_decode_column(&mut empty)
        }
    }
    /// Decode the column.
    fn try_decode_column<R: Read>(reader: &mut R) -> anyhow::Result<Self>
    where
        Self: Sized;
}

impl<T: ColumnDecoder> ColumnDecoder for Option<T> {
    fn try_decode<R: Read>(reader: &mut R) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Self::try_decode_column(reader)
    }

    fn try_decode_column<R: Read>(reader: &mut R) -> anyhow::Result<Self> {
        let len = i32::try_decode_column(reader)?;
        if len > 0 {
            let mut handle = reader.take(len as u64);
            Ok(Some(T::try_decode_column(&mut handle)?))
        } else {
            Ok(None)
        }
    }
}

impl ColumnDecoder for i64 {
    fn try_decode_column<R: Read>(reader: &mut R) -> anyhow::Result<Self> {
        let mut buf = [0u8; 8];
        reader.read_exact(&mut buf)?;
        Ok(i64::from_be_bytes(buf))
    }
}

impl ColumnDecoder for u64 {
    fn try_decode_column<R: Read>(reader: &mut R) -> anyhow::Result<Self> {
        let mut buf = [0u8; 8];
        reader.read_exact(&mut buf)?;
        Ok(u64::from_be_bytes(buf))
    }
}

impl ColumnDecoder for f64 {
    fn try_decode_column<R: Read>(reader: &mut R) -> anyhow::Result<Self> {
        let mut buf = [0u8; 8];
        reader.read_exact(&mut buf)?;
        Ok(f64::from_be_bytes(buf))
    }
}

impl ColumnDecoder for i32 {
    fn try_decode_column<R: Read>(reader: &mut R) -> anyhow::Result<Self> {
        let mut buf = [0u8; 4];
        reader.read_exact(&mut buf)?;
        Ok(i32::from_be_bytes(buf))
    }
}

impl ColumnDecoder for u32 {
    fn try_decode_column<R: Read>(reader: &mut R) -> anyhow::Result<Self> {
        let mut buf = [0u8; 4];
        reader.read_exact(&mut buf)?;
        Ok(u32::from_be_bytes(buf))
    }
}

impl ColumnDecoder for f32 {
    fn try_decode_column<R: Read>(reader: &mut R) -> anyhow::Result<Self> {
        let mut buf = [0u8; 4];
        reader.read_exact(&mut buf)?;
        Ok(f32::from_be_bytes(buf))
    }
}

impl ColumnDecoder for i16 {
    fn try_decode_column<R: Read>(reader: &mut R) -> anyhow::Result<Self> {
        let mut buf = [0u8; 2];
        reader.read_exact(&mut buf)?;
        Ok(i16::from_be_bytes(buf))
    }
}

impl ColumnDecoder for u16 {
    fn try_decode_column<R: Read>(reader: &mut R) -> anyhow::Result<Self> {
        let mut buf = [0u8; 2];
        reader.read_exact(&mut buf)?;
        Ok(u16::from_be_bytes(buf))
    }
}

impl ColumnDecoder for i8 {
    fn try_decode_column<R: Read>(reader: &mut R) -> anyhow::Result<Self> {
        let mut buf = [0u8; 1];
        reader.read_exact(&mut buf)?;
        Ok(i8::from_be_bytes(buf))
    }
}

impl ColumnDecoder for u8 {
    fn try_decode_column<R: Read>(reader: &mut R) -> anyhow::Result<Self> {
        let mut buf = [0u8; 1];
        reader.read_exact(&mut buf)?;
        Ok(buf[0])
    }
}

impl ColumnDecoder for bool {
    fn try_decode_column<R: Read>(reader: &mut R) -> anyhow::Result<Self> {
        Ok(u8::try_decode_column(reader)? != 0)
    }
}

impl ColumnDecoder for String {
    fn try_decode_column<R: Read>(reader: &mut R) -> anyhow::Result<Self> {
        let mut buf = String::new();
        reader.read_to_string(&mut buf)?;
        Ok(buf)
    }
}

impl ColumnDecoder for IpAddr {
    fn try_decode_column<R: Read>(reader: &mut R) -> anyhow::Result<Self> {
        let octects_len = reader.bytes().size_hint().0;
        Ok(if octects_len == 4 {
            IpAddr::V4(Ipv4Addr::try_decode_column(reader)?)
        } else {
            IpAddr::V6(Ipv6Addr::try_decode_column(reader)?)
        })
    }
}

impl ColumnDecoder for Ipv4Addr {
    fn try_decode_column<R: Read>(reader: &mut R) -> anyhow::Result<Self> {
        let mut buf = [0u8; 4];
        reader.read_exact(&mut buf)?;
        Ok(Ipv4Addr::from(buf))
    }
}

impl ColumnDecoder for Ipv6Addr {
    fn try_decode_column<R: Read>(reader: &mut R) -> anyhow::Result<Self> {
        let mut buf = [0u8; 16];
        reader.read_exact(&mut buf)?;
        Ok(Ipv6Addr::from(buf))
    }
}

impl ColumnDecoder for Cursor<Vec<u8>> {
    fn try_decode_column<R: Read>(reader: &mut R) -> anyhow::Result<Self> {
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes)?;
        Ok(Cursor::new(bytes))
    }
}

impl<E> ColumnDecoder for Vec<E>
where
    E: ColumnDecoder,
{
    fn try_decode_column<R: Read>(reader: &mut R) -> anyhow::Result<Self> {
        let list_len = i32::try_decode_column(reader)?;
        let mut list: Vec<E> = Vec::new();
        for _ in 0..list_len {
            let item = E::try_decode(reader)?;
            list.push(item);
        }
        Ok(list)
    }
}

impl<E> ColumnDecoder for VecDeque<E>
where
    E: ColumnDecoder,
{
    fn try_decode_column<R: Read>(reader: &mut R) -> anyhow::Result<Self> {
        let list_len = i32::try_decode_column(reader)?;
        let mut list: VecDeque<E> = VecDeque::new();
        for _ in 0..list_len {
            let item = E::try_decode(reader)?;
            list.push_back(item);
        }
        Ok(list)
    }
}

impl<K, V, S> ColumnDecoder for HashMap<K, V, S>
where
    K: Eq + Hash + ColumnDecoder,
    V: ColumnDecoder,
    S: ::std::hash::BuildHasher + Default,
{
    fn try_decode_column<R: Read>(reader: &mut R) -> anyhow::Result<Self> {
        let map_len = i32::try_decode_column(reader)?;
        let mut map: HashMap<K, V, S> = HashMap::default();
        for _ in 0..map_len {
            let k = K::try_decode(reader)?;
            let v = V::try_decode(reader)?;
            map.insert(k, v);
        }
        Ok(map)
    }
}

impl<K, V> ColumnDecoder for BTreeMap<K, V>
where
    K: Ord + ColumnDecoder,
    V: ColumnDecoder,
{
    fn try_decode_column<R: Read>(reader: &mut R) -> anyhow::Result<Self> {
        let map_len = i32::try_decode_column(reader)?;
        let mut map: BTreeMap<K, V> = BTreeMap::default();
        for _ in 0..map_len {
            let k = K::try_decode(reader)?;
            let v = V::try_decode(reader)?;
            map.insert(k, v);
        }
        Ok(map)
    }
}

impl<E> ColumnDecoder for BTreeSet<E>
where
    E: Ord + ColumnDecoder,
{
    fn try_decode_column<R: Read>(reader: &mut R) -> anyhow::Result<Self> {
        let list_len = i32::try_decode_column(reader)?;
        let mut list: BTreeSet<E> = BTreeSet::new();
        for _ in 0..list_len {
            let item = E::try_decode(reader)?;
            list.insert(item);
        }
        Ok(list)
    }
}

impl<E> ColumnDecoder for HashSet<E>
where
    E: Hash + Eq + ColumnDecoder,
{
    fn try_decode_column<R: Read>(reader: &mut R) -> anyhow::Result<Self> {
        let list_len = i32::try_decode_column(reader)?;
        let mut list: HashSet<E> = HashSet::new();
        for _ in 0..list_len {
            let item = E::try_decode(reader)?;
            list.insert(item);
        }
        Ok(list)
    }
}

impl<E> ColumnDecoder for BinaryHeap<E>
where
    E: Ord + ColumnDecoder,
{
    fn try_decode_column<R: Read>(reader: &mut R) -> anyhow::Result<Self> {
        let list_len = i32::try_decode_column(reader)?;
        let mut list: BinaryHeap<E> = BinaryHeap::new();
        for _ in 0..list_len {
            let item = E::try_decode(reader)?;
            list.push(item);
        }
        Ok(list)
    }
}

impl<E> ColumnDecoder for LinkedList<E>
where
    E: ColumnDecoder,
{
    fn try_decode_column<R: Read>(reader: &mut R) -> anyhow::Result<Self> {
        let list_len = i32::try_decode_column(reader)?;
        let mut list: LinkedList<E> = LinkedList::new();
        for _ in 0..list_len {
            let item = E::try_decode(reader)?;
            list.push_back(item);
        }
        Ok(list)
    }
}

impl ColumnDecoder for NaiveDate {
    fn try_decode_column<R: Read>(reader: &mut R) -> anyhow::Result<Self> {
        let num_days = u32::try_decode_column(reader)? - (1u32 << 31);
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).ok_or(anyhow::anyhow!("Out of range ymd"))?;
        Ok(epoch
            .checked_add_signed(chrono::Duration::days(num_days as i64))
            .ok_or(anyhow!("Overflowed epoch + duration::days"))?)
    }
}

impl ColumnDecoder for NaiveTime {
    fn try_decode_column<R: Read>(reader: &mut R) -> anyhow::Result<Self> {
        let nanos = u64::try_decode_column(reader)?;
        let (secs, nanos) = (nanos / 1_000_000_000, nanos % 1_000_000_000);
        Ok(NaiveTime::from_num_seconds_from_midnight_opt(secs as u32, nanos as u32)
            .ok_or(anyhow::anyhow!("Out of range num_seconds_from_midnight"))?)
    }
}

impl ColumnDecoder for NaiveDateTime {
    fn try_decode_column<R: Read>(reader: &mut R) -> anyhow::Result<Self> {
        let millis = u64::try_decode_column(reader)?;
        let (secs, nanos) = (millis / 1_000, millis % 1_000 * 1_000_000);
        Ok(NaiveDateTime::from_timestamp_opt(secs as i64, nanos as u32)
            .ok_or(anyhow::anyhow!("Out of range timestamp"))?)
    }
}

// helper types decoder functions
/// Get the string list from a u8 slice.
pub fn string_list<R: Read>(reader: &mut R) -> anyhow::Result<Vec<String>> {
    let list_len = u16::try_decode_column(reader)? as usize;
    let mut list: Vec<String> = Vec::with_capacity(list_len);
    for _ in 0..list_len {
        let string_len = u16::try_decode_column(reader)? as u64;
        let mut h = reader.take(string_len);
        let string = String::try_decode_column(&mut h)?;
        list.push(string.to_string());
    }
    Ok(list)
}

/// Get the `String` from a u8 slice.
pub fn string<R: Read>(reader: &mut R) -> anyhow::Result<String> {
    let length = u16::try_decode_column(reader)? as u64;
    let mut h = reader.take(length);
    Ok(String::try_decode_column(&mut h)?)
}

/// Get the vector from byte slice.
pub fn bytes<R: Read>(reader: &mut R) -> anyhow::Result<Option<Vec<u8>>> {
    let length = i32::try_decode_column(reader)?;
    Ok(if length >= 0 {
        let mut bytes = Vec::new();
        let mut h = reader.take(length as u64);
        h.read_to_end(&mut bytes)?;
        Some(bytes)
    } else {
        None
    })
}

/// Get the `short_bytes` from a u8 slice.
#[allow(unused)]
pub fn short_bytes(slice: &[u8]) -> anyhow::Result<Vec<u8>> {
    let length = u16::from_be_bytes(slice[0..2].try_into()?) as usize;
    Ok(slice[2..][..length].into())
}

/// Get the `prepared_id` from a u8 slice.
pub fn prepared_id<R: Read>(reader: &mut R) -> anyhow::Result<[u8; 16]> {
    let length = u16::try_decode_column(reader)? as usize;
    let mut h = reader.take(length as u64);
    let mut res = [0u8; 16];
    h.read_exact(&mut res)?;
    Ok(res)
}

/// Get hashmap of string to string vector from slice.
pub fn string_multimap<R: Read>(reader: &mut R) -> anyhow::Result<HashMap<String, Vec<String>>> {
    let length = u16::try_decode_column(reader)? as usize;
    let mut multimap = HashMap::with_capacity(length);
    for _ in 0..length {
        let key = string(reader)?;
        let list = string_list(reader)?;
        multimap.insert(key, list);
    }
    Ok(multimap)
}
