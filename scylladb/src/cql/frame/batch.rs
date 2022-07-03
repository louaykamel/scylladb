//! This module implements the batch query frame.

use super::{
    batchflags::*,
    consistency::Consistency,
    encoder::{
        ColumnEncoder,
        BE_8_BYTES_LEN,
        BE_NULL_BYTES_LEN,
        BE_UNSET_BYTES_LEN,
    },
    opcode::BATCH,
    Binder,
    Statements,
    MD5_BE_LENGTH,
};
use crate::cql::compression::{
    Compression,
    MyCompression,
};

/// Blanket cql frame header for BATCH frame.
const BATCH_HEADER: &'static [u8] = &[4, 0, 0, 0, BATCH, 0, 0, 0, 0];

/// The batch frame.
pub struct Batch(pub Vec<u8>);

#[repr(u8)]
/// The batch type enum.
pub enum BatchTypes {
    /// The batch will be logged.
    Logged = 0,
    /// The batch will be unlogged.
    Unlogged = 1,
    /// The batch will be a "counter" batch.
    Counter = 2,
}

/// Batch request builder. Maintains a type-gated stage so that operations
/// are applied in a valid order.
///
/// ## Example
/// ```
/// use scylla_rs::cql::{
///     Batch,
///     Consistency,
///     Statements,
/// };
///
/// let builder = Batch::new();
/// let batch = builder
///     .logged()
///     .statement("statement")
///     .consistency(Consistency::One)
///     .build()?;
/// let payload = batch.0;
/// # Ok::<(), anyhow::Error>(())
/// ```
#[derive(Clone)]
pub struct BatchBuilder<Type: Copy + Into<u8>, Stage: Copy> {
    buffer: Vec<u8>,
    query_count: u16,
    batch_type: Type,
    stage: Stage,
}

/// Gating type for batch headers
#[derive(Copy, Clone)]
pub struct BatchHeader;

/// Gating type for batch type
#[derive(Copy, Clone)]
pub struct BatchType;

/// Gating type for unset batch type
#[derive(Copy, Clone)]
pub struct BatchTypeUnset;
impl Into<u8> for BatchTypeUnset {
    fn into(self) -> u8 {
        panic!("Batch type is not set!")
    }
}

/// Gating type for logged batch type
#[derive(Copy, Clone)]
pub struct BatchTypeLogged;
impl Into<u8> for BatchTypeLogged {
    fn into(self) -> u8 {
        0
    }
}

/// Gating type for unlogged batch type
#[derive(Copy, Clone)]
pub struct BatchTypeUnlogged;
impl Into<u8> for BatchTypeUnlogged {
    fn into(self) -> u8 {
        1
    }
}

/// Gating type for counter batch type
#[derive(Copy, Clone)]
pub struct BatchTypeCounter;
impl Into<u8> for BatchTypeCounter {
    fn into(self) -> u8 {
        2
    }
}

/// Gating type for statement / prepared id
#[derive(Copy, Clone)]
pub struct BatchStatementOrId;

/// Gating type for statement values
#[derive(Copy, Clone)]
pub struct BatchValues {
    value_count: u16,
    index: usize,
}

/// Gating type for batch flags
#[derive(Copy, Clone)]
pub struct BatchFlags;

/// Gating type for batch timestamp
#[derive(Copy, Clone)]
pub struct BatchTimestamp;

/// Gating type for completed batch
#[derive(Copy, Clone)]
pub struct BatchBuild;

impl BatchBuilder<BatchTypeUnset, BatchHeader> {
    /// Create a new batch builder
    pub fn new() -> BatchBuilder<BatchTypeUnset, BatchType> {
        let mut buffer: Vec<u8> = Vec::new();
        buffer.extend_from_slice(&BATCH_HEADER);
        BatchBuilder {
            buffer,
            query_count: 0,
            batch_type: BatchTypeUnset,
            stage: BatchType,
        }
    }
    /// Create a new batch build with a given buffer capacity
    pub fn with_capacity(capacity: usize) -> BatchBuilder<BatchTypeUnset, BatchType> {
        let mut buffer: Vec<u8> = Vec::with_capacity(capacity);
        buffer.extend_from_slice(&BATCH_HEADER);
        BatchBuilder {
            buffer,
            query_count: 0,
            batch_type: BatchTypeUnset,
            stage: BatchType,
        }
    }
}

impl BatchBuilder<BatchTypeUnset, BatchType> {
    /// Set the batch type in the Batch frame. See https://cassandra.apache.org/doc/latest/cql/dml.html#batch
    pub fn batch_type<Type: Copy + Into<u8>>(mut self, batch_type: Type) -> BatchBuilder<Type, BatchStatementOrId> {
        // push batch_type and pad zero querycount
        self.buffer.extend(&[batch_type.into(), 0, 0]);
        BatchBuilder {
            buffer: self.buffer,
            query_count: self.query_count,
            batch_type,
            stage: BatchStatementOrId,
        }
    }
    /// Set the batch type to logged. See https://cassandra.apache.org/doc/latest/cql/dml.html#batch
    pub fn logged(mut self) -> BatchBuilder<BatchTypeLogged, BatchStatementOrId> {
        // push logged batch_type and pad zero querycount
        self.buffer.extend(&[0, 0, 0]);
        BatchBuilder {
            buffer: self.buffer,
            query_count: self.query_count,
            batch_type: BatchTypeLogged,
            stage: BatchStatementOrId,
        }
    }
    /// Set the batch type to unlogged. See https://cassandra.apache.org/doc/latest/cql/dml.html#unlogged-batches
    pub fn unlogged(mut self) -> BatchBuilder<BatchTypeUnlogged, BatchStatementOrId> {
        // push unlogged batch_type and pad zero querycount
        self.buffer.extend(&[1, 0, 0]);
        BatchBuilder {
            buffer: self.buffer,
            query_count: self.query_count,
            batch_type: BatchTypeUnlogged,
            stage: BatchStatementOrId,
        }
    }
    /// Set the batch type to counter. See https://cassandra.apache.org/doc/latest/cql/dml.html#counter-batches
    pub fn counter(mut self) -> BatchBuilder<BatchTypeCounter, BatchStatementOrId> {
        // push counter batch_type and pad zero querycount
        self.buffer.extend(&[2, 0, 0]);
        BatchBuilder {
            buffer: self.buffer,
            query_count: self.query_count,
            batch_type: BatchTypeCounter,
            stage: BatchStatementOrId,
        }
    }
}

impl<Type: Copy + Into<u8>> Statements for BatchBuilder<Type, BatchStatementOrId> {
    type Return = BatchBuilder<Type, BatchValues>;
    /// Set the statement in the Batch frame.
    fn statement(mut self, statement: &str) -> Self::Return {
        // normal query
        self.buffer.push(0);
        self.buffer.extend(&i32::to_be_bytes(statement.len() as i32));
        self.buffer.extend(statement.bytes());
        self.query_count += 1; // update querycount
        let index = self.buffer.len();
        // pad zero value_count for the query
        self.buffer.extend(&[0, 0]);
        BatchBuilder {
            buffer: self.buffer,
            query_count: self.query_count,
            batch_type: self.batch_type,
            stage: BatchValues { value_count: 0, index },
        }
    }
    /// Set the id in the Batch frame.
    fn id(mut self, id: &[u8; 16]) -> Self::Return {
        // prepared query
        self.buffer.push(1);
        self.buffer.extend(&MD5_BE_LENGTH);
        self.buffer.extend(id);
        self.query_count += 1;
        let index = self.buffer.len();
        // pad zero value_count for the query
        self.buffer.extend(&[0, 0]);
        BatchBuilder {
            buffer: self.buffer,
            query_count: self.query_count,
            batch_type: self.batch_type,
            stage: BatchValues { value_count: 0, index },
        }
    }
}

impl<Type: Copy + Into<u8>> BatchBuilder<Type, BatchValues> {
    pub(crate) fn commit_value_count(&mut self) {
        self.buffer[self.stage.index..(self.stage.index + 2)]
            .copy_from_slice(&u16::to_be_bytes(self.stage.value_count));
    }
}

impl<Type: Copy + Into<u8>> Binder for BatchBuilder<Type, BatchValues> {
    /// Set the value in the Batch frame.
    fn value<V: ColumnEncoder + Sync>(mut self, value: V) -> Self
    where
        Self: Sized,
    {
        value.encode(&mut self.buffer);
        self.stage.value_count += 1;
        self
    }

    /// Set the value to be unset in the Batch frame.
    fn unset_value(mut self) -> Self
    where
        Self: Sized,
    {
        self.buffer.extend(&BE_UNSET_BYTES_LEN);
        self.stage.value_count += 1;
        self
    }

    /// Set the value to be null in the Batch frame.
    fn null_value(mut self) -> Self
    where
        Self: Sized,
    {
        self.buffer.extend(&BE_NULL_BYTES_LEN);
        self.stage.value_count += 1;
        self
    }
}

impl<Type: Copy + Into<u8>> Statements for BatchBuilder<Type, BatchValues> {
    type Return = Self;
    /// Set the statement in the Batch frame.
    fn statement(mut self, statement: &str) -> BatchBuilder<Type, BatchValues> {
        // adjust value_count for prev query(if any)
        self.commit_value_count();
        // normal query
        self.buffer.push(0);
        self.buffer.extend(&i32::to_be_bytes(statement.len() as i32));
        self.buffer.extend(statement.bytes());
        self.query_count += 1; // update querycount
                               // pad zero value_count for the query
        let index = self.buffer.len();
        self.buffer.extend(&[0, 0]);
        BatchBuilder {
            buffer: self.buffer,
            query_count: self.query_count,
            batch_type: self.batch_type,
            stage: BatchValues { value_count: 0, index },
        }
    }
    /// Set the id in the Batch frame.
    fn id(mut self, id: &[u8; 16]) -> BatchBuilder<Type, BatchValues> {
        // adjust value_count for prev query
        self.commit_value_count();
        // prepared query
        self.buffer.push(1);
        self.buffer.extend(&MD5_BE_LENGTH);
        self.buffer.extend(id);
        self.query_count += 1;
        // pad zero value_count for the query
        let index = self.buffer.len();
        self.buffer.extend(&[0, 0]);
        BatchBuilder {
            buffer: self.buffer,
            query_count: self.query_count,
            batch_type: self.batch_type,
            stage: BatchValues { value_count: 0, index },
        }
    }
}
impl<Type: Copy + Into<u8>> BatchBuilder<Type, BatchValues> {
    /// Set the consistency of the Batch frame.
    pub fn consistency(mut self, consistency: Consistency) -> BatchBuilder<Type, BatchFlags> {
        // adjust value_count for prev query
        self.commit_value_count();
        self.buffer.extend(&u16::to_be_bytes(consistency as u16));
        BatchBuilder {
            buffer: self.buffer,
            query_count: self.query_count,
            batch_type: self.batch_type,
            stage: BatchFlags,
        }
    }
}

impl<Type: Copy + Into<u8>> BatchBuilder<Type, BatchFlags> {
    /// Set the serial consistency in the Batch frame.
    pub fn serial_consistency(mut self, consistency: Consistency) -> BatchBuilder<Type, BatchTimestamp> {
        // add serial_consistency byte for batch flags
        self.buffer.push(SERIAL_CONSISTENCY);
        self.buffer.extend(&u16::to_be_bytes(consistency as u16));
        BatchBuilder {
            buffer: self.buffer,
            query_count: self.query_count,
            batch_type: self.batch_type,
            stage: BatchTimestamp,
        }
    }
    /// Set the timestamp of the Batch frame.
    pub fn timestamp(mut self, timestamp: i64) -> BatchBuilder<Type, BatchBuild> {
        // add timestamp byte for batch flags
        self.buffer.push(TIMESTAMP);
        self.buffer.extend(&BE_8_BYTES_LEN);
        self.buffer.extend(&i64::to_be_bytes(timestamp));
        BatchBuilder {
            buffer: self.buffer,
            query_count: self.query_count,
            batch_type: self.batch_type,
            stage: BatchBuild,
        }
    }
    /// Build a Batch frame.
    pub fn build(mut self) -> anyhow::Result<Batch> {
        // apply compression flag(if any to the header)
        self.buffer[1] |= MyCompression::flag();
        // add noflags byte for batch flags
        self.buffer.push(NOFLAGS);
        // adjust the querycount
        self.buffer[10..12].copy_from_slice(&u16::to_be_bytes(self.query_count));
        self.buffer = MyCompression::get().compress(self.buffer)?;
        Ok(Batch(self.buffer))
    }
}

impl<Type: Copy + Into<u8>> BatchBuilder<Type, BatchTimestamp> {
    /// Set the timestamp of the Batch frame.
    pub fn timestamp(mut self, timestamp: i64) -> BatchBuilder<Type, BatchBuild> {
        self.buffer.last_mut().map(|last_byte| *last_byte |= TIMESTAMP);
        self.buffer.extend(&BE_8_BYTES_LEN);
        self.buffer.extend(&i64::to_be_bytes(timestamp));
        BatchBuilder {
            buffer: self.buffer,
            query_count: self.query_count,
            batch_type: self.batch_type,
            stage: BatchBuild,
        }
    }
    /// Build a Batch frame.
    pub fn build(mut self) -> anyhow::Result<Batch> {
        // apply compression flag(if any to the header)
        self.buffer[1] |= MyCompression::flag();
        // adjust the querycount
        self.buffer[10..12].copy_from_slice(&u16::to_be_bytes(self.query_count));
        self.buffer = MyCompression::get().compress(self.buffer)?;
        Ok(Batch(self.buffer))
    }
}

impl<Type: Copy + Into<u8>> BatchBuilder<Type, BatchBuild> {
    /// Build a Batch frame.
    pub fn build(mut self) -> anyhow::Result<Batch> {
        // apply compression flag(if any to the header)
        self.buffer[1] |= MyCompression::flag();
        // adjust the querycount
        self.buffer[10..12].copy_from_slice(&u16::to_be_bytes(self.query_count));
        self.buffer = MyCompression::get().compress(self.buffer)?;
        Ok(Batch(self.buffer))
    }
}
impl Batch {
    /// Create Batch cql frame
    pub fn new() -> BatchBuilder<BatchTypeUnset, BatchType> {
        BatchBuilder::new()
    }
    /// Create Batch cql frame with capacity
    pub fn with_capacity(capacity: usize) -> BatchBuilder<BatchTypeUnset, BatchType> {
        BatchBuilder::with_capacity(capacity)
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    // note: junk data
    fn simple_query_builder_test() {
        let Batch(_payload) = Batch::new()
            .logged()
            .statement("INSERT_TX_QUERY")
            .value(&"HASH_VALUE")
            .value(&"PAYLOAD_VALUE")
            .id(&[0; 16]) // add second query(prepared one) to the batch
            .value(&"JUNK_VALUE") // junk value
            .consistency(Consistency::One)
            .build()
            .unwrap();
    }
}
