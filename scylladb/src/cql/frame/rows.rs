//! This module defines the row/column decoder/encoder for the frame structure.

use std::{
    borrow::BorrowMut,
    collections::HashMap,
};

use super::{
    decoder::string,
    ColumnDecoder,
    Frame,
};
use log::error;

/// The column count type.
pub type ColumnsCount = i32;
#[derive(Debug, Clone, Copy)]
/// The flags for row decoder.
pub struct Flags {
    #[allow(unused)]
    global_table_spec: bool,
    #[allow(unused)]
    has_more_pages: bool,
    #[allow(unused)]
    no_metadata: bool,
}

impl Flags {
    /// Decode i32 to flags of row decoder.
    pub fn from_i32(flags: i32) -> Self {
        Flags {
            global_table_spec: (flags & 1) == 1,
            has_more_pages: (flags & 2) == 2,
            no_metadata: (flags & 4) == 4,
        }
    }
    /// Check if are there more pages to decode.
    pub fn has_more_pages(&self) -> bool {
        self.has_more_pages
    }
    /// Check if no_metadata is set.
    pub fn no_metadata(&self) -> bool {
        self.no_metadata
    }
    /// Check if global_table_spec exist.
    pub fn global_table_spec(&self) -> bool {
        self.global_table_spec
    }
}
#[derive(Debug, Clone)]
/// The pageing state of the response.
pub struct PagingState {
    paging_state: Option<Vec<u8>>,
}
impl PagingState {
    /// Create a new paing state.
    pub fn new(paging_state: Option<Vec<u8>>) -> Self {
        PagingState { paging_state }
    }
}

#[derive(Debug, Clone)]
/// Global Table spec
pub struct TableSpec {
    keyspace: String,
    table_name: String,
}

impl TableSpec {
    pub(super) fn new(keyspace: String, table_name: String) -> Self {
        Self { keyspace, table_name }
    }
    /// Returns the keyspace name
    pub fn keyspace(&self) -> &String {
        &self.keyspace
    }
    /// Returns the table name
    pub fn table_name(&self) -> &String {
        &self.table_name
    }
}

#[derive(Debug, Clone)]
/// The column spec
pub struct ColumnSpec {
    /// The global table spec avaliable if flag is not set
    table_spec: Option<TableSpec>,
    /// Column name
    col_name: String,
    /// Column type
    col_type: ColType,
}

impl ColumnSpec {
    /// Create new column spec
    pub(super) fn new(table_spec: Option<TableSpec>, col_name: String, col_type: ColType) -> Self {
        Self {
            table_spec,
            col_name,
            col_type,
        }
    }
    /// Returns the table spec
    pub fn table_spec(&self) -> Option<&TableSpec> {
        self.table_spec.as_ref()
    }
    /// Returns the column name
    pub fn col_name(&self) -> &String {
        &self.col_name
    }
    /// Returns the column type
    pub fn col_type(&self) -> &ColType {
        &self.col_type
    }
}
#[derive(Debug, Clone)]
/// The cql column type
pub enum ColType {
    /// Custom cql column type
    Custom(String),
    /// Ascii cql column type
    Ascii,
    /// Bigint cql column type
    Bigint,
    /// Blob cql column type
    Blob,
    /// Bool cql column type
    Boolean,
    /// Counter cql column type
    Counter,
    /// Decimal cql column type
    Decimal,
    /// Double cql column type
    Double,
    /// Float cql column type
    Float,
    /// Int cql column type
    Int,
    /// Timestamp cql column type
    Timestamp,
    /// UUID cql column type
    Uuid,
    /// Varchar cql column type
    Varchar,
    /// Varint cql column type
    Varint,
    /// TimeUuid cql column type
    Timeuuid,
    /// Inet cql column type
    Inet,
    /// Date cql column type
    Date,
    /// Time cql column type
    Time,
    /// SmallInt cql column type
    Smallint,
    /// Tinyint cql column type
    Tinyint,
    /// List cql column type
    List {
        /// The list elements type
        element: Box<ColType>,
    },
    /// Map collection cql column type
    Map {
        /// The map key type
        key: Box<ColType>,
        /// The map value key type
        value: Box<ColType>,
    },
    /// Set cql column type
    Set {
        /// The Set elements type
        element: Box<ColType>,
    },
    /// User defined cql column type
    Udt {
        /// keyspace name this UDT is part of.
        ks: String,
        /// UDT name
        udt_name: String,
        /// UDT Fields where key is the name and value is the type
        fields: HashMap<String, ColType>,
    },
    /// Tuple cql column type
    Tuple {
        /// The Tuple elements type
        elements: Vec<ColType>,
    },
}

impl TryFrom<&mut std::io::Cursor<Vec<u8>>> for ColType {
    type Error = anyhow::Error;

    fn try_from(reader: &mut std::io::Cursor<Vec<u8>>) -> Result<Self, Self::Error> {
        let option_id = u16::try_decode_column(reader)?;
        match option_id {
            0 => Ok(Self::Custom(string(reader.borrow_mut())?)),
            1 => Ok(Self::Ascii),
            2 => Ok(Self::Bigint),
            3 => Ok(Self::Blob),
            4 => Ok(Self::Boolean),
            5 => Ok(Self::Counter),
            6 => Ok(Self::Decimal),
            7 => Ok(Self::Double),
            8 => Ok(Self::Float),
            9 => Ok(Self::Int),
            11 => Ok(Self::Timestamp),
            12 => Ok(Self::Uuid),
            13 => Ok(Self::Varchar),
            14 => Ok(Self::Varint),
            15 => Ok(Self::Timeuuid),
            16 => Ok(Self::Inet),
            17 => Ok(Self::Date),
            18 => Ok(Self::Time),
            19 => Ok(Self::Smallint),
            20 => Ok(Self::Tinyint),
            32 => Ok(Self::List {
                element: Box::new(Self::try_from(reader.borrow_mut())?),
            }),
            33 => Ok(Self::Map {
                key: Box::new(Self::try_from(reader.borrow_mut())?),
                value: Box::new(Self::try_from(reader.borrow_mut())?),
            }),
            34 => Ok(Self::Set {
                element: Box::new(Self::try_from(reader.borrow_mut())?),
            }),
            48 => Ok(Self::Udt {
                ks: string(reader.borrow_mut())?,
                udt_name: string(reader.borrow_mut())?,
                fields: {
                    let mut fields = HashMap::new();
                    let n = u16::try_decode_column(reader.borrow_mut())?;
                    for _ in 0..n {
                        let field_name = string(reader.borrow_mut())?;
                        let field_type = Self::try_from(reader.borrow_mut())?;
                        fields.insert(field_name, field_type);
                    }
                    fields
                },
            }),
            49 => Ok(Self::Tuple {
                elements: {
                    let mut tuple = Vec::new();
                    let n = u16::try_decode_column(reader.borrow_mut())?;
                    for _ in 0..n {
                        let ele_type = Self::try_from(reader.borrow_mut())?;
                        tuple.push(ele_type);
                    }
                    tuple.reverse();
                    tuple
                },
            }),
            _ => anyhow::bail!("Invalid option coltype id"),
        }
    }
}

#[derive(Debug, Clone)]
/// The meta structure of the row.
pub struct Metadata {
    flags: Flags,
    #[allow(unused)]
    columns_count: ColumnsCount,
    paging_state: PagingState,
    global_table_spec: Option<TableSpec>,
    columns_specs: Vec<ColumnSpec>,
}

impl Metadata {
    /// Create a new meta data.
    pub fn new(
        flags: Flags,
        columns_count: ColumnsCount,
        paging_state: PagingState,
        global_table_spec: Option<TableSpec>,
        columns_specs: Vec<ColumnSpec>,
    ) -> Self {
        Metadata {
            flags,
            columns_count,
            paging_state,
            global_table_spec,
            columns_specs,
        }
    }
    /// Returns the global spec (only if flag is set)
    pub fn global_table_spec(&self) -> Option<&TableSpec> {
        self.global_table_spec.as_ref()
    }
    /// Returns the column specs
    pub fn column_specs(&self) -> &Vec<ColumnSpec> {
        &self.columns_specs
    }
    /// Take the paging state of the metadata.
    pub fn take_paging_state(&mut self) -> Option<Vec<u8>> {
        self.paging_state.paging_state.take()
    }
    /// Get reference to the paging state of the metadata.
    pub fn get_paging_state(&self) -> Option<&Vec<u8>> {
        self.paging_state.paging_state.as_ref()
    }
    /// Check if it has more pages to request
    pub fn has_more_pages(&self) -> bool {
        self.flags.has_more_pages()
    }
}

/// Rows trait to decode the final result from scylla
pub trait Rows: Iterator {
    /// create new rows decoder struct
    fn new(decoder: super::decoder::Decoder) -> anyhow::Result<Self>
    where
        Self: Sized;
    /// Take the paging_state from the Rows result
    fn take_paging_state(&mut self) -> Option<Vec<u8>>;
}

/// Defines a result-set row
pub trait Row: Sized {
    /// Get the rows iterator
    fn rows_iter(decoder: super::Decoder) -> anyhow::Result<Iter<Self>> {
        Iter::new(decoder)
    }
    /// Define how to decode the row
    fn try_decode_row<R: ColumnValue>(rows: &mut R) -> anyhow::Result<Self>
    where
        Self: Sized;
}

impl<T> Row for T
where
    T: ColumnDecoder,
{
    fn try_decode_row<R: ColumnValue>(rows: &mut R) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        rows.column_value()
    }
}

/// Defines a result-set column value
pub trait ColumnValue {
    /// Decode the column value of C type;
    fn column_value<C: ColumnDecoder>(&mut self) -> anyhow::Result<C>;
}

#[allow(unused)]
#[derive(Clone, Debug)]
/// Column iterator
pub struct ColumnIter<T> {
    decoder: super::Decoder,
    rows_count: usize,
    remaining_rows_count: usize,
    remaining_columns_count: usize,
    metadata: Metadata,
    _marker: std::marker::PhantomData<T>,
}

#[allow(unused)]
#[derive(Clone, Debug)]
/// AnyIterator to iterator over the column values
pub struct AnyIter {
    decoder: super::Decoder,
    rows_count: usize,
    remaining_total_columns_count: usize,
    metadata: Metadata,
}

impl AnyIter {
    /// Iterates the next column value or row
    pub fn next<T: Row>(&mut self) -> Option<T> {
        if self.remaining_total_columns_count > 0 {
            T::try_decode_row(self).map_err(|e| error!("{}", e)).ok()
        } else {
            None
        }
    }
    /// Create new iterator
    pub fn new(mut decoder: super::Decoder) -> anyhow::Result<Self> {
        let metadata = decoder.metadata()?;
        let columns_count = metadata.columns_count;
        let rows_count = i32::try_decode_column(decoder.reader())?;
        Ok(Self {
            decoder,
            metadata,
            rows_count: rows_count as usize,
            remaining_total_columns_count: (columns_count * rows_count) as usize,
        })
    }
    /// Take the paging state
    pub fn take_paging_state(&mut self) -> Option<Vec<u8>> {
        self.metadata.take_paging_state()
    }

    /// Check if the iterator doesn't have any row
    pub fn is_empty(&self) -> bool {
        self.rows_count == 0
    }
    /// Get the iterator rows count
    pub fn rows_count(&self) -> usize {
        self.rows_count
    }

    /// Get the iterator remaining total columns count
    pub fn remaining_total_columns_count(&self) -> usize {
        self.remaining_total_columns_count
    }

    /// Get the columns count
    pub fn columns_count(&self) -> usize {
        self.metadata.columns_count as usize
    }

    /// Check if it has more pages to request
    pub fn has_more_pages(&self) -> bool {
        self.metadata.has_more_pages()
    }
}

impl<T> ColumnIter<T> {
    /// Check if the iterator doesn't have any row
    pub fn is_empty(&self) -> bool {
        self.rows_count == 0
    }
    /// Get the iterator rows count
    pub fn rows_count(&self) -> usize {
        self.rows_count
    }
    /// Get the iterator remaining rows count
    pub fn remaining_rows_count(&self) -> usize {
        self.remaining_rows_count
    }
    /// Check if it has more pages to request
    pub fn has_more_pages(&self) -> bool {
        self.metadata.has_more_pages()
    }
}

impl<T: ColumnDecoder> Rows for ColumnIter<T> {
    fn new(mut decoder: super::Decoder) -> anyhow::Result<Self> {
        let metadata = decoder.metadata()?;
        let columns_count = metadata.columns_count;
        let rows_count = i32::try_decode_column(decoder.reader())?;
        Ok(Self {
            decoder,
            metadata,
            rows_count: rows_count as usize,
            remaining_rows_count: rows_count as usize,
            remaining_columns_count: columns_count as usize,
            _marker: std::marker::PhantomData,
        })
    }
    fn take_paging_state(&mut self) -> Option<Vec<u8>> {
        self.metadata.take_paging_state()
    }
}

/// An iterator over the rows of a result-set
#[allow(unused)]
#[derive(Clone, Debug)]
pub struct Iter<T: Row> {
    decoder: super::Decoder,
    rows_count: usize,
    remaining_rows_count: usize,
    metadata: Metadata,
    _marker: std::marker::PhantomData<T>,
}

impl<T: Row> Iter<T> {
    /// Check if the iterator doesn't have any row
    pub fn is_empty(&self) -> bool {
        self.rows_count == 0
    }
    /// Get the iterator rows count
    pub fn rows_count(&self) -> usize {
        self.rows_count
    }
    /// Get the iterator remaining rows count
    pub fn remaining_rows_count(&self) -> usize {
        self.remaining_rows_count
    }
    /// Check if it has more pages to request
    pub fn has_more_pages(&self) -> bool {
        self.metadata.has_more_pages()
    }
}
impl<T: Row> Rows for Iter<T> {
    fn new(mut decoder: super::Decoder) -> anyhow::Result<Self> {
        let metadata = decoder.metadata()?;
        let rows_count = i32::try_decode_column(decoder.reader())?;
        Ok(Self {
            decoder,
            metadata,
            rows_count: rows_count as usize,
            remaining_rows_count: rows_count as usize,
            _marker: std::marker::PhantomData,
        })
    }
    fn take_paging_state(&mut self) -> Option<Vec<u8>> {
        self.metadata.take_paging_state()
    }
}

impl<T: Row> Iterator for Iter<T> {
    type Item = T;
    /// Note the row decoder is implemented in this `next` method of HardCodedSpecs.
    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        if self.remaining_rows_count > 0 {
            self.remaining_rows_count -= 1;
            T::try_decode_row(self).map_err(|e| error!("{}", e)).ok()
        } else {
            None
        }
    }
}

impl<T: ColumnDecoder> Iterator for ColumnIter<T> {
    type Item = T;
    /// Note the row decoder is implemented in this `next` method of HardCodedSpecs.
    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        if self.remaining_rows_count > 0 {
            if self.remaining_columns_count > 0 {
                self.column_value::<T>().map_err(|e| error!("{}", e)).ok()
            } else {
                self.remaining_columns_count = self.metadata.columns_count as usize;
                self.remaining_rows_count -= 1;
                self.next()
            }
        } else {
            None
        }
    }
}

impl<T> ColumnValue for ColumnIter<T> {
    fn column_value<C: ColumnDecoder>(&mut self) -> anyhow::Result<C> {
        self.remaining_columns_count -= 1;
        C::try_decode(self.decoder.reader())
    }
}
impl ColumnValue for AnyIter {
    fn column_value<C: ColumnDecoder>(&mut self) -> anyhow::Result<C> {
        self.remaining_total_columns_count -= 1;
        C::try_decode(self.decoder.reader())
    }
}

impl<T: Row> ColumnValue for Iter<T> {
    fn column_value<C: ColumnDecoder>(&mut self) -> anyhow::Result<C> {
        C::try_decode(self.decoder.reader())
    }
}

macro_rules! row {
    (@tuple ($($t:tt),*)) => {
        impl<$($t: ColumnDecoder),*> Row for ($($t,)*) {
            fn try_decode_row<R: ColumnValue>(rows: &mut R) -> anyhow::Result<Self> {
                Ok((
                    $(
                        rows.column_value::<$t>()?,
                    )*
                ))
            }
        }
    };
}

// HardCoded Specs,
row!(@tuple (T));
row!(@tuple (T,TT));
row!(@tuple (T, TT, TTT));
row!(@tuple (T, TT, TTT, TTTT));
row!(@tuple (T, TT, TTT, TTTT, TTTTT));
row!(@tuple (T, TT, TTT, TTTT, TTTTT, TTTTTT));
row!(@tuple (T, TT, TTT, TTTT, TTTTT, TTTTTT, TTTTTTT));
row!(@tuple (T, TT, TTT, TTTT, TTTTT, TTTTTT, TTTTTTT, TTTTTTTT));
row!(@tuple (T, TT, TTT, TTTT, TTTTT, TTTTTT, TTTTTTT, TTTTTTTT, TTTTTTTTT));
row!(@tuple (T, TT, TTT, TTTT, TTTTT, TTTTTT, TTTTTTT, TTTTTTTT, TTTTTTTTT, TTTTTTTTTT));
row!(@tuple (T, TT, TTT, TTTT, TTTTT, TTTTTT, TTTTTTT, TTTTTTTT, TTTTTTTTT, TTTTTTTTTT, TTTTTTTTTTT));
row!(@tuple (T, TT, TTT, TTTT, TTTTT, TTTTTT, TTTTTTT, TTTTTTTT, TTTTTTTTT, TTTTTTTTTT, TTTTTTTTTTT, TTTTTTTTTTTT));
row!(@tuple (T, TT, TTT, TTTT, TTTTT, TTTTTT, TTTTTTT, TTTTTTTT, TTTTTTTTT, TTTTTTTTTT, TTTTTTTTTTT, TTTTTTTTTTTT, TTTTTTTTTTTTT));
row!(@tuple (T, TT, TTT, TTTT, TTTTT, TTTTTT, TTTTTTT, TTTTTTTT, TTTTTTTTT, TTTTTTTTTT, TTTTTTTTTTT, TTTTTTTTTTTT, TTTTTTTTTTTTT, TTTTTTTTTTTTTT));
row!(@tuple (T, TT, TTT, TTTT, TTTTT, TTTTTT, TTTTTTT, TTTTTTTT, TTTTTTTTT, TTTTTTTTTT, TTTTTTTTTTT, TTTTTTTTTTTT, TTTTTTTTTTTTT, TTTTTTTTTTTTTT, TTTTTTTTTTTTTTT));

#[macro_export]
/// The rows macro implements the row decoder.
macro_rules! rows {
    (@common_rows $rows:ident$(<$($t:ident),+>)?) => {
        #[allow(dead_code)]
        #[allow(unused_parens)]
        /// The `rows` struct for processing each received row in ScyllaDB.
        pub struct $rows$(<$($t),+>)? {
            decoder: Decoder,
            rows_count: usize,
            remaining_rows_count: usize,
            metadata: Metadata,
            $(_marker: PhantomData<($($t),+)>,)?
        }

        impl$(<$($t),+>)? $rows$(<$($t),+>)? {
            #[allow(dead_code)]
            pub fn rows_count(&self) -> usize {
                self.rows_count
            }

            #[allow(dead_code)]
            pub fn remaining_rows_count(&self) -> usize {
                self.remaining_rows_count
            }
        }

        #[allow(unused_parens)]
        impl$(<$($t),+>)? Rows for $rows$(<$($t),+>)? {
            /// Create a new rows structure.
            fn new(mut decoder: Decoder) -> anyhow::Result<Self> {
                let metadata = decoder.metadata()?;
                let rows_count = i32::try_decode_column(decoder.reader())?;
                Ok(Self {
                    decoder,
                    metadata,
                    rows_count: rows_count as usize,
                    remaining_rows_count: rows_count as usize,
                    $(_marker: PhantomData::<($($t),+)>,)?
                })
            }
            fn take_paging_state(&mut self) -> Option<Vec<u8>> {
                self.metadata.take_paging_state()
            }
        }
    };
    (@common_row $row:ident {$( $col_field:ident: $col_type:ty),*}) => {
        /// It's the `row` struct
        pub struct $row {
            $(
                pub $col_field: $col_type,
            )*
        }
    };
    (@common_iter $rows:ident$(<$($t:ident),+>)?, $row:ident {$( $col_field:ident: $col_type:ty),*}, $row_into:ty) => {
        impl$(<$($t),+>)? Iterator for $rows$(<$($t),+>)? {
            type Item = $row_into;
            /// Note the row decoder is implemented in this `next` method.
            fn next(&mut self) -> Option<<Self as Iterator>::Item> {
                if self.remaining_rows_count > 0 {
                    self.remaining_rows_count -= 1;
                    let row_struct = $row {
                        $(
                            $col_field: {
                                <$col_type>::try_decode(self.decoder.reader()).ok()?
                            },
                        )*
                    };
                    Some(row_struct.into())
                } else {
                    None
                }
            }
        }
    };
    (single_row: $rows:ident$(<$($t:ident),+>)?, row: $row:ident {$( $col_field:ident: $col_type:ty),* $(,)?}, row_into: $row_into:ty $(,)? ) => {
        rows!(@common_rows $rows$(<$($t),+>)?);

        rows!(@common_row $row {$( $col_field: $col_type),*});

        rows!(@common_iter $rows$(<$($t),+>)?, $row {$( $col_field: $col_type),*}, $row_into);

        impl $rows {
            pub fn get(&mut self) -> Option<$row_into> {
                self.next()
            }
        }
    };
    (rows: $rows:ident$(<$($t:ident),+>)?, row: $row:ty, row_into: $row_into:ty $(,)? ) => {
        rows!(@common_rows $rows$(<$($t),+>)?);

        impl$(<$($t),+>)? Iterator for $rows$(<$($t),+>)? {
            type Item = $row_into;
            /// Note the row decoder is implemented in this `next` method.
            fn next(&mut self) -> Option<<Self as Iterator>::Item> {
                if self.remaining_rows_count > 0 {
                    self.remaining_rows_count -= 1;
                    <$row>::try_decode_row(self).map_err(|e| error!("{}", e)).ok().into()
                } else {
                    None
                }
            }
        }
    };
    (rows: $rows:ident$(<$($t:ident),+>)?, row: $row:ident {$( $col_field:ident: $col_type:ty),* $(,)?}, row_into: $row_into:ty $(,)? ) => {
        rows!(@common_rows $rows$(<$($t),+>)?);

        rows!(@common_row $row {$( $col_field: $col_type),*});

        rows!(@common_iter $rows$(<$($t),+>)?, $row {$( $col_field: $col_type),*}, $row_into);
    };
}
