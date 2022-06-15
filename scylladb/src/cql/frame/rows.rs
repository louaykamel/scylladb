//! This module defines the row/column decoder/encoder for the frame structure.

use super::{
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
/// The meta structure of the row.
pub struct Metadata {
    flags: Flags,
    #[allow(unused)]
    columns_count: ColumnsCount,
    paging_state: PagingState,
}

impl Metadata {
    /// Create a new meta data.
    pub fn new(flags: Flags, columns_count: ColumnsCount, paging_state: PagingState) -> Self {
        Metadata {
            flags,
            columns_count,
            paging_state,
        }
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
    fn try_decode_row<R: Rows + ColumnValue>(rows: &mut R) -> anyhow::Result<Self>
    where
        Self: Sized;
}

impl<T> Row for T
where
    T: ColumnDecoder,
{
    fn try_decode_row<R: Rows + ColumnValue>(rows: &mut R) -> anyhow::Result<Self>
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
    remaining_column_count: usize,
    metadata: Metadata,
    _marker: std::marker::PhantomData<T>,
}

#[allow(unused)]
#[derive(Clone, Debug)]
/// AnyIterator to iterator over the column values
pub struct AnyIter {
    decoder: super::Decoder,
    rows_count: usize,
    remaining_rows_count: usize,
    remaining_column_count: usize,
    metadata: Metadata,
}

impl AnyIter {
    /// Iterates the next column value
    pub fn next<T: ColumnDecoder>(&mut self) -> Option<T> {
        if self.remaining_rows_count > 0 {
            if self.remaining_column_count > 0 {
                self.remaining_column_count -= 1;
                Some(self.column_value::<T>().unwrap())
            } else {
                self.remaining_column_count = self.metadata.columns_count as usize;
                self.remaining_rows_count -= 1;
                self.next()
            }
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
            remaining_rows_count: rows_count as usize,
            remaining_column_count: columns_count as usize,
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
    /// Get the iterator remaining rows count
    pub fn remaining_rows_count(&self) -> usize {
        self.remaining_rows_count
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
            remaining_column_count: columns_count as usize,
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
            if self.remaining_column_count > 0 {
                self.remaining_column_count -= 1;
                Some(self.column_value::<T>().unwrap())
            } else {
                self.remaining_column_count = self.metadata.columns_count as usize;
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
        C::try_decode(self.decoder.reader())
    }
}
impl ColumnValue for AnyIter {
    fn column_value<C: ColumnDecoder>(&mut self) -> anyhow::Result<C> {
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
